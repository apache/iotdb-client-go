/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// This example mirrors SubscriptionSessionExample.dataSubscription1 and adds
// record-based push delivery. The generated database is retained for inspection.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/subscription"
	subconsumer "github.com/apache/iotdb-client-go/v2/subscription/consumer"
	subpayload "github.com/apache/iotdb-client-go/v2/subscription/payload"
	subsession "github.com/apache/iotdb-client-go/v2/subscription/session"
)

func main() {
	host := flag.String("host", "127.0.0.1", "IoTDB host")
	port := flag.Int("port", 6667, "IoTDB RPC port")
	username := flag.String("username", "root", "IoTDB username")
	consumer := flag.String("consumer", "pull", "Consumer type: pull or push")
	ack := flag.String("ack", "after", "Push acknowledgment: after or before")
	duration := flag.Duration("duration", 10*time.Second, "Push consumption duration; ignored by pull")
	flag.Parse()
	if *port < 1 || *port > 65535 || (*consumer != "pull" && *consumer != "push") ||
		(*ack != "after" && *ack != "before") || (*consumer == "push" && *duration <= 0) {
		log.Fatal("Invalid port, consumer, acknowledgment strategy, or push duration")
	}
	password, supplied := os.LookupEnv("IOTDB_PASSWORD")
	if !supplied {
		password = "root"
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *host, *port, *username, password, *consumer, *ack, *duration); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, host string, port int, username, password, kind, ack string, duration time.Duration) (result error) {
	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	database := "root.subscription_tree_" + runID
	device := database + ".d1"
	topic := "subscription_tree_" + runID
	if _, err := fmt.Printf("Database retained for inspection: %s; device=%s; topic=%s\n", database, device, topic); err != nil {
		return err
	}
	if err := prepareData(ctx, host, port, username, password, database, device); err != nil {
		return err
	}
	cfg := subsession.DefaultSubscriptionSessionConfig()
	cfg.Connection.NodeURLs = []string{net.JoinHostPort(host, strconv.Itoa(port))}
	cfg.Connection.Username, cfg.Connection.Password = username, password
	manager, err := subsession.NewSubscriptionSession(cfg)
	if err != nil {
		return err
	}
	created := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if created {
			result = errors.Join(result, manager.DropTopic(cleanupCtx, topic, subsession.DropOptions{IfExists: true}))
		}
		result = errors.Join(result, manager.Close())
	}()
	if err := manager.Open(ctx); err != nil {
		return err
	}
	if err := manager.CreateTopic(ctx, topic, subsession.CreateTopicOptions{Attributes: map[string]string{
		"mode": "initial",
		"format": "SubscriptionRecordHandler",
		"path": device + ".s1",
		"start-time": "25",
		"end-time": "75",
	}}); err != nil {
		return err
	}
	created = true
	if kind == "push" {
		return consumePush(ctx, cfg.Connection, manager, topic, ack, duration)
	}
	return consumePull(ctx, cfg.Connection, manager, topic)
}

func prepareData(ctx context.Context, host string, port int, username, password, database, device string) (result error) {
	session := client.NewSession(&client.Config{Host: host, Port: strconv.Itoa(port), UserName: username, Password: password})
	if err := session.Open(false, 10000); err != nil {
		return err
	}
	defer func() { result = errors.Join(result, session.Close()) }()
	for _, sql := range []string{
		"CREATE DATABASE " + database,
		"CREATE TIMESERIES " + device + ".s1 WITH DATATYPE=INT64, ENCODING=RLE",
		"CREATE TIMESERIES " + device + ".s2 WITH DATATYPE=INT64, ENCODING=RLE",
	} {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := session.ExecuteNonQueryStatement(sql); err != nil {
			return err
		}
	}
	for timestamp := int64(0); timestamp < 100; timestamp++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := session.InsertRecord(device, []string{"s1", "s2"}, []client.TSDataType{client.INT64, client.INT64}, []interface{}{int64(1), int64(2)}, timestamp); err != nil {
			return err
		}
	}
	return session.ExecuteNonQueryStatement("FLUSH")
}

func consumePull(ctx context.Context, connection subscription.ConnectionConfig, manager subsession.SubscriptionSession, topic string) (result error) {
	cfg := subconsumer.DefaultSubscriptionPullConsumerConfig()
	cfg.Consumer.Connection = connection
	consumer, err := subconsumer.NewSubscriptionPullConsumer(cfg)
	if err != nil {
		return err
	}
	// Close flushes pending automatic commits while the subscription still exists.
	defer func() { result = errors.Join(result, consumer.Close()) }()
	if err := consumer.Open(ctx); err != nil {
		return err
	}
	if err := consumer.Subscribe(ctx, topic); err != nil {
		return err
	}
	if err := printManagement(ctx, manager, topic); err != nil {
		return err
	}
	emptyPolls := 0
	for emptyPolls < 3 && !consumer.AllConsumed() {
		messages, err := consumer.Poll(ctx, 10*time.Second)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			emptyPolls++
		} else {
			emptyPolls = 0
		}
		for index, message := range messages {
			err := printMessage(ctx, message)
			message.Release()
			if err != nil {
				for _, remaining := range messages[index+1:] {
					remaining.Release()
				}
				return err
			}
		}
	}
	return nil
}

func consumePush(ctx context.Context, connection subscription.ConnectionConfig, manager subsession.SubscriptionSession, topic, ack string, duration time.Duration) (result error) {
	cfg := subconsumer.DefaultSubscriptionPushConsumerConfig()
	cfg.Consumer.Connection = connection
	if ack == "before" {
		cfg.AckStrategy = subconsumer.AckBeforeConsume
	}
	// The delivery loop owns messages; a push Handler must never call Release.
	cfg.Handler = printMessage
	consumer, err := subconsumer.NewSubscriptionPushConsumer(cfg)
	if err != nil {
		return err
	}
	defer func() {
		result = errors.Join(result, consumer.Close())
		// Close cancels callbacks but may return before the Handler finishes.
		waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for {
			select {
			case failure, ok := <-consumer.Errors():
				if !ok {
					return
				}
				result = errors.Join(result, failure)
			case <-waitCtx.Done():
				result = errors.Join(result, waitCtx.Err())
				return
			}
		}
	}()
	if err := consumer.Open(ctx); err != nil {
		return err
	}
	if err := consumer.Subscribe(ctx, topic); err != nil {
		return err
	}
	if err := printManagement(ctx, manager, topic); err != nil {
		return err
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case err, ok := <-consumer.Errors():
		if ok {
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func printManagement(ctx context.Context, manager subsession.SubscriptionSession, topic string) error {
	topics, err := manager.Topics(ctx)
	if err != nil {
		return err
	}
	subscriptions, err := manager.Subscriptions(ctx, topic)
	if err != nil {
		return err
	}
	_, err = fmt.Printf("Topics: %+v\nSubscriptions for %s: %+v\n", topics, topic, subscriptions)
	return err
}

func printMessage(ctx context.Context, message subpayload.SubscriptionMessage) error {
	batches, err := message.Records()
	if err != nil {
		return err
	}
	if _, err := fmt.Printf("Commit context: %+v\n", message.CommitContext()); err != nil {
		return err
	}
	for _, batch := range batches {
		if _, err := fmt.Printf("dialect=%s database=%s target=%s timeSelected=%t columnCount=%d\n", batch.Dialect(), batch.Database(), batch.Target(), batch.TimeSelected(), batch.ColumnCount()); err != nil {
			return err
		}
		for row, err := range batch.Rows() {
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if row.Timestamp == nil {
				_, err = fmt.Printf("values=%v\n", row.Values)
			} else {
				_, err = fmt.Printf("time=%d values=%v\n", *row.Timestamp, row.Values)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
