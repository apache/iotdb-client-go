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

// This example adapts Java TableModelSubscriptionSessionExample's record topic
// and metadata traversal. Record-format Push delivery is an additional Go path.
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

	"github.com/apache/iotdb-client-go/subscription"
	subconsumer "github.com/apache/iotdb-client-go/subscription/consumer"
	subpayload "github.com/apache/iotdb-client-go/subscription/payload"
	subsession "github.com/apache/iotdb-client-go/subscription/session"
	"github.com/apache/iotdb-client-go/v2/client"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	host := flag.String("host", "127.0.0.1", "IoTDB host")
	port := flag.Int("port", 6667, "IoTDB RPC port")
	username := flag.String("username", "root", "IoTDB username")
	mode := flag.String("consumer", "pull", "Record consumer: pull or push")
	ack := flag.String("ack", "after", "Push acknowledgment strategy: after or before")
	duration := flag.Duration("duration", 10*time.Second, "How long to receive Push records")
	flag.Parse()
	if *port < 1 || *port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	if *mode != "pull" && *mode != "push" {
		return errors.New("consumer must be pull or push")
	}
	if *ack != "after" && *ack != "before" {
		return errors.New("ack must be after or before")
	}
	if *mode == "push" && *duration <= 0 {
		return errors.New("duration must be positive for Push consumption")
	}
	password, supplied := os.LookupEnv("IOTDB_PASSWORD")
	if !supplied {
		password = "root"
	}
	address := net.JoinHostPort(*host, strconv.Itoa(*port))
	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	database := "go_subscription_table_" + runID
	table := "events"
	topic := "go_subscription_table_topic_" + runID
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dataSession, err := client.NewTableSession(&client.Config{
		Host: *host,
		Port: strconv.Itoa(*port),
		UserName: *username,
		Password: password,
	}, false, 10000)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dataSession.Close()) }()
	if err := prepareData(dataSession, database, table); err != nil {
		return err
	}
	if _, err := fmt.Printf("Database %s and table %s are left in place for inspection.\n", database, table); err != nil {
		return err
	}

	managementConfig := subsession.DefaultSubscriptionSessionConfig()
	managementConfig.Dialect = subscription.TableDialect
	managementConfig.Connection.NodeURLs = []string{address}
	managementConfig.Connection.Username = *username
	managementConfig.Connection.Password = password
	management, err := subsession.NewSubscriptionSession(managementConfig)
	if err != nil {
		return err
	}
	if err := management.Open(ctx); err != nil {
		return err
	}
	defer func() { err = errors.Join(err, management.Close()) }()
	if err := management.CreateTopic(ctx, topic, subsession.CreateTopicOptions{
		Attributes: map[string]string{
			"mode": "initial",
			"format": "SubscriptionRecordHandler",
			"database": database,
			"table": table,
			"start-time": "25",
			"end-time": "75",
			"strict": "true",
		},
	}); err != nil {
		return err
	}
	// Drop only the unique topic whose creation succeeded in this run.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = errors.Join(err, management.DropTopic(cleanupCtx, topic, subsession.DropOptions{IfExists: true}))
	}()
	if topics, err := management.Topics(ctx); err != nil {
		return err
	} else {
		if _, err := fmt.Printf("Topics: %+v\n", topics); err != nil {
			return err
		}
	}

	consumerConfig := subconsumer.DefaultSubscriptionConsumerConfig()
	consumerConfig.Dialect = subscription.TableDialect
	consumerConfig.Connection = managementConfig.Connection
	consumerConfig.ConsumerID = "table_consumer_" + runID
	consumerConfig.GroupID = "table_group_" + runID
	if *mode == "push" {
		config := subconsumer.DefaultSubscriptionPushConsumerConfig()
		config.Consumer = consumerConfig
		if *ack == "before" {
			config.AckStrategy = subconsumer.AckBeforeConsume
		}
		config.Handler = func(callbackCtx context.Context, message subpayload.SubscriptionMessage) error {
			_, err := printMessage(callbackCtx, message)
			// Push owns the message; the Handler must not call Release.
			return err
		}
		consumer, err := subconsumer.NewSubscriptionPushConsumer(config)
		if err != nil {
			return err
		}
		if err := consumer.Open(ctx); err != nil {
			return err
		}
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = errors.Join(err, consumer.Close())
			// Close cancels delivery; a callback may still be returning. Wait for
			// the loop with a fresh budget, even if the run context was canceled.
			for {
				select {
				case terminal, ok := <-consumer.Errors():
					if !ok {
						return
					}
					err = errors.Join(err, terminal)
				case <-cleanupCtx.Done():
					err = errors.Join(err, fmt.Errorf("waiting for Push shutdown: %w", cleanupCtx.Err()))
					return
				}
			}
		}()
		if err := consumer.Subscribe(ctx, topic); err != nil {
			return err
		}
		if err := showSubscriptions(ctx, management, topic); err != nil {
			return err
		}
		timer := time.NewTimer(*duration)
		defer timer.Stop()
		select {
		case terminal, ok := <-consumer.Errors():
			if ok {
				return terminal
			}
			return nil
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	config := subconsumer.DefaultSubscriptionPullConsumerConfig()
	config.Consumer = consumerConfig
	// Keep the default automatic commit behavior of the ordinary Java example.
	consumer, err := subconsumer.NewSubscriptionPullConsumer(config)
	if err != nil {
		return err
	}
	if err := consumer.Open(ctx); err != nil {
		return err
	}
	// Close commits pending automatic acknowledgments before unregistering the
	// consumer. Unsubscribing first could invalidate those outstanding contexts.
	defer func() { err = errors.Join(err, consumer.Close()) }()
	if err := consumer.Subscribe(ctx, topic); err != nil {
		return err
	}
	if err := showSubscriptions(ctx, management, topic); err != nil {
		return err
	}
	rows, emptyRounds := 0, 0
	for round := 0; round < 20; round++ {
		messages, err := consumer.Poll(ctx, time.Second)
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			emptyRounds++
			if rows >= 5 && emptyRounds >= 3 {
				break
			}
			continue
		}
		emptyRounds = 0
		count, err := printPullMessages(ctx, messages)
		if err != nil {
			return err
		}
		rows += count
	}
	if rows < 5 {
		return fmt.Errorf("received %d of at least 5 expected rows; check subscription support on the server", rows)
	}
	return nil
}

func prepareData(session client.ITableSession, database, table string) error {
	for _, statement := range []string{
		"CREATE DATABASE " + database,
		"USE " + database,
		"CREATE TABLE " + table + "(tag1 STRING TAG, s1 INT64 FIELD)",
	} {
		if err := session.ExecuteNonQueryStatement(statement); err != nil {
			return err
		}
	}
	for index := 0; index < 5; index++ {
		statement := fmt.Sprintf("INSERT INTO %s(time, tag1, s1) VALUES (%d, 'device_%d', %d)", table, 25+index, index, index*10)
		if err := session.ExecuteNonQueryStatement(statement); err != nil {
			return err
		}
	}
	return session.ExecuteNonQueryStatement("FLUSH")
}

func showSubscriptions(ctx context.Context, management subsession.SubscriptionSession, topic string) error {
	subscriptions, err := management.Subscriptions(ctx, topic)
	if err == nil {
		_, err = fmt.Printf("Subscriptions: %+v\n", subscriptions)
	}
	return err
}

func printPullMessages(ctx context.Context, messages []subpayload.SubscriptionMessage) (int, error) {
	defer func() {
		for _, message := range messages {
			message.Release()
		}
	}()
	rows := 0
	for _, message := range messages {
		count, err := printMessage(ctx, message)
		if err != nil {
			return rows, err
		}
		rows += count
	}
	return rows, nil
}

func printMessage(ctx context.Context, message subpayload.SubscriptionMessage) (int, error) {
	batches, err := message.Records()
	if err != nil {
		return 0, err
	}
	rows := 0
	for _, batch := range batches {
		if _, err := fmt.Printf("database=%s table=%s timeSelected=%t columnCount=%d\n", batch.Database(), batch.Target(), batch.TimeSelected(), batch.ColumnCount()); err != nil {
			return rows, err
		}
		for row, err := range batch.Rows() {
			if err != nil {
				return rows, err
			}
			if err := ctx.Err(); err != nil {
				return rows, err
			}
			var timestamp any
			if row.Timestamp != nil {
				timestamp = *row.Timestamp
			}
			if _, err := fmt.Printf("time=%v values=%v\n", timestamp, row.Values); err != nil {
				return rows, err
			}
			rows++
		}
	}
	return rows, nil
}
