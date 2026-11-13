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

// This example mirrors ConsensusSubscriptionSessionExample using incremental
// record topics. The generated database is retained for inspection.
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
	"github.com/apache/iotdb-client-go/subscription"
	subconsumer "github.com/apache/iotdb-client-go/subscription/consumer"
	subpayload "github.com/apache/iotdb-client-go/subscription/payload"
	subprocessor "github.com/apache/iotdb-client-go/subscription/processor"
	subsession "github.com/apache/iotdb-client-go/subscription/session"
)

func main() {
	host := flag.String("host", "127.0.0.1", "IoTDB host")
	port := flag.Int("port", 6667, "IoTDB RPC port")
	username := flag.String("username", "root", "IoTDB username")
	processors := flag.Bool("processors", false, "Order messages by watermark, then forward-fill NULL columns")
	flag.Parse()
	if *port < 1 || *port > 65535 {
		log.Fatal("Port must be between 1 and 65535")
	}
	password, supplied := os.LookupEnv("IOTDB_PASSWORD")
	if !supplied {
		password = "root"
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *host, *port, *username, password, *processors); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, host string, port int, username, password string, processors bool) (result error) {
	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	database := "root.subscription_tree_incremental_" + runID
	device := database + ".d0"
	topic := "subscription_tree_incremental_" + runID
	if _, err := fmt.Printf("Database retained for inspection: %s; device=%s; topic=%s\n", database, device, topic); err != nil {
		return err
	}
	writer := client.NewSession(&client.Config{Host: host, Port: strconv.Itoa(port), UserName: username, Password: password})
	if err := writer.Open(false, 10000); err != nil {
		return err
	}
	defer func() { result = errors.Join(result, writer.Close()) }()
	// Bootstrap and flush before topic creation so the server has a data region.
	for _, sql := range []string{
		"CREATE DATABASE " + database,
		"CREATE TIMESERIES " + device + ".s1 WITH DATATYPE=INT64, ENCODING=RLE",
		"INSERT INTO " + device + "(time,s1) VALUES (0,0)",
		"FLUSH",
	} {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := writer.ExecuteNonQueryStatement(sql); err != nil {
			return err
		}
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
		"mode": "incremental",
		"format": "SubscriptionRecordHandler",
		"path": database + ".**",
		"order-mode": "per-writer",
	}}); err != nil {
		return err
	}
	created = true
	return consume(ctx, cfg.Connection, &writer, device, topic, processors)
}

func consume(ctx context.Context, connection subscription.ConnectionConfig, writer *client.Session, device, topic string, processors bool) (result error) {
	cfg := subconsumer.DefaultSubscriptionPullConsumerConfig()
	cfg.Consumer.Connection = connection
	autoCommit := false
	cfg.AutoCommit = &autoCommit
	consumer, err := subconsumer.NewSubscriptionPullConsumer(cfg)
	if err != nil {
		return err
	}
	opened, subscribed, drained := false, false, false
	defer func() {
		// Cleanup uses its own live context even when a signal canceled ctx.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if opened && !drained {
			_, drainErr := drainAndCommit(cleanupCtx, consumer)
			result = errors.Join(result, drainErr)
		}
		if subscribed {
			result = errors.Join(result, consumer.Unsubscribe(cleanupCtx, topic))
		}
		result = errors.Join(result, consumer.Close())
	}()
	if processors {
		watermark, err := subprocessor.NewWatermarkProcessor(subprocessor.WatermarkProcessorConfig{
			MaxOutOfOrderness: 1,
			Timeout: 2 * time.Second,
		})
		if err != nil {
			return err
		}
		alignment, err := subprocessor.NewColumnAlignProcessor(subscription.TreeDialect)
		if err != nil {
			return err
		}
		if err := consumer.AddProcessor(watermark); err != nil {
			return err
		}
		if err := consumer.AddProcessor(alignment); err != nil {
			return err
		}
	}
	if err := consumer.Open(ctx); err != nil {
		return err
	}
	opened = true
	if err := consumer.Subscribe(ctx, topic); err != nil {
		return err
	}
	subscribed = true
	fmt.Printf("Consumer: %+v\n", consumer.Info())
	// These five writes occur after Subscribe; the bootstrap row is historical.
	for timestamp := int64(1); timestamp <= 5; timestamp++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := writer.InsertRecord(device, []string{"s1"}, []client.TSDataType{client.INT64}, []interface{}{timestamp * 10}, timestamp); err != nil {
			return err
		}
	}
	if err := writer.ExecuteNonQueryStatement("FLUSH"); err != nil {
		return err
	}
	totalRows, emptyPolls := 0, 0
	for round := 1; round <= 20; round++ {
		poll, err := consumer.PollWithInfo(ctx, time.Second)
		if err != nil {
			return err
		}
		fmt.Printf("Poll round=%d buffered=%d watermark=%d hasWatermark=%t\n", round, poll.BufferedCount, poll.Watermark, poll.HasWatermark)
		rows, err := processAndCommit(ctx, consumer, poll.Messages)
		if err != nil {
			return err
		}
		totalRows += rows
		if err := printPositions(consumer, topic); err != nil {
			return err
		}
		if len(poll.Messages) == 0 {
			emptyPolls++
		} else {
			emptyPolls = 0
		}
		if totalRows >= 5 && emptyPolls >= 3 {
			break
		}
	}
	// Drain before checkpoint/seek so seek cannot discard unprocessed buffers.
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 10*time.Second)
	rows, err := drainAndCommit(drainCtx, consumer)
	cancelDrain()
	if err != nil {
		return err
	}
	drained = true
	totalRows += rows
	if totalRows != 5 {
		return fmt.Errorf("expected 5 realtime rows, consumed %d; check server incremental subscription support", totalRows)
	}
	if err := printPositions(consumer, topic); err != nil {
		return err
	}
	// This checkpoint is an owned in-memory snapshot, not persistent storage.
	checkpoint, err := consumer.CommittedPositions(topic)
	if err != nil {
		return err
	}
	fmt.Printf("In-memory checkpoint: %+v\n", checkpoint)
	if len(checkpoint) == 0 {
		return errors.New("server returned no writer checkpoint for incremental consumption")
	}
	if err := consumer.SeekAfter(ctx, topic, checkpoint); err != nil {
		return err
	}
	fmt.Println("SeekAfter resumed once from the confirmed in-memory checkpoint")
	return printPositions(consumer, topic)
}

func drainAndCommit(ctx context.Context, consumer subconsumer.SubscriptionPullConsumer) (int, error) {
	messages, err := consumer.DrainBufferedMessages(ctx)
	if err != nil {
		return 0, err
	}
	return processAndCommit(ctx, consumer, messages)
}

func processAndCommit(ctx context.Context, consumer subconsumer.SubscriptionPullConsumer, messages []subpayload.SubscriptionMessage) (int, error) {
	defer func() {
		for _, message := range messages {
			message.Release()
		}
	}()
	total := 0
	for _, message := range messages {
		rows, err := printMessage(ctx, message)
		if err != nil {
			return total, err
		}
		// Never confirm a message whose processing or context check failed.
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if err := consumer.Commit(ctx, message); err != nil {
			return total, err
		}
		total += rows
	}
	return total, nil
}

func printPositions(consumer subconsumer.SubscriptionPullConsumer, topic string) error {
	positions, err := consumer.Positions(topic)
	if err != nil {
		return err
	}
	committed, err := consumer.CommittedPositions(topic)
	if err != nil {
		return err
	}
	_, err = fmt.Printf("Positions: %+v\nCommitted positions: %+v\n", positions, committed)
	return err
}

func printMessage(ctx context.Context, message subpayload.SubscriptionMessage) (int, error) {
	batches, err := message.Records()
	if err != nil {
		return 0, err
	}
	if _, err := fmt.Printf("Commit context: %+v\n", message.CommitContext()); err != nil {
		return 0, err
	}
	total := 0
	for _, batch := range batches {
		if _, err := fmt.Printf("dialect=%s database=%s target=%s timeSelected=%t columns=%+v\n", batch.Dialect(), batch.Database(), batch.Target(), batch.TimeSelected(), batch.Columns()); err != nil {
			return total, err
		}
		for row, err := range batch.Rows() {
			if err != nil {
				return total, err
			}
			if err := ctx.Err(); err != nil {
				return total, err
			}
			if row.Timestamp == nil {
				_, err = fmt.Printf("values=%v\n", row.Values)
			} else {
				_, err = fmt.Printf("time=%d values=%v\n", *row.Timestamp, row.Values)
			}
			if err != nil {
				return total, err
			}
			total++
		}
	}
	return total, nil
}
