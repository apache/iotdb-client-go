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

// This example follows Java ConsensusTableModelSubscriptionSessionExample's
// incremental record flow, adding progress, SeekAfter, and optional processors.
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

	"github.com/apache/iotdb-client-go/v2/subscription"
	subconsumer "github.com/apache/iotdb-client-go/v2/subscription/consumer"
	subpayload "github.com/apache/iotdb-client-go/v2/subscription/payload"
	subprocessor "github.com/apache/iotdb-client-go/v2/subscription/processor"
	subsession "github.com/apache/iotdb-client-go/v2/subscription/session"
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
	processors := flag.Bool("processors", false, "Enable Watermark followed by table ColumnAlign processing")
	flag.Parse()
	if *port < 1 || *port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	password, supplied := os.LookupEnv("IOTDB_PASSWORD")
	if !supplied {
		password = "root"
	}
	address := net.JoinHostPort(*host, strconv.Itoa(*port))
	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	database := "go_subscription_table_incremental_" + runID
	table := "events"
	topic := "go_subscription_table_incremental_topic_" + runID
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
	if err := prepareBootstrapData(dataSession, database, table); err != nil {
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
			"mode": "incremental",
			"format": "SubscriptionRecordHandler",
			"database": database,
			"table": table,
			"order-mode": "per-writer",
		},
	}); err != nil {
		return err
	}
	// This cleanup owns only the topic successfully created above, not user data.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = errors.Join(err, management.DropTopic(cleanupCtx, topic, subsession.DropOptions{IfExists: true}))
	}()

	config := subconsumer.DefaultSubscriptionPullConsumerConfig()
	config.Consumer.Dialect = subscription.TableDialect
	config.Consumer.Connection = managementConfig.Connection
	config.Consumer.ConsumerID = "table_incremental_consumer_" + runID
	config.Consumer.GroupID = "table_incremental_group_" + runID
	autoCommit := false
	config.AutoCommit = &autoCommit
	consumer, err := subconsumer.NewSubscriptionPullConsumer(config)
	if err != nil {
		return err
	}
	var watermark *subprocessor.WatermarkProcessor
	var alignment *subprocessor.ColumnAlignProcessor
	if *processors {
		watermark, err = subprocessor.NewWatermarkProcessor(subprocessor.WatermarkProcessorConfig{
			MaxOutOfOrderness: 1,
			Timeout: 2 * time.Second,
		})
		if err != nil {
			return err
		}
		alignment, err = subprocessor.NewColumnAlignProcessor(subscription.TableDialect)
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
	defer func() {
		if err != nil {
			// An error is not permission to acknowledge unprocessed data. Discard
			// processor state without ACK so Close can release the connections.
			if watermark != nil {
				watermark.Reset()
			}
			if alignment != nil {
				alignment.Reset()
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			discarded, drainErr := consumer.DrainBufferedMessages(cleanupCtx)
			for _, message := range discarded {
				message.Release()
			}
			err = errors.Join(err, drainErr)
		}
		err = errors.Join(err, consumer.Close())
	}()
	if err := consumer.Subscribe(ctx, topic); err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err = errors.Join(err, consumer.Unsubscribe(cleanupCtx, topic))
	}()
	// The bootstrap only creates the region. Incremental consumption starts
	// after Subscribe, so write the five demonstrated records afterward.
	if err := writeRealtimeData(dataSession, table); err != nil {
		return err
	}

	rows := 0
	for round := 1; round <= 20 && rows < 5; round++ {
		result, err := consumer.PollWithInfo(ctx, time.Second)
		if err != nil {
			return err
		}
		count, err := processAndCommit(ctx, consumer, result.Messages)
		if err != nil {
			return err
		}
		rows += count
		if _, err := fmt.Printf("poll=%d rows=%d buffered=%d watermark=%d hasWatermark=%t\n", round, rows, result.BufferedCount, result.Watermark, result.HasWatermark); err != nil {
			return err
		}
		if err := printProgress(consumer, topic); err != nil {
			return err
		}
	}
	// Flush processor output before checkpointing or seeking. Only successfully
	// printed records are committed, using a live operation context.
	drained, err := consumer.DrainBufferedMessages(ctx)
	if err != nil {
		return err
	}
	count, err := processAndCommit(ctx, consumer, drained)
	if err != nil {
		return err
	}
	rows += count
	if err := printProgress(consumer, topic); err != nil {
		return err
	}
	if rows < 5 {
		return fmt.Errorf("received %d of at least 5 realtime rows; check incremental subscription support on the server", rows)
	}
	checkpoint, err := consumer.CommittedPositions(topic)
	if err != nil {
		return err
	}
	if len(checkpoint) == 0 {
		return errors.New("no committed writer progress is available for SeekAfter")
	}
	// This checkpoint is an in-memory snapshot for this run, not durable storage.
	if err := consumer.SeekAfter(ctx, topic, checkpoint); err != nil {
		return err
	}
	_, err = fmt.Printf("SeekAfter completed with the in-memory checkpoint: %+v\n", checkpoint)
	return err
}

func prepareBootstrapData(session client.ITableSession, database, table string) error {
	for _, statement := range []string{
		"CREATE DATABASE " + database,
		"USE " + database,
		"CREATE TABLE " + table + "(tag1 STRING TAG, s1 INT64 FIELD)",
		"INSERT INTO " + table + "(time, tag1, s1) VALUES (0, 'bootstrap', 0)",
		"FLUSH",
	} {
		if err := session.ExecuteNonQueryStatement(statement); err != nil {
			return err
		}
	}
	return nil
}

func writeRealtimeData(session client.ITableSession, table string) error {
	for index := 1; index <= 5; index++ {
		statement := fmt.Sprintf("INSERT INTO %s(time, tag1, s1) VALUES (%d, 'device_%d', %d)", table, index, index, index*10)
		if err := session.ExecuteNonQueryStatement(statement); err != nil {
			return err
		}
	}
	return session.ExecuteNonQueryStatement("FLUSH")
}

func processAndCommit(ctx context.Context, consumer subconsumer.SubscriptionPullConsumer, messages []subpayload.SubscriptionMessage) (int, error) {
	defer func() {
		for _, message := range messages {
			message.Release()
		}
	}()
	rows := 0
	for _, message := range messages {
		batches, err := message.Records()
		if err != nil {
			return rows, err
		}
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
	}
	if len(messages) != 0 {
		if err := consumer.Commit(ctx, messages...); err != nil {
			return rows, err
		}
	}
	return rows, nil
}

func printProgress(consumer subconsumer.SubscriptionPullConsumer, topic string) error {
	positions, err := consumer.Positions(topic)
	if err != nil {
		return err
	}
	committed, err := consumer.CommittedPositions(topic)
	if err != nil {
		return err
	}
	_, err = fmt.Printf("positions=%+v committed=%+v\n", positions, committed)
	return err
}
