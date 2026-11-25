<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Go Subscription Client

The `github.com/apache/iotdb-client-go/v2/subscription` package family provides native Go APIs for topic management, subscription management, Pull and Push record consumption, automatic and manual commit, progress tracking, Seek, and message processors.

The subscription packages are part of the main `github.com/apache/iotdb-client-go/v2` module and share its version with the Session client.

## Requirements

- Go 1.25 or later
- An IoTDB server with subscription support enabled
- A `SubscriptionRecordHandler` topic for record consumption

Install the client module from a Go project:

```sh
go get github.com/apache/iotdb-client-go/v2@latest
```

## Packages

| Package | Contents |
| --- | --- |
| `subscription` | Data-model constants, connection configuration, and public errors |
| `subscription/session` | Subscription management session and Topic/Subscription types |
| `subscription/consumer` | Pull/Push consumers, configuration, commit, progress, Seek, and processors attachment |
| `subscription/payload` | Messages, record batches, rows, commit contexts, and progress types |
| `subscription/processor` | Processor interfaces and built-in Watermark and ColumnAlign processors |

Use aliases when local variables are named `session`, `consumer`, or `processor`:

```go
import (
    "github.com/apache/iotdb-client-go/v2/subscription"
    subconsumer "github.com/apache/iotdb-client-go/v2/subscription/consumer"
    subpayload "github.com/apache/iotdb-client-go/v2/subscription/payload"
    subprocessor "github.com/apache/iotdb-client-go/v2/subscription/processor"
    subsession "github.com/apache/iotdb-client-go/v2/subscription/session"
)
```

## Data Models

Each Session or Consumer instance uses one data model:

```go
subscription.TreeDialect
subscription.TableDialect
```

Select it in the configuration:

```go
cfg := subsession.DefaultSubscriptionSessionConfig()
cfg.Dialect = subscription.TableDialect
```

## Connection Configuration

`subscription.ConnectionConfig` is shared by sessions and consumers.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `NodeURLs` | `[]string` | `["localhost:6667"]` | Single address or cluster seed list |
| `Username` | `string` | `"root"` | User name |
| `Password` | `string` | `"root"` | Plaintext password; mutually exclusive with `EncryptedPassword` |
| `EncryptedPassword` | `string` | Empty | Encrypted password; mutually exclusive with `Password` |
| `ConnectTimeout` | `time.Duration` | `10s` | Connection and authentication timeout per seed |
| `MaxFrameSize` | `int32` | `64 MiB` | Maximum Thrift frame size |

TLS connections are not currently supported by this module.

## Quick Start

### Manage Topics

```go
cfg := subsession.DefaultSubscriptionSessionConfig()
cfg.Dialect = subscription.TreeDialect

session, err := subsession.NewSubscriptionSession(cfg)
if err != nil {
    return err
}
if err := session.Open(ctx); err != nil {
    return err
}
defer session.Close()

if err := session.CreateTopic(ctx, "all_metrics", subsession.CreateTopicOptions{
    Attributes: map[string]string{
        "format": "SubscriptionRecordHandler",
        "mode":   "snapshot",
        "path":   "root.factory.**",
    },
    IfNotExists: true,
}); err != nil {
    return err
}

topics, err := session.Topics(ctx)
if err != nil {
    return err
}
fmt.Println(topics)
```

### Pull Records

The example assumes that a table-model Topic named `metrics_topic` already exists with `format=SubscriptionRecordHandler`.

```go
cfg := subconsumer.DefaultSubscriptionPullConsumerConfig()
cfg.Consumer.Dialect = subscription.TableDialect
cfg.Consumer.ConsumerID = "go-pull-consumer"
cfg.Consumer.GroupID = "go-consumer-group"

manual := false
cfg.AutoCommit = &manual

consumer, err := subconsumer.NewSubscriptionPullConsumer(cfg)
if err != nil {
    return err
}
if err := consumer.Open(ctx); err != nil {
    return err
}
defer consumer.Close()

if err := consumer.Subscribe(ctx, "metrics_topic"); err != nil {
    return err
}

messages, err := consumer.Poll(ctx, 5*time.Second)
if err != nil {
    return err
}
if len(messages) == 0 {
    return nil
}

defer func() {
    for _, message := range messages {
        message.Release()
    }
}()

for _, message := range messages {
    batches, err := message.Records()
    if err != nil {
        return err
    }
    for _, batch := range batches {
        for row, err := range batch.Rows() {
            if err != nil {
                return err
            }
            fmt.Println(batch.Database(), batch.Target(), row.Timestamp, row.Values)
        }
    }
}

if err := consumer.Commit(ctx, messages...); err != nil {
    return err
}
```

`AutoCommit == nil` selects the default value `true`. Set a pointer to `false` for manual commit.

### Push Records

```go
cfg := subconsumer.DefaultSubscriptionPushConsumerConfig()
cfg.Consumer.Dialect = subscription.TreeDialect
cfg.Consumer.ConsumerID = "go-push-consumer"
cfg.Consumer.GroupID = "go-consumer-group"
cfg.AckStrategy = subconsumer.AckAfterConsume

cfg.Handler = func(callbackCtx context.Context, message subpayload.SubscriptionMessage) error {
    batches, err := message.Records()
    if err != nil {
        return err
    }
    for _, batch := range batches {
        for row, err := range batch.Rows() {
            if err != nil {
                return err
            }
            if err := callbackCtx.Err(); err != nil {
                return err
            }
            fmt.Println(batch.Target(), row.Timestamp, row.Values)
        }
    }
    return nil
}

consumer, err := subconsumer.NewSubscriptionPushConsumer(cfg)
if err != nil {
    return err
}
if err := consumer.Open(ctx); err != nil {
    return err
}
defer consumer.Close()

if err := consumer.Subscribe(ctx, "all_metrics"); err != nil {
    return err
}

select {
case err, ok := <-consumer.Errors():
    if ok {
        return err
    }
    return nil
case <-ctx.Done():
    return ctx.Err()
}
```

Do not call `Release` from a Push handler. The delivery loop manages message lifetime.

## Configuration

### Subscription Session

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `Connection` | `subscription.ConnectionConfig` | Connection defaults above | Endpoint and authentication |
| `Dialect` | `subscription.Dialect` | `TreeDialect` | Tree or table model |

### Shared Consumer Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `Connection` | `subscription.ConnectionConfig` | Connection defaults above | Endpoint and authentication |
| `Dialect` | `subscription.Dialect` | `TreeDialect` | Tree or table model |
| `ConsumerID` | `string` | Assigned by the server after Open | Consumer identity |
| `GroupID` | `string` | Assigned by the server after Open | Consumer group identity |
| `OwnerID` | `string` | Empty | Topic owner identity |
| `OwnerEpoch` | `*int64` | `nil` | Owner epoch; a pointer to zero sets epoch zero |
| `HeartbeatInterval` | `time.Duration` | `30s`, minimum `1s` | Provider heartbeat interval |
| `EndpointsSyncInterval` | `time.Duration` | `2m`, minimum `5s` | Endpoint synchronization interval |
| `MaxPollParallelism` | `int` | `1` | Accepted for compatibility; Poll currently runs serially |

### Pull Consumer Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `Consumer` | `SubscriptionConsumerConfig` | Shared consumer defaults | Shared consumer settings |
| `AutoCommit` | `*bool` | `true` | `nil` uses the default; explicit `false` enables manual commit |
| `AutoCommitInterval` | `time.Duration` | `5s`, minimum `500ms` | Automatic commit interval |

### Push Consumer Configuration

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `Consumer` | `SubscriptionConsumerConfig` | Shared consumer defaults | Shared consumer settings |
| `AckStrategy` | `AckStrategy` | `AckAfterConsume` | Acknowledgment timing |
| `Handler` | `ConsumeFunc` | Required | Record callback |
| `PollInterval` | `time.Duration` | `100ms`, minimum `1ms` | Delay between polling rounds |
| `PollTimeout` | `time.Duration` | `10s`, minimum `1s` | Timeout for one automatic poll |

Default configuration functions:

```go
subsession.DefaultSubscriptionSessionConfig()
subconsumer.DefaultSubscriptionConsumerConfig()
subconsumer.DefaultSubscriptionPullConsumerConfig()
subconsumer.DefaultSubscriptionPushConsumerConfig()
```

## Public APIs

### Session

```go
NewSubscriptionSession(config)
Open(ctx)
Close()
CreateTopic(ctx, name, options)
AlterTopic(ctx, name, attributes)
AlterTopicOwner(ctx, name, owner)
DropTopic(ctx, name, options)
Topics(ctx)
Topic(ctx, name)
Subscriptions(ctx, topic)
DropSubscription(ctx, id, options)
```

### Pull Consumer

```go
NewSubscriptionPullConsumer(config)
Open(ctx)
Close()
Subscribe(ctx, topics...)
Unsubscribe(ctx, topics...)
Poll(ctx, timeout, topics...)
PollWithInfo(ctx, timeout, topics...)
DrainBufferedMessages(ctx)
Commit(ctx, messages...)
CommitAsync(ctx, messages...)
AddProcessor(processor)
Info()
LatestWatermark()
AllConsumed()
LastAutoCommitError()
Positions(topic)
CommittedPositions(topic)
SeekToBeginning(ctx, topic)
SeekToEnd(ctx, topic)
Seek(ctx, topic, progress)
SeekAfter(ctx, topic, progress)
```

### Push Consumer

```go
NewSubscriptionPushConsumer(config)
Open(ctx)
Close()
Subscribe(ctx, topics...)
Unsubscribe(ctx, topics...)
Errors()
Info()
LatestWatermark()
AllConsumed()
Positions(topic)
CommittedPositions(topic)
SeekToBeginning(ctx, topic)
SeekToEnd(ctx, topic)
Seek(ctx, topic, progress)
SeekAfter(ctx, topic, progress)
```

### Messages and Records

`SubscriptionMessage` exposes:

```go
Kind()
CommitContext()
Records()
Watermark()
EstimatedSize()
Release()
```

`RecordBatch` exposes:

```go
Dialect()
Database()
Target()
ColumnCount()
ColumnName(column)
ColumnDataType(column)
ColumnCategory(column)
TimeSelected()
Len()
Timestamp(row)
Value(row, column)
Rows()
```

Use `Timestamp` and `Value` for raw record indexes. `Rows` returns the visible records in stable timestamp-ascending order.

### Progress and Commit

`payload.TopicProgress` maps a region ID to `RegionProgress`. `RegionProgress` maps a `WriterID` to `WriterProgress`.

```go
positions, err := consumer.Positions(topic)
committed, err := consumer.CommittedPositions(topic)
err = consumer.SeekAfter(ctx, topic, committed)
```

`Commit` confirms a batch synchronously. `CommitAsync` returns a channel that receives one result and then closes.

## Processors

Pull consumers support ordered processor stages through `AddProcessor`.

```go
align, err := subprocessor.NewColumnAlignProcessor(subscription.TableDialect)
if err != nil {
    return err
}

watermark, err := subprocessor.NewWatermarkProcessor(subprocessor.WatermarkProcessorConfig{
    MaxOutOfOrderness:  1000,
    Timeout:            30 * time.Second,
    StaleRegionTimeout: 2 * time.Minute,
    MaxBufferBytes:     64 * 1024 * 1024,
})
if err != nil {
    return err
}

if err := consumer.AddProcessor(align); err != nil {
    return err
}
if err := consumer.AddProcessor(watermark); err != nil {
    return err
}
```

Custom processors implement `Process`, `Flush`, and `Reset`. A processor that retains messages should implement `BufferingProcessor`. A processor used with topic-scoped Seek should implement `TopicResetter`. A processor that exposes its watermark may implement `WatermarkReporter`.

## Message Ownership

- `CommitContext()` returns a caller-owned snapshot.
- `Records()` returns borrowed, read-only batches and rows.
- Borrowed data is valid until `Release()`.
- `Release()` is idempotent. Commit metadata and watermark remain available after release.
- Pull callers should release record messages after processing and commit.
- Push handlers must not release messages. The consumer releases them after the callback.
- Copy byte slices and row values before retaining them after callback or release.

## Errors

Use `errors.Is` and `errors.As` to classify operation errors.

| Error | Meaning |
| --- | --- |
| `ErrUnsupportedTsFile` | TsFile payload decoding is unsupported |
| `ErrClosed` | The client has been closed |
| `ErrNotOpen` | The operation requires an open client |
| `ErrDataReleased` | Record buffers have already been released |
| `ErrIncompatibleMessage` | The accessor does not match the message kind |
| `ErrConsumerFenced` | The consumer identity was taken over |
| `ErrOwnerFenced` | Topic owner, epoch, or lease is invalid |
| `SubscriptionError` | Server or transport error with `Kind`, `Code`, `Message`, `Critical`, and `Cause` |

```go
var subscriptionErr *subscription.SubscriptionError
if errors.As(err, &subscriptionErr) {
    fmt.Println(subscriptionErr.Kind, subscriptionErr.Code, subscriptionErr.Critical)
}
```

Connection errors redact plaintext and encrypted passwords.

## Limitations

- Only record payloads are decoded.
- Topic management and subscription do not reject TsFile topics in advance.
- A TsFile payload returns `subscription.ErrUnsupportedTsFile` during decode.
- A failed TsFile poll is not acknowledged, does not advance positions, and does not enter processor or automatic-commit buffers.
- Push reports a TsFile decode failure through `Errors()` before invoking the handler.
- No Reader or file-management API is provided for TsFile payloads.
- TLS connections are not currently supported.

## Consumer Lifecycle

- `Open(ctx)` bounds connection establishment only; a successfully opened consumer owns its background lifetime.
- Repeated `Open` and repeated `Close` are safe. A successfully closed consumer cannot reopen.
- Pull requests execute serially. `MaxPollParallelism` is accepted but does not create parallel network polls.
- `AllConsumed()` is true only after the server confirms that no subscribed topics remain.
- A pending or failed automatic unsubscribe keeps `AllConsumed()` false.
- A retained snapshot topic waits for a termination from every expected DataNode before automatic unsubscribe; a topology change revokes earlier completion observations until the new membership completes.
- `AllConsumed()` does not imply that processor buffers are empty. Manual-commit callers should call `DrainBufferedMessages`, process and commit every returned message, and drain again until no messages remain.
- `LastAutoCommitError()` exposes the latest automatic commit failure.
- `Close()` stops new work and cancels active contexts.
- In manual-commit mode, `Close()` can return an error while processors still hold messages. Drain, process, and commit those messages before calling `Close()` again.
- Push delivery continues after transient Poll and acknowledgment errors; `Errors()` reports only terminal failures. Redelivery follows the configured `AckStrategy`.
- Push `Close()` does not wait for a still-running handler to return.

## Examples

Complete programs are in [example/subscription](../example/subscription):

```sh
cd example
go run ./subscription/tree
go run ./subscription/table
go run ./subscription/tree/incremental
go run ./subscription/table/incremental
```

## Testing

Run subscription unit tests from the repository root:

```sh
go test ./subscription/...
```

Server-backed tests are in [test/e2e](../test/e2e). Subscription tests are skipped unless `IOTDB_SUBSCRIPTION_E2E=1` is set. See the E2E guide for connection variables, server requirements, and coverage.
