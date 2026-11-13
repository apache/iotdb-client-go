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

# Go Subscription API

The module implements `SubscriptionSession` management, Pull consumption with record polling, commits, progress, seek, processor pipelines, and background connection coordination, and Push consumption with sequential callbacks and acknowledgment strategies. Poll response decoding, record decoding, and message release are also connected. Built-in Watermark and ColumnAlign processors are available for the Pull pipeline. These implementations have not been compiled or exercised at runtime; this document describes the code and its contracts.

The module path is `github.com/apache/iotdb-client-go/subscription`. It uses Go 1.25 and maintains dependencies and versions separately from the existing `github.com/apache/iotdb-client-go/v2` Session module. Its direct dependencies are `github.com/apache/iotdb-client-go/v2` at `v2.0.0-20260914074454-a25f68aa9681`, which provides existing types such as `TSDataType` and `ColumnCategory`, and `github.com/apache/thrift` at `v0.24.0`, which provides the RPC transport and protocol. It adds no TsFile, cgo, or TLS dependency and does not change the existing Session interfaces.

The Java reference is `iotdb-client/subscription` at IoTDB commit `d9122d837610`. The supported payload format is records: this module defines no TsFile handler, download, file-management, or Reader API. Topic management and `Subscribe` do not reject TsFile topics in advance. Dispatching a file payload during decoding returns `ErrUnsupportedTsFile`; that payload must not be delivered or acknowledged.

## Configuration and Data Model Selection

Go uses `Dialect` to distinguish the tree and table models. Management and consumption each share a contract rather than duplicating Java's tree/table types and Builder hierarchy.

Java's `ISubscriptionTreeSession/ISubscriptionTableSession`, tree/table Pull interfaces, and tree/table Push interfaces have identical method signatures within each pair. Go therefore uses `SubscriptionSession`, `SubscriptionPullConsumer`, and `SubscriptionPushConsumer` for those common operations. Configuration selects the model through `Dialect`; each instance uses one model.

Only the public method contract is shared. Management sessions pass the model when connecting. Consumer handshakes carry the selected model, and record decoding distinguishes tree device paths from table databases, tables, and TAG identities. Sharing an interface does not require the two models to use the same internal implementation.

| Public Go type | Java counterpart | Go representation |
| --- | --- | --- |
| `Dialect`, `TreeDialect`, `TableDialect` | Tree/table Session and Consumer types, SQL dialect | Select the model in `SubscriptionSessionConfig` or `SubscriptionConsumerConfig` |
| `ConnectionConfig` | Session/Consumer Builder settings for nodes, authentication, connection timeout, and maximum frame size | `NodeURLs` represents either a single node or cluster seed addresses; durations use `time.Duration`; plaintext and encrypted passwords are mutually exclusive |
| `SubscriptionSessionConfig` | `SubscriptionTreeSessionBuilder`, `SubscriptionTableSessionBuilder` | Obtain defaults through `DefaultSubscriptionSessionConfig`, then pass them to `NewSubscriptionSession`; construction does not connect |
| `SubscriptionConsumerConfig` | `AbstractSubscriptionConsumerBuilder` | Shared configuration for consumer identity, ownership, heartbeats, endpoint synchronization, and parallelism |
| `SubscriptionPullConsumerConfig` | `AbstractSubscriptionPullConsumerBuilder` | `AutoCommit == nil` selects Java's default, `true`; a pointer to `false` explicitly selects manual commits |
| `SubscriptionPushConsumerConfig` | `AbstractSubscriptionPushConsumerBuilder` | Combines a Handler, ACK strategy, and polling interval and timeout |
| `AckStrategy`, `AckAfterConsume`, `AckBeforeConsume` | `AckStrategy.AFTER_CONSUME`, `BEFORE_CONSUME` | The zero value acknowledges after consumption; callback failure does not cause redelivery when acknowledgment precedes consumption |

`SubscriptionConsumerConfig.OwnerEpoch == nil` means that no epoch was supplied; a pointer to zero explicitly supplies epoch 0. `DefaultSubscriptionConsumerConfig`, `DefaultSubscriptionPullConsumerConfig`, and `DefaultSubscriptionPushConsumerConfig` are available. The default Push Handler remains nil, so callers must supply a real handler. Internal normalization copies endpoint slices and optional pointers. Zero durations select defaults, negative durations return errors, and positive values below Java's minimum are raised to that minimum. Construct consumers with `NewSubscriptionPullConsumer` or `NewSubscriptionPushConsumer`; neither constructor connects to the network.

The management session defaults are `localhost:6667`, the tree model, root/root authentication, a 64 MiB frame limit, and a ten-second connection and authentication timeout. Java management sessions do not limit connection time by default; the Go entry point uses a bounded timeout here. The constructor fills empty endpoints, username, and dialect, as well as zero timeout and frame size, validates addresses and mutually exclusive authentication, and copies the endpoint slice. Passwords are always preserved as supplied, including empty strings. Start with the default configuration function when root/root defaults are needed. To use an encrypted password, clear `Password` and set `EncryptedPassword`.

```go
cfg := subscription.DefaultSubscriptionSessionConfig()
cfg.Dialect = subscription.TableDialect
s, err := subscription.NewSubscriptionSession(cfg)
if err != nil {
    return err
}
if err := s.Open(ctx); err != nil {
    return err
}
defer s.Close()
topics, err := s.Topics(ctx)
```

Management connections own query pagination and closing while reusing the existing TsBlock decoder. They do not use the existing SessionDataSet, whose pagination uses `context.Background()`. Cancellation interrupts the underlying connection. Later operations rebuild a failed connection with the same model and authentication settings. Already-sent management statements are not automatically replayed after a connection failure; callers can query server state before deciding whether to retry. A management session can be opened again after closing, separately from the Consumer contract that prohibits reopening after a successful close.

## Internal Subscription Protocol and Providers

The internal implementation covers handshakes, heartbeats, subscribe/unsubscribe, close, commits, seek, Poll and Tablet-continuation requests, and oversized-request slicing. Pull and Push share these internal capabilities.

- Primitive encoding uses Java's big-endian layout. Commit contexts use version 2 and retain legacy commit fields plus optional Writer identity and progress. Topic/Region/Writer progress follows the original protocol structure.
- Topic sets in subscribe and Poll requests use Java ObjectSet string tags, separately from the ordinary StringList used by heartbeat unsubscribe lists. Set encoding is sorted for stability. Null strings map to empty strings in existing Go string fields.
- Each Provider owns a plain Thrift connection and a serialized RPC channel. Opening includes authentication, frame negotiation, and the subscription handshake, all bounded by the opening timeout. The Provider retains server-assigned consumer identities and sends instance identity and ownership information in the handshake.
- The frame limit is the smaller of the local and server limits, with protocol space reserved. Older servers without the property field or property-probe RPC fall back to the local limit. All slices of a sliceable request retain the same connection, order, and batch identity.
- Heartbeats batch buffered commit contexts by message size. Commits reserve response space and send batches; a later batch failure still returns earlier accepted contexts and progress. A response without a body retains an empty result rather than implying that the entire batch succeeded.
- Status handling distinguishes ordinary errors, owner fencing, consumer fencing, connection replacement, and critical errors. A Poll diagnostic reporting a missing consumer-group Broker invalidates the old connection for Consumer recovery. Late responses cannot reactivate an invalidated connection.
- Providers decode every nonempty Poll response body and retain record, error, termination, and watermark events. File responses return `ErrUnsupportedTsFile` during type dispatch. Mixed responses do not return partially decoded records, and file polling is not implemented.

## Record Messages and Decoding

Records use the Tablet serialization format of Java's current `2.5.1-261023-SNAPSHOT` dependency, separately from insert RPC and query TsBlock formats. They do not depend on TsFile Go or native libraries.

- Supported types are BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT, STRING, TIMESTAMP, DATE, BLOB, and OBJECT, preserving TAG/FIELD/ATTRIBUTE categories. TEXT/STRING values are strings, BLOB/OBJECT values are `[]byte`, and DATE values are `time.Time` at midnight UTC.
- Bitmaps and absent value columns determine NULL values. A Binary absence marker first becomes empty content, after which the Bitmap is applied. Empty strings and empty BLOBs are not automatically converted to NULL. Dates follow Java's integer date representation.
- `Timestamp/Value` use original row indexes. `Rows()` iterates a separate stable view in ascending timestamp order, retaining original order for equal timestamps. Row value slices are not reused between iterations, and internal locks are not held while invoking the caller's iteration function.
- Tree records always expose timestamps. Table records first use database/table-specific time projection, falling back to the message-level flag. Older responses without the projection suffix retain Java's default of selecting time.
- Message release is idempotent and clears record buffers. Commit contexts and their optional Writer metadata remain available as independent snapshots, as do watermark and message kind. Accessing data through previously borrowed batches after release returns an error.
- Record continuation uses the original DataNode Provider, checks the complete commit context, and merges data and time projections across pages. The final count is checked against the number of Tablets, not rows. Outdated or unavailable partial deliveries are not exposed.
- Empty-record deliveries and termination events retain their contexts at the decoding layer. Pull handles empty-message ACKs and termination unsubscribe only after confirming that the entire poll round has no TsFile or fencing error. Server ERROR payloads retain their severity, and watermarks can become processor messages.

## Pull Consumption

`NewSubscriptionPullConsumer` copies and validates configuration without connecting. `Open(ctx)` opens seed connections and discovers other nodes. Background lifetime is independent of the opening context. `Subscribe/Unsubscribe` update subscriptions, and `AddProcessor` adds processors in order.

```go
cfg := subscription.DefaultSubscriptionPullConsumerConfig()
cfg.Consumer.Dialect = subscription.TableDialect
autoCommit := false
cfg.AutoCommit = &autoCommit
consumer, err := subscription.NewSubscriptionPullConsumer(cfg)
if err != nil {
    return err
}
if err := consumer.Open(ctx); err != nil {
    return err
}
defer consumer.Close()
if err := consumer.Subscribe(ctx, "measurements"); err != nil {
    return err
}
messages, err := consumer.Poll(ctx, 5*time.Second)
if err != nil {
    return err
}
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
            fmt.Println(row.Timestamp, row.Values)
        }
    }
    message.Release()
}
return consumer.Commit(ctx, messages...)
```

- A positive Poll timeout starts at entry and covers waiting for the serialized foreground gate and the network budget. Processor callbacks must return promptly. Negative timeouts return argument errors. A zero timeout does not wait for a busy foreground gate or perform synchronous provider recovery; it sends one round of requests to available Providers with server timeout 0. Network I/O can still wait and is bounded by the caller's context. Caller context cancellation always returns an error.
- Multiple topics are partitioned according to `MaxPollParallelism`. When only Poll's own budget expires, already completed healthy partitions may still be delivered while incomplete sibling results are discarded. TsFile or current-consumer fencing makes the entire round fail, without passing records to processors, automatic commit tracking, or fetch positions. Custom processor code must still avoid blocking indefinitely.
- Heartbeats renew records buffered by Pull processors. Automatic commits track only records delivered to the caller and wait at least one configured interval. Manual `Commit` can run concurrently with Poll; `CommitAsync` reports its result once through a buffered channel.
- Commit and seek server requests and local progress updates are mutually exclusive. Partial acceptance is applied first and accepted contexts leave the pending queue; only the remainder is retried. NACKs are not replayed. A writer ACK pending after node loss is redirected only after the same Topic/Region reappears through a healthy node.
- Heartbeats are scheduled independently per Provider. Endpoint synchronization and connection failure trigger recovery. Background recovery attempts are at least five seconds apart. Late old-connection responses cannot reactivate an old Provider or incorrectly fence the replacement connection's Consumer.
- `Close` interrupts blocked polling and stops background work. In manual mode, buffered processor messages make closing return an error while preserving the drain path. Call `DrainBufferedMessages`, process and commit its records, then close again. Automatic mode makes a best-effort commit of delivered and drained records during closing. Commit failures return errors; a successful close does not imply that all historical messages were processed.

## Push Consumption

`NewSubscriptionPushConsumer` normalizes configuration once, requires a non-nil Handler, and does not connect. It reuses the manual Pull core for providers, progress, and polling, without installing processors or enabling automatic commits. `Open(ctx)` starts one fixed-delay polling loop; its background lifetime is independent of the opening context. Handlers run sequentially, and the next round starts only after the current round and `PollInterval` delay finish.

```go
cfg := subscription.DefaultSubscriptionPushConsumerConfig()
cfg.Consumer.Dialect = subscription.TableDialect
cfg.AckStrategy = subscription.AckAfterConsume
cfg.Handler = func(callbackCtx context.Context, message subscription.SubscriptionMessage) error {
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
            fmt.Println(row.Timestamp, row.Values)
        }
    }
    return nil
}
consumer, err := subscription.NewSubscriptionPushConsumer(cfg)
if err != nil {
    return err
}
if err := consumer.Open(ctx); err != nil {
    return err
}
defer consumer.Close()
if err := consumer.Subscribe(ctx, "measurements"); err != nil {
    return err
}
select {
case err, ok := <-consumer.Errors():
    if !ok {
        return nil
    }
    return err
case <-ctx.Done():
    return ctx.Err()
}
```

- `AckBeforeConsume` acknowledges the whole batch before invoking any Handler. A partial or failed ACK prevents callbacks for that batch. A later Handler error or panic does not trigger NACK under this strategy.
- `AckAfterConsume` runs all callbacks in order, then acknowledges the successful group and nacks the failed group. An ACK error skips the subsequent NACK request. A nil Handler result means success; an error or panic means consumption failed, not that the background loop has terminated.
- Ordinary network failures are retried in a later polling round. TsFile decoding, fencing, and critical background errors stop delivery. `Errors()` buffers one result, sends the first terminal error once, and is closed by the loop when it exits. Normal closing does not invent a terminal error. A TsFile failure never enters the Handler or acknowledgment path.
- Handlers run without an internal consumer operation gate and may call Subscribe, Seek, or Close. Push seek invalidates pending delivery for the target topic before sending the request. That topic's active callback context is canceled after the seek request finishes, whether it succeeds or fails, so a Handler can use its own context to call Seek without canceling that request at entry. An already-started callback can run until it returns; later callbacks and ACKs for the old delivery generation are skipped. A failed seek does not restore the old batch because server positions may already have changed partially. Already accepted acknowledgments are not rolled back.
- `Close` cancels the active callback context and synchronously closes core connection resources, but does not wait for user Handler code to return. This permits calling Close from inside a Handler without waiting on itself. The active message is released only after its callback returns, so borrowed data remains valid during that callback. Handlers should return promptly after cancellation. If a callback has not returned, the loop and `Errors()` channel may finish after Close returns.
- Push messages are owned by the delivery loop. Do not call `Release` in a Handler; copy any record data that must outlive the callback. Watermarks update consumer state but do not reach record Handlers. Push does not expose the Pull processor pipeline.

## Management Sessions

`SubscriptionSession` corresponds to Java's `ISubscriptionTreeSession` and `ISubscriptionTableSession`. Methods take an explicit `context.Context`, while parameter structures express existence conditions.

| Go method or type | Java counterpart | Consolidation |
| --- | --- | --- |
| `Open(ctx)`, `Close()` | `open()`, `AutoCloseable.close()` | Opening is cancelable; closing owns resource release |
| `CreateTopic(ctx, name, options)`, `CreateTopicOptions` | Four `createTopic/createTopicIfNotExists` overloads | `Attributes` carries optional properties; `IfNotExists` requests conditional creation |
| `AlterTopic(ctx, name, attributes)` | `alterTopic(name, properties)` | Attributes use `map[string]string` |
| `AlterTopicOwner(ctx, name, owner)`, `TopicOwner` | Two `alterTopicOwner` overloads | A `LeaseDuration` pointer represents an optional lease duration |
| `DropTopic(ctx, name, options)`, `DropOptions` | `dropTopic/dropTopicIfExists` | `IfExists` requests conditional deletion |
| `Topics(ctx)`, `Topic(ctx, name)` | `getTopics()`, `getTopic(name)` | Collections are slices; an absent single topic returns `found == false` |
| `Subscriptions(ctx, topic)` | `getSubscriptions()`, `getSubscriptions(topic)` | `topic == ""` lists all subscriptions |
| `DropSubscription(ctx, id, options)` | `dropSubscription/dropSubscriptionIfExists` | Reuses `DropOptions` |
| `Topic` | `model.Topic` | Retains the server's raw attribute text rather than assuming it is directly parseable as a map |
| `Subscription` | `model.Subscription` | Retains the server's raw `ConsumerIDs` text rather than assuming it is CSV |

## Consumers, Commits, and Progress

`SubscriptionConsumer` is the common interface for Pull and Push. It embeds `Positioner`, so both share progress and seek contracts, including public methods inherited by Java's concrete consumers from their base class.

| Go method or type | Java counterpart | Consolidation |
| --- | --- | --- |
| `NewSubscriptionPullConsumer(config)` | `SubscriptionTreePullConsumerBuilder.build`, `SubscriptionTablePullConsumerBuilder.build` | One constructor with `Dialect`; validates configuration without connecting |
| `NewSubscriptionPushConsumer(config)` | `SubscriptionTreePushConsumerBuilder.build`, `SubscriptionTablePushConsumerBuilder.build` | One constructor with `Dialect` and a required Go Handler; validates configuration without connecting |
| `SubscriptionConsumer.Open(ctx)`, `Close()` | Each Consumer's `open/close` | Shared lifecycle interface |
| `Subscribe(ctx, topics...)`, `Unsubscribe(ctx, topics...)` | Single-topic, array, and collection overloads | Variadic topic names |
| `Info()`, `SubscriptionConsumerInfo` | `getConsumerId/getConsumerGroupId/getOwnerId/getOwnerEpoch` | Returns a credential-free identity snapshot, including server-assigned IDs |
| `LatestWatermark()` | `getLatestWatermarkTimestamp()` | An additional `ok` distinguishes an unavailable watermark from a valid timestamp |
| `AllConsumed()` | `allTopicMessagesHaveBeenConsumed()` | True without subscriptions; an active unbounded topic prevents completion |
| `SubscriptionPullConsumer.Poll(ctx, timeout, topics...)` | Four `poll` overloads | `time.Duration` replaces millisecond and Java Duration overloads; omitted topics mean all current subscriptions |
| `PollWithInfo(ctx, timeout, topics...)`, `PollResult` | Concrete Pull classes' `pollWithInfo`, `payload.PollResult` | Returns messages, buffered count, and processor watermark together |
| `DrainBufferedMessages(ctx)` | `drainBufferedMessages()` | Explicitly drains processors or pending delivery buffers |
| `Commit(ctx, messages...)` | Single/batch `commitSync` | Variadic messages for synchronous commits |
| `CommitAsync(ctx, messages...)` | `CompletableFuture` and `AsyncCommitCallback` overloads | Returns `<-chan error`; callers can wait directly or wait in a goroutine before invoking a callback |
| `AddProcessor(processor)` | Concrete Pull classes' `addProcessor` | Adds processing stages in order and returns an error rather than a chained Builder |
| `ConsumeFunc`, `SubscriptionPushConsumerConfig.Handler` | `ConsumeListener.onReceive`, `ConsumeResult` | `func(ctx, message) error`; nil means success and a non-nil error means failure |
| `SubscriptionPushConsumer.Errors()` | Explicit Go error outlet for Java's automatic-poll exception path | Reports the first terminal background error once; ordinary Handler failures are not terminal |
| `Positioner.Positions(topic)` | `positions(topic)` | Returns a deep snapshot of current fetch positions |
| `CommittedPositions(topic)` | `committedPositions(topic)` | Separately retains confirmed, recoverable positions |
| `SeekToBeginning(ctx, topic)`, `SeekToEnd(ctx, topic)` | Methods with the same names | Retains both positioning operations |
| `Seek(ctx, topic, progress)`, `SeekAfter(ctx, topic, progress)` | `seek/seekAfter` | Replays from progress or resumes after it, respectively |
| `TopicProgress`, `RegionProgress` | Classes with the same names | Topic → Region → Writer maps rather than a single offset |
| `WriterID`, `WriterProgress` | `WriterId`, `WriterProgress` | Comparable Writer IDs serve as map keys; progress retains physical time and local sequence |
| `WriterPosition`, `CommitContext`, `InvalidCommitID` | Optional writer progress and legacy commit metadata in `SubscriptionCommitContext` | Preserves paired writer identity and progress, routing, restart, and seek-generation information |

`CommitAsync` returns a channel with capacity 1. It sends one result, nil on success, then closes. An unread result cannot block cleanup. Canceling the context does not undo commits accepted by the server, and a synchronous or asynchronous commit error does not mean that the whole batch failed.

The `SubscriptionPushConsumer.Errors()` channel buffers one error. A TsFile decoding error stops automatic polling before notification; the failed round cannot enter the Handler or ACK path. The polling loop owns channel closure. If Close cancels a still-running Handler, channel closure can occur after Close returns, when that Handler returns and the loop finishes.

## Messages, Records, and Processors

| Go method or type | Java counterpart | Go representation |
| --- | --- | --- |
| `SubscriptionMessage`, `SubscriptionMessageKind`, `RecordMessage`, `WatermarkMessage` | `SubscriptionMessage`, record and watermark message kinds | Public kinds are independent of numeric wire types; Pull and Push deliver records only, while processors can receive watermarks |
| `SubscriptionMessage.Kind()`, `CommitContext()` | `getMessageType/getCommitContext` | Returns the public kind and a commit-context snapshot |
| `SubscriptionMessage.Records()` | `getResultSets/getRecordTabletIterator`, `SubscriptionRecordHandler` | Returns `[]RecordBatch` across databases and Tablets without reproducing Java's ResultSet hierarchy |
| `SubscriptionMessage.Watermark()` | `getWatermarkTimestamp()` | `ok` identifies a watermark message |
| `EstimatedSize()`, `Release()` | `estimateSize/removeUserData` | Releases record buffers while retaining kind and commit identity, allowing a later commit |
| `RecordBatch` | `SubscriptionRecordHandler.SubscriptionResultSet` and its Tablet view | Exposes model, database, target, column metadata, time projection, row count, and indexed access |
| `Column` | Measurement schema, column type, and category | Reuses `client.TSDataType/ColumnCategory`; time is represented separately |
| `RecordBatch.Rows()`, `Row` | Iteration views of `ResultSet`, `RowRecord`, and `TSRecord` | Uses `iter.Seq2[Row, error]` with zero-based indexes |
| `RecordBatch.TimeSelected()`, `Row.Timestamp` | Message-level and per-table time-selection metadata | Each batch retains its projection; `Row.Timestamp == nil` when time is not selected |
| `RecordBatch.Timestamp(row)` | Raw Tablet timestamp access | Processors can read raw timestamps even when time is not projected |
| `SubscriptionMessageProcessor.Process/Flush/Reset` | Methods with the same roles in `SubscriptionMessageProcessor` | Processing and flushing explicitly return errors; consumers invoke each processor serially |
| `BufferingProcessor.Buffered()`, `ProcessorBuffer` | `getBufferedCount/getBufferedCommitContexts` | An optional buffering interface reports both the count and all renewable contexts |
| `TopicResetter.ResetTopic(topic)` | `supportsTopicScopedReset/reset(topic)` | Interface implementation replaces Java's capability flag |
| `WatermarkReporter.Watermark()` | `WatermarkProcessor.getWatermark()` | Optionally exposes a processor's ordering watermark, distinct from the Consumer's latest server watermark |
| `NewWatermarkProcessor(config)`, `WatermarkProcessorConfig` | Both `WatermarkProcessor` constructors | One configuration structure; event-time distance retains server precision, while timeout fields use `time.Duration` |
| `WatermarkProcessor.Process/Flush/Buffered/Reset/ResetTopic/Watermark` | `process/flush/getBufferedCount/getBufferedCommitContexts/reset/getWatermark` | Buffers whole messages by maximum timestamp, renews every retained commit context, and resets by topic |
| `NewColumnAlignProcessor(dialect)` | Both `ColumnAlignProcessor` constructors and its nested `Dialect` | Reuses the module's `Dialect`; empty selects tree and table requires `TableDialect` |
| `ColumnAlignProcessor.Process/Flush/Reset/ResetTopic` | Methods with the same roles | Preserves delivery identity and returns independently owned record buffers with NULLs forward-filled |

A `SubscriptionMessageProcessor` that retains messages must implement `BufferingProcessor` so heartbeats can renew undelivered records. Seek clears only its target topic. With multiple subscribed topics, a processor without `TopicResetter` makes seek fail before server positions change. A single-topic consumer can fall back to `Reset()`. Messages returned by `Flush` pass through subsequent stages; the processor must not retain or modify messages after transferring them.

## Built-in Pull Processors

Create processors and install them with `AddProcessor` before the first Poll. A processor instance belongs to one consumer. Installation order determines pipeline order; putting watermark ordering before column alignment lets aligned records benefit from message reordering.

```go
// This example assumes the server uses millisecond timestamps.
watermark, err := subscription.NewWatermarkProcessor(subscription.WatermarkProcessorConfig{
    MaxOutOfOrderness: 5000,
    Timeout: 30 * time.Second,
})
if err != nil {
    return err
}
alignment, err := subscription.NewColumnAlignProcessor(subscription.TableDialect)
if err != nil {
    return err
}
if err := consumer.AddProcessor(watermark); err != nil {
    return err
}
if err := consumer.AddProcessor(alignment); err != nil {
    return err
}
```

`WatermarkProcessor` tracks sources by topic, DataNode ID, and region ID, matching Java's region identity. Its watermark is the minimum latest timestamp among active sources minus `MaxOutOfOrderness`. Repeated or older timestamps do not refresh source activity. Zero `StaleRegionTimeout` selects 30 seconds, a negative value disables stale-source exclusion, and zero `MaxBufferBytes` selects 64 MiB. Negative event-time distance, timeout, or byte limit is rejected. `Timeout == 0` immediately flushes remaining buffered messages on a Process call without ordered output. The timeout is evaluated on Process calls, including empty calls from Poll; no independent timer goroutine is installed.

Messages are ordered by their maximum raw timestamp and emitted whole when that maximum is at most the watermark. This does not globally sort rows across messages or batches. Equal message maxima have no guaranteed order. Capacity overflow force-flushes all remaining messages after normal emission. A timeout force-flushes when no message was emitted and the interval since the previous emission has elapsed. When every source is stale, the previous watermark remains in effect. New sources and topic resets can move the watermark backward, matching Java. Subtraction saturates at the minimum timestamp instead of overflowing. Empty record messages use the current wall-clock milliseconds, matching Java's fallback; actual record and watermark timestamps retain server precision.

`Buffered()` snapshots the message count and every committable context for heartbeat renewal. `Flush()` transfers buffered records in maximum-timestamp order without clearing source progress. `Reset()` releases all retained records and clears ordering state; `ResetTopic(topic)` releases only that topic and recomputes ordering from the remaining sources. Reset discards unconfirmed records without acknowledging them. `Watermark()` returns `ok == false` before progress is known or when reset leaves no active sources.

`ColumnAlignProcessor` preserves raw row order, as Java does. It fills a NULL only from a cached non-null value with a strictly earlier timestamp; a non-null observation updates the cache at equal or greater timestamps. Columns are identified by name and data type, so reordered columns remain independent and changed types cannot reuse incompatible values. Tree state is isolated by topic and device path. Table state is isolated by topic, database, table name, and ordered TAG values per row. Trailing NULL TAGs are ignored as in TsFile device IDs, while interior NULL and empty strings remain distinct. Tables without TAGs and those with all TAGs NULL still have a device identity.

Alignment reads inputs through the existing public RecordBatch API, copies record buffers, and returns replacement messages with the same commit contexts and projection. It releases consumed originals only after all input access succeeds. Input errors and dialect mismatches leave inputs and cache unchanged. Cache byte values and filled byte values are independently copied so earlier Release calls cannot invalidate later records. Watermarks pass through unchanged. It buffers no messages, so Flush is empty; Reset and ResetTopic clear the corresponding cached values. The per-device value cache is not size-bounded. The processor borrows no message storage across calls.

## Data Ownership and Context Contracts

- Session query collections, Consumer identity, commit contexts, and progress are caller-owned snapshots; progress maps are deep snapshots. Seek copies its input rather than retaining the caller's maps.
- Batches, columns, and row data from `SubscriptionMessage.Records()` are borrowed, read-only views valid until the message's `Release()`. Callers must copy values and byte slices that need to outlive that point.
- `Release()` is idempotent. Do not reuse borrowed batch, column, or row data after release. `Records()` returns `ErrDataReleased`, while kind, commit context, and watermark remain available.
- Nil in `Row.Values` means SQL NULL. DATE uses `time.Time`; TIMESTAMP and record timestamps use `int64` in server precision; BLOB/OBJECT use `[]byte`.
- A Push Handler must not release its message. Copy record data before returning if it is needed afterward. The callback context is canceled when delivery stops or after a seek request for its topic finishes, regardless of that request's outcome. Close does not wait for the Handler; its message remains valid until the Handler returns.
- The `Open(ctx)` context bounds opening only, not the background lifetime. Close stops new work and cancels active work. Other operation contexts bound their corresponding operations only; Push user code can finish after Close returns.
- A positive Pull timeout bounds foreground queue waiting and network polling, subject to an earlier context deadline. Zero has the single-round behavior described above; processor callbacks must return promptly. Operation timeouts use `time.Duration` without changing message timestamp units.
- Repeated Consumer `Open` while open and repeated `Close` after closing are harmless. A successfully closed Consumer cannot reopen. A failed close requiring buffered data to be drained preserves the drain-and-commit recovery path.

## Errors and Remaining Work

| Go sentinel error | Java counterpart or behavior |
| --- | --- |
| `ErrUnsupportedTsFile` | The excluded TsFile handler; returned only while decoding a file payload, not during management or subscribe |
| `ErrClosed`, `ErrNotOpen` | Consumer lifecycle checks |
| `ErrDataReleased` | Record access after user data release |
| `ErrIncompatibleMessage` | Incompatible payload access represented by `SubscriptionIncompatibleHandlerException` |
| `ErrConsumerFenced`, `ErrOwnerFenced` | `SubscriptionConsumerFencedException`, `SubscriptionOwnerFencedException` |

Operation errors can wrap these sentinels; callers identify them with `errors.Is`. Unsupported TsFile errors must not trigger connection retries or file-payload acknowledgments. Pull returns an error and no messages for that round, including records obtained by parallel branches. That round must not advance positions or enter processor or automatic-commit buffers. Previously delivered records and confirmed progress are not rolled back. Callers can unsubscribe the offending topic and poll again.

The built-in Watermark/ColumnAlign processors are implemented; further consumption examples, behavior tests, and runtime verification are pending. Management queries reuse the root module's decoder; this step does not change its handling of malformed data. Verification has been static only: no compilation, tests, or formatting tools have run, and no runtime validation result is claimed.
