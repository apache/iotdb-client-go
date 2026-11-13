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

The module implements `SubscriptionSession` management and the configuration, construction, subscription management, record polling, commits, progress, seek, processor pipeline, and background connection coordination of `SubscriptionPullConsumer`. Poll response decoding, record decoding, and message release are also connected. Push consumers and built-in processors remain for subsequent steps. These implementations have not been compiled or exercised at runtime; this document describes the code and its contracts.

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

`SubscriptionConsumerConfig.OwnerEpoch == nil` means that no epoch was supplied; a pointer to zero explicitly supplies epoch 0. `DefaultSubscriptionConsumerConfig`, `DefaultSubscriptionPullConsumerConfig`, and `DefaultSubscriptionPushConsumerConfig` are available. The default Push Handler remains nil, so callers must supply a real handler. Internal normalization copies endpoint slices and optional pointers. Zero durations select defaults, negative durations return errors, and positive values below Java's minimum are raised to that minimum. Construct Pull consumers with `NewSubscriptionPullConsumer`; a Push constructor is not yet available.

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

The internal implementation covers handshakes, heartbeats, subscribe/unsubscribe, close, commits, seek, Poll and Tablet-continuation requests, and oversized-request slicing. These are connected to the public Pull consumer.

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

The `SubscriptionPushConsumer.Errors()` channel buffers at least one error. A TsFile decoding error must stop automatic polling before notification; the failed round cannot enter the Handler or ACK path. The lifecycle owner closes the channel after coordinating background senders.

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

A `SubscriptionMessageProcessor` that retains messages must implement `BufferingProcessor` so heartbeats can renew undelivered records. Seek clears only its target topic. With multiple subscribed topics, a processor without `TopicResetter` makes seek fail before server positions change. A single-topic consumer can fall back to `Reset()`. Messages returned by `Flush` pass through subsequent stages; the processor must not retain or modify messages after transferring them.

## Data Ownership and Context Contracts

- Session query collections, Consumer identity, commit contexts, and progress are caller-owned snapshots; progress maps are deep snapshots. Seek copies its input rather than retaining the caller's maps.
- Batches, columns, and row data from `SubscriptionMessage.Records()` are borrowed, read-only views valid until the message's `Release()`. Callers must copy values and byte slices that need to outlive that point.
- `Release()` is idempotent. Do not reuse borrowed batch, column, or row data after release. `Records()` returns `ErrDataReleased`, while kind, commit context, and watermark remain available.
- Nil in `Row.Values` means SQL NULL. DATE uses `time.Time`; TIMESTAMP and record timestamps use `int64` in server precision; BLOB/OBJECT use `[]byte`.
- A Push Handler must not release its message. Copy record data before returning if it is needed afterward. The callback context is canceled when delivery stops.
- The `Open(ctx)` context bounds opening only, not the background lifetime. Background work stops with `Close()`. Other operation contexts bound their corresponding operations only.
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

Remaining work includes Push consumers, built-in Watermark/ColumnAlign processors, further consumption examples, and behavior tests. Management queries reuse the root module's decoder; this step does not change its handling of malformed data. Verification has been static only: no compilation, tests, or formatting tools have run, and no runtime validation result is claimed.
