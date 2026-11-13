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

# Subscription Examples

These programs belong to the standalone `github.com/apache/iotdb-client-go/example` module, alongside the Session and database/sql examples. Run commands from `example/` with Go 1.25 and a server that supports record subscriptions. This module uses local replacements for the Session and subscription clients, so keep the repository layout intact. The incremental programs additionally require server support for incremental subscriptions and per-writer ordering.

| Program | Scenario | Java reference |
| --- | --- | --- |
| [tree](tree/main.go) | Tree path and time filtering, topic management, automatic-commit Pull or record Push | `SubscriptionSessionExample.dataSubscription1` |
| [table](table/main.go) | Database/table and time filtering, table schema metadata, automatic-commit Pull or record Push | `TableModelSubscriptionSessionExample.dataSubscription` |
| [tree/incremental](tree/incremental/main.go) | Tree incremental subscription, manual commit, progress, checkpoint resume, optional processors | `ConsensusSubscriptionSessionExample` |
| [table/incremental](table/incremental/main.go) | Table incremental subscription, manual commit, progress, checkpoint resume, optional processors | `ConsensusTableModelSubscriptionSessionExample` |

The Java references are under `example/subscription/src/main/java/org/apache/iotdb` in the IoTDB checkout. The Go programs use the current topic mode name, `incremental`, for the scenarios whose Java class names contain `Consensus`. Every program explicitly requests `format=SubscriptionRecordHandler`. Java's TsFile examples are excluded because this module returns `ErrUnsupportedTsFile` when decoding file payloads.

Record Push, progress/seek, and processor usage extend the Java record examples. They demonstrate the Go APIs implemented by this module; the Java example directory does not contain equivalent record-only examples for those combinations.

## Public Package Imports

The programs use the subscription module's `session`, `consumer`, `payload`, and `processor` packages. The root `subscription` import supplies shared model constants, connection settings, and errors. Business APIs are no longer exported from the root package; there are no old-name aliases or forwarding constructors. See the [package responsibilities and Java mappings](../../subscription/README.md) for the complete ownership table and seven internal boundaries.

```go
import (
    "github.com/apache/iotdb-client-go/subscription"
    subconsumer "github.com/apache/iotdb-client-go/subscription/consumer"
    subpayload "github.com/apache/iotdb-client-go/subscription/payload"
    subprocessor "github.com/apache/iotdb-client-go/subscription/processor"
    subsession "github.com/apache/iotdb-client-go/subscription/session"
)
```

Import only what the program needs. The aliases keep constructor references clear when local variables are named `consumer` or `session`. For example, management uses `subsession.NewSubscriptionSession`, polling uses `subconsumer.NewSubscriptionPullConsumer`, callbacks accept `subpayload.SubscriptionMessage`, and processor installation uses `subprocessor.NewWatermarkProcessor`.

This import migration does not change the standalone example module path, its dependency versions, or its local replacements. The subscription and E2E directories also remain separate modules; running root-module `go test ./...` does not traverse them or this example module.

## Ordinary Record Subscriptions

The ordinary programs prepare historical sample data, create a topic with a path or database/table filter and a time range, show topic and subscription metadata, and consume records. Pull is the default and uses automatic commits of delivered messages. Use the incremental programs' manual-commit pattern when processing success must control acknowledgment. Each table batch also prints its database, table, and column metadata.

```sh
cd example
go run ./subscription/tree
go run ./subscription/table
```

Select Push to deliver records through a sequential callback. `-ack=after` confirms successful callbacks; `-ack=before` confirms the batch before callbacks. The Push examples observe `Errors()` and stop after `-duration` or a termination signal.

```sh
go run ./subscription/tree -consumer=push -ack=after -duration=10s
go run ./subscription/table -consumer=push -ack=before -duration=10s
```

The Push callback reads borrowed records without releasing the message. Pull releases records after processing; the consumer retains their commit identities for acknowledgment.

## Incremental Subscriptions

The incremental programs prepare a bootstrap record and flush it to establish the source region. They then create a topic with `mode=incremental` and `order-mode=per-writer`, subscribe, and write five new records. This order follows the Java incremental examples: the records of interest are written after the subscription exists.

```sh
go run ./subscription/tree/incremental
go run ./subscription/table/incremental
```

Each program disables automatic commits, calls `PollWithInfo`, processes records, and commits only successfully processed messages. It prints fetched and committed positions, takes an in-memory checkpoint of confirmed progress, and demonstrates `SeekAfter` once. The checkpoint is scoped to that run and is not saved to disk.

Enable the optional Pull processor chain with `-processors`:

```sh
go run ./subscription/tree/incremental -processors
go run ./subscription/table/incremental -processors
```

The chain installs `WatermarkProcessor` before the model-specific `ColumnAlignProcessor`. Its out-of-order distance is one raw server timestamp unit, and its wall-clock timeout is two seconds. Message ordering is based on each message's maximum timestamp; it does not globally sort rows across messages. The examples drain retained records and commit successfully processed output before closing a manual-commit consumer.

## Connection Settings and Resources

All programs accept `-host`, `-port`, and `-username`, defaulting to `127.0.0.1`, `6667`, and `root`. `IOTDB_PASSWORD` supplies the password; when it is unset, the default is `root`. An explicitly empty environment value remains an empty password.

```sh
IOTDB_PASSWORD='your-password' go run ./subscription/table -host=127.0.0.1 -port=6667 -username=root
```

Each run uses its own database, topic, consumer, and group. Database and topic names are generated locally; consumer identities are either supplied by the program or assigned by the server. The programs print resource and subscription information, create sample data through the existing Session client, and remove only the topic they created after closing the consumer. The sample database and records remain available for inspection. Topic cleanup and manual-commit shutdown use bounded contexts independent of a canceled polling context. Errors from processing, acknowledgment, and cleanup are reported.

These commands are provided for manual use. No compilation, tests, examples, formatting tools, or compiler/syntax checks were run during this package migration, and no runtime validation result is claimed.
