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

These programs are part of the independent `github.com/apache/iotdb-client-go/example` module. They demonstrate the Go subscription client with Pull and Push consumers, manual commit, progress, Seek, and processors.

Run the commands from the `example/` directory with Go 1.25 or later and a subscription-enabled IoTDB server. This module uses local replacements for the Session and subscription client modules, so keep the repository layout intact.

## Programs

| Program | Scenario | APIs demonstrated |
| --- | --- | --- |
| [tree](tree/main.go) | Tree path/time filtering and topic management | Session management, Pull/Push record consumption, automatic commit |
| [table](table/main.go) | Table database/table/time filtering and schema metadata | Session management, Pull/Push record consumption, automatic commit |
| [tree/incremental](tree/incremental/main.go) | Tree incremental records with checkpoint recovery | Manual commit, progress, SeekAfter, optional processors |
| [table/incremental](table/incremental/main.go) | Table incremental records with checkpoint recovery | Manual commit, progress, SeekAfter, optional processors |

## Imports

```go
import (
    "github.com/apache/iotdb-client-go/subscription"
    subconsumer "github.com/apache/iotdb-client-go/subscription/consumer"
    subpayload "github.com/apache/iotdb-client-go/subscription/payload"
    subprocessor "github.com/apache/iotdb-client-go/subscription/processor"
    subsession "github.com/apache/iotdb-client-go/subscription/session"
)
```

The root `subscription` package contains shared model constants, connection configuration, and public errors. Management uses `subsession`, consumption uses `subconsumer`, record values use `subpayload`, and processors use `subprocessor`.

## Ordinary Record Subscriptions

The ordinary programs create sample data and a `SubscriptionRecordHandler` topic, subscribe, consume records, and print topic/subscription metadata.

```sh
cd example
go run ./subscription/tree
go run ./subscription/table
```

Pull is the default consumer mode and uses automatic commit. Process a delivered batch according to application requirements; automatic commit confirms it after the configured interval.

Push delivery runs on the consumer background loop and invokes a sequential callback:

```sh
go run ./subscription/tree -consumer=push -ack=after -duration=10s
go run ./subscription/table -consumer=push -ack=before -duration=10s
```

- `-ack=after` acknowledges successful callbacks and nacks failed callbacks.
- `-ack=before` acknowledges the batch before invoking callbacks.
- Push handlers must not release the message.
- Pull callers should release messages after processing and commit.

## Incremental Subscriptions

The incremental programs create a bootstrap record before subscribing, then write new records through the incremental path. They use `PollWithInfo`, manual commit, `Positions`, `CommittedPositions`, and `SeekAfter`.

```sh
go run ./subscription/tree/incremental
go run ./subscription/table/incremental
```

The checkpoint is kept in memory for the current run; the programs do not persist it to disk.

Enable the optional processor chain with `-processors`:

```sh
go run ./subscription/tree/incremental -processors
go run ./subscription/table/incremental -processors
```

The chain installs `WatermarkProcessor` and the model-specific `ColumnAlignProcessor`. It demonstrates message-level watermark ordering and in-place column alignment.

## Connection Settings

The programs accept `-host`, `-port`, and `-username`, defaulting to `127.0.0.1:6667` and `root`. `IOTDB_PASSWORD` supplies the password and defaults to `root` when unset.

```sh
IOTDB_PASSWORD='your-password' go run ./subscription/table \
  -host=127.0.0.1 \
  -port=6667 \
  -username=root
```

## Resources and Cleanup

Each run uses unique resource names. The programs:

- Open the subscription session and consumer with bounded contexts.
- Create and close their own consumer subscriptions.
- Drop the generated topic after the consumer closes.
- Leave the sample database intact for inspection.
- Report processing, acknowledgment, and cleanup errors.

## Limitations

- The examples use record payloads only.
- TsFile payload decoding returns `subscription.ErrUnsupportedTsFile`.
- The subscription client does not provide a TsFile Reader or file-management API.
- TLS connections are not currently supported.

See the [subscription client guide](../../subscription/README.md) for configuration defaults, message ownership, errors, and the complete public API.
