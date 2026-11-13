<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Go client E2E tests

This directory is an independent Go module requiring Go 1.25 or later. Its `go.mod` uses local `replace` directives for both production modules: the root Session client (`../..`) and the subscription client (`../../subscription`). Keep the repository layout intact when running it.

A `go test ./...` command at the repository root does not traverse this nested module, the subscription module, or the independent [example module](../../example/README.md). Run subscription unit tests from the subscription module:

```sh
cd subscription
go test ./...
```

Run the complete E2E module from the repository root with:

```sh
cd test/e2e
go test -v ./...
```

The existing Session suites use the Docker hostname `iotdb` and their existing fixed connection settings. For a locally managed server, select only the subscription cases:

```sh
cd test/e2e
IOTDB_SUBSCRIPTION_E2E=1 IOTDB_SUBSCRIPTION_NODE_URLS=127.0.0.1:6667 go test -v -run '^TestSubscription' ./...
```

The commands above are instructions for the user; they were not executed during this change.

## Subscription Package Layout

The subscription cases import management through `session`, Pull/Push APIs through `consumer`, and message/commit/progress contracts through `payload`. Shared models, connection settings, and sentinel errors stay in the root `subscription` package. Processor APIs live in `processor`. Old root business exports have no compatibility aliases or constructor facade. See the [subscription package guide](../../subscription/README.md) for ownership and the internal boundaries; this import migration changes no E2E scenarios or module paths/dependency versions.

The subscription module's existing 19 unit-test cases were moved with the implementation: protocol and real-message lifecycle cases are under `internal/protocol`, while processor behavior uses the external `processor_test` package under `processor`. Run them from `subscription` with the command above. The E2E module continues to contain separate server-backed tests and the existing Session suites.

## Subscription server requirements

Use a server with subscription support enabled. Incremental cases additionally require WAL subscriptions and a compatible non-Ratis DataRegion consensus protocol, such as the supported IoTConsensus configuration of the target server. Merely exposing the normal Session RPC endpoint does not establish that these subscription features are available.

Subscription cases skip unless `IOTDB_SUBSCRIPTION_E2E=1` is set. Once selected, connection errors, disabled subscription support, unsupported topic modes, and unavailable WAL features fail the tests; an empty poll does not count as successful delivery.

| Environment variable | Behavior |
| --- | --- |
| `IOTDB_SUBSCRIPTION_E2E` | Set exactly to `1` to enable subscription tests. |
| `IOTDB_SUBSCRIPTION_NODE_URLS` | Comma-separated `host:port` seeds; defaults to the existing E2E `nodeUrls` value (`iotdb:6668,iotdb:6667,iotdb:6669`). Whitespace around seeds is removed. |
| `IOTDB_SUBSCRIPTION_USERNAME` | Subscription and data-writer username; defaults to `root` when unset or empty. |
| `IOTDB_SUBSCRIPTION_PASSWORD` | Password when present, including an explicitly empty value. Otherwise falls back to `IOTDB_PASSWORD`, then `root`. |
| `IOTDB_PASSWORD` | Password fallback when `IOTDB_SUBSCRIPTION_PASSWORD` is absent; an explicitly empty value is preserved. |

Each test uses unique resource names and closes consumers before dropping topics and data. Subscription operations have bounded contexts, with independent contexts for topic cleanup. The existing Session and `ITableSession` data preparation/cleanup APIs do not accept per-statement contexts, so those calls do not inherit the subscription test deadline.

## Coverage and Java references

`subscription_tree_test.go` covers topic DDL, path/time filtering, snapshot completion with automatic commits, incremental checkpoint replay, Push callback failure/redelivery with confirmed writer progress, and explicit TsFile rejection during Pull.

`subscription_table_test.go` covers same-named tree/table topic isolation, database/table/time filtering with snapshot completion, incremental column projection that excludes time, exact checkpoint-tail replay, Push callback failure/redelivery with confirmed writer progress, and explicit TsFile rejection during Push. Projected table records check both the visible absence of time and the public raw timestamp/value access.

Comments next to each case identify the relevant Java IT file and method under `integration-test/src/test/java/org/apache/iotdb/subscription/it` in the reference IoTDB checkout. Table checkpoint replay extends the Java tree seek scenario. Callback failure recovery and the Go TsFile rejection contract are additional Go coverage. Java source references describe the intended behavior; some reference cases are ignored and do not prove runtime compatibility.

## Docker entry point

The existing `make e2e_test` and `make e2e_test_for_parent_git_repo` workflows still prepare the IoTDB distribution and start `test/e2e/docker-compose.yml`. The client container runs tests from this independent module using Go 1.25.

The current Compose file does not forward subscription environment variables or configure the server for subscription/WAL support. Its default execution therefore runs the legacy Session suites and skips subscription cases. To run subscription cases in containers, supply your own compatible server configuration and a Compose override that explicitly forwards `IOTDB_SUBSCRIPTION_E2E=1`, the seed list, and any credentials to the client service. Setting these variables only in the host shell does not forward them through the existing Compose file.

No compilation, tests, examples, formatting tools, or compiler/syntax checks were run during this package migration. Runtime results remain unverified.
