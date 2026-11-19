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

# Go Client E2E Tests

This directory contains Session and subscription E2E tests in the main Go module. It requires Go 1.25 or later.

## Unit Tests

Run subscription unit tests from the repository root:

```sh
go test ./subscription/...
```

## E2E Tests

Run all Session and subscription E2E tests from the repository root:

```sh
go test -v ./test/e2e
```

The subscription cases are discovered by the normal test run but skip unless `IOTDB_SUBSCRIPTION_E2E=1` is set.

## Subscription Test Configuration

```sh
cd "$(git rev-parse --show-toplevel)"
IOTDB_SUBSCRIPTION_E2E=1 \
IOTDB_E2E_NODE_URLS=iotdb:6668,iotdb:6667,iotdb:6669 \
IOTDB_E2E_USERNAME=root \
IOTDB_E2E_PASSWORD=root \
go test -v -run '^TestSubscription' ./test/e2e
```

| Environment variable | Default | Description |
| --- | --- | --- |
| `IOTDB_SUBSCRIPTION_E2E` | Unset; subscription tests skip | Exactly `1` enables subscription tests |
| `IOTDB_E2E_NODE_URLS` | `iotdb:6668,iotdb:6667,iotdb:6669` | Comma-separated Session/subscription seed addresses |
| `IOTDB_E2E_USERNAME` | `root` | Shared user name; `IOTDB_USERNAME` is a fallback |
| `IOTDB_E2E_PASSWORD` | `root` | Shared password; `IOTDB_PASSWORD` is a fallback |

An explicitly empty password remains empty. Whitespace around node addresses is ignored.

## Server Requirements

Use an IoTDB server with subscription support enabled. Incremental tests additionally require WAL subscriptions and a compatible consensus protocol.

The Docker server startup script enables `subscription_enabled` and assigns a nonzero subscription share in `chunk_timeseriesmeta_free_memory_proportion`. A separately managed server needs equivalent configuration sized for its available memory and subscription queues.

Subscription tests fail on connection errors, disabled subscription support, unsupported Topic modes, and unavailable WAL features. An empty poll is not treated as successful delivery.

## Session E2E Configuration

The existing Session E2E suites use the Docker hostname `iotdb` and fixed single-node Table settings. Run those suites inside the Docker Compose network or provide equivalent host resolution and service availability.

Session and subscription tests share [config_test.go](config_test.go). The shared environment variables support external deployments while keeping subscription tests opt-in.

## Subscription Coverage

`subscription_tree_test.go` covers:

- Topic management and subscription metadata
- Tree path and time filtering
- Snapshot completion with automatic commit
- Incremental checkpoint replay
- Pull/Push failure behavior
- Consumer groups and multi-Topic consumption
- `SeekToBeginning` and checkpoint reuse
- Stale commit context after Seek
- TsFile payload rejection during Pull

`subscription_table_test.go` covers:

- Tree/table Topic isolation
- Database, table, time, column, and TAG filters
- Snapshot completion with automatic and manual commit
- Column projection without time
- Column/TAG filter changes and Topic rebinding
- Checkpoint-tail replay
- Push callback failure/retry
- TsFile payload rejection during Push

Each test uses unique resource names and closes consumers before dropping topics and data. Subscription operations use bounded contexts, including independent cleanup contexts.

## Known Coverage Limits

The single-node Compose fixture does not exercise:

- DataNode or Broker restart
- Provider replacement
- Deliberately failed ACKs
- Heartbeat reconciliation after an unsubscribe timeout
- Multi-DataNode region movement

Those scenarios require multi-node or host-side fault orchestration.

## Docker Entry Point

The repository Makefile prepares an IoTDB distribution and starts `test/e2e/docker-compose.yml`:

```sh
make e2e_test
make e2e_test_clean
```

The client container runs the `test/e2e` package from the root module. `IOTDB_E2E_*` and `IOTDB_SUBSCRIPTION_E2E` are passed into the container when set. Unset values use the defaults described above.

See the [subscription client guide](../../subscription/README.md) for API contracts and connection defaults.
