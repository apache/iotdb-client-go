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

# Go Client Examples

These examples are packages in the main `github.com/apache/iotdb-client-go/v2` module. They require Go 1.25 or later and can be run directly from a repository checkout.

The main module contains the Session client, database/sql driver, and subscription packages. Existing imports, including `github.com/apache/iotdb-client-go/v2/client` and `github.com/apache/iotdb-client-go/v2/database`, are unchanged.

| Example | Command from this directory |
| --- | --- |
| [Tree Session](session_example.go) | `go run .` |
| [Table Session](table/table_session_example.go) | `go run ./table` |
| [Tree Session pool](session_pool/session_pool_example.go) | `go run ./session_pool` |
| [Table Session pool](session_pool/table/table_session_pool_example.go) | `go run ./session_pool/table` |
| [database/sql](database_sql/database_example.go) | `go run ./database_sql` |
| [Tree record subscriptions](subscription/tree/main.go) | `go run ./subscription/tree` |
| [Table record subscriptions](subscription/table/main.go) | `go run ./subscription/table` |
| [Tree incremental subscriptions](subscription/tree/incremental/main.go) | `go run ./subscription/tree/incremental` |
| [Table incremental subscriptions](subscription/table/incremental/main.go) | `go run ./subscription/table/incremental` |

These programs connect to IoTDB and create or modify sample data. Connection settings for the existing Session, pool, and database/sql examples are defined in their source files. The subscription programs accept connection flags and a password environment variable; their [instructions](subscription/README.md) describe server requirements, Pull/Push selection, ACK strategies, progress, and optional processors.

From the repository root, a subscription example can be selected as follows:

```sh
cd example
go run ./subscription/tree -host=127.0.0.1 -port=6667 -username=root
```

These programs are built by the root-module CI together with the client tests and [E2E tests](../test/e2e/README.md). CI does not execute the sample programs.
