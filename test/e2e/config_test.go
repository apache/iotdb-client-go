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

package e2e

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/subscription"
	subsession "github.com/apache/iotdb-client-go/v2/subscription/session"
)

const (
	e2eDefaultNodeURLs  = "iotdb:6668,iotdb:6667,iotdb:6669"
	e2eDefaultTableHost = "iotdb"
	e2eDefaultTablePort = "6667"
	e2eDefaultUsername  = "root"
	e2eDefaultPassword  = "root"
)

var (
	e2eNodeURLs = e2eDefaultValue("IOTDB_E2E_NODE_URLS", "", e2eDefaultNodeURLs)
	e2eUsername = e2eDefaultValue("IOTDB_E2E_USERNAME", "IOTDB_USERNAME", e2eDefaultUsername)
	e2ePassword = e2eValueOrFallback("IOTDB_E2E_PASSWORD", "IOTDB_PASSWORD", e2eDefaultPassword)
)

// Subscription cases are discovered by the normal go test run but require an
// explicit environment opt-in. Endpoint and credential defaults come from the
// shared E2E configuration.
const (
	subscriptionE2EDefaultEnabled = false

	subscriptionE2ETestTimeout = 30 * time.Second
	subscriptionE2EOpenTimeout = 10 * time.Second
	// The server closes an inactive consumer after its 10s timeout plus up to
	// one 5s checker interval, and metadata propagation may add further delay.
	// Keep a full extra checker window for CI scheduling variance.
	subscriptionE2ECleanupTimeout  = 30 * time.Second
	subscriptionE2EWriterTimeoutMs = 10000

	subscriptionE2EHeartbeatInterval     = time.Second
	subscriptionE2EEndpointsSyncInterval = 5 * time.Second
	subscriptionE2EAutoCommitInterval    = 500 * time.Millisecond
	subscriptionE2EPollTimeout           = time.Second
	subscriptionE2EPushPollInterval      = 100 * time.Millisecond
	subscriptionE2EMaxPollRounds         = 40
	subscriptionE2EQuietPollRounds       = 3

	subscriptionTreeE2EProgressPollInterval  = 20 * time.Millisecond
	subscriptionTableE2EProgressPollInterval = 100 * time.Millisecond
)

// Subscription tests require IOTDB_SUBSCRIPTION_E2E=1 and a subscription-capable
// server. Incremental cases require WAL subscriptions with a compatible
// DataRegion consensus protocol; Ratis is not supported. Opting in makes
// unavailable server features fail rather than silently skip these cases.
// Java topic/path/time/snapshot ITs provide design references, but some are
// @Ignore in that checkout; their presence is not evidence these Go tests ran.
func subscriptionE2EConnection(t *testing.T) subscription.ConnectionConfig {
	t.Helper()
	enabled := subscriptionE2EDefaultEnabled
	if value, supplied := os.LookupEnv("IOTDB_SUBSCRIPTION_E2E"); supplied {
		enabled = value == "1"
	}
	if !enabled {
		t.Skip("set IOTDB_SUBSCRIPTION_E2E=1 to run subscription server tests")
	}
	shared := e2eClusterConfig()
	if len(shared.NodeUrls) == 0 {
		t.Fatal("subscription endpoint list is empty")
	}
	config := subsession.DefaultSubscriptionSessionConfig().Connection
	config.NodeURLs = shared.NodeUrls
	config.Username = shared.UserName
	config.Password = shared.Password
	return config
}

func e2eDefaultValue(primary, fallbackEnv, defaultValue string) string {
	value := e2eValueOrFallback(primary, fallbackEnv, defaultValue)
	if value == "" {
		return defaultValue
	}
	return value
}

func e2eValueOrFallback(primary, fallbackEnv, defaultValue string) string {
	if value, supplied := os.LookupEnv(primary); supplied {
		return value
	}
	if fallbackEnv != "" {
		if value, supplied := os.LookupEnv(fallbackEnv); supplied {
			return value
		}
	}
	return defaultValue
}

func e2eAddresses() []string {
	var addresses []string
	for _, address := range strings.Split(e2eNodeURLs, ",") {
		if address = strings.TrimSpace(address); address != "" {
			addresses = append(addresses, address)
		}
	}
	return addresses
}

func e2eClusterConfig() client.ClusterConfig {
	return client.ClusterConfig{
		NodeUrls: e2eAddresses(),
		UserName: e2eUsername,
		Password: e2ePassword,
	}
}

func e2eTableEndpoint() (host, port string) {
	return e2eDefaultTableHost, e2eDefaultTablePort
}

func e2eTableConfig(database string) *client.Config {
	host, port := e2eTableEndpoint()
	return &client.Config{
		Host:     host,
		Port:     port,
		UserName: e2eUsername,
		Password: e2ePassword,
		Database: database,
	}
}

func e2eTablePoolConfig(database string) *client.PoolConfig {
	host, port := e2eTableEndpoint()
	return &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: e2eUsername,
		Password: e2ePassword,
		Database: database,
	}
}
