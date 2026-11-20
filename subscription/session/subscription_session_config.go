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

package session

import (
	"github.com/apache/iotdb-client-go/v2/subscription"
	"github.com/apache/iotdb-client-go/v2/subscription/internal/configutil"
)

// SubscriptionSessionConfig configures a topic and subscription management session.
type SubscriptionSessionConfig struct {
	// Connection supplies the endpoint, authentication, and transport settings.
	Connection subscription.ConnectionConfig
	// Dialect selects the model used by the management session's connection and SQL.
	Dialect subscription.Dialect
}

// DefaultSubscriptionSessionConfig returns tree-model settings for localhost:6667
// with root/root authentication and a 64 MiB frame limit. The Go session uses a
// ten-second connection and opening timeout rather than an unbounded open.
// Each call owns its endpoint slice and does not establish a connection.
func DefaultSubscriptionSessionConfig() SubscriptionSessionConfig {
	return SubscriptionSessionConfig{
		Connection: configutil.DefaultConnection(),
		Dialect: subscription.TreeDialect,
	}
}

func normalizeSubscriptionSessionConfig(config SubscriptionSessionConfig) (SubscriptionSessionConfig, error) {
	var err error
	config.Dialect, err = configutil.NormalizeDialect(config.Dialect)
	if err != nil {
		return SubscriptionSessionConfig{}, err
	}
	config.Connection, err = configutil.NormalizeConnection(config.Connection)
	if err != nil {
		return SubscriptionSessionConfig{}, err
	}
	return config, nil
}
