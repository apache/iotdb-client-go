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

package subscription

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

// DefaultSubscriptionSessionConfig returns tree-model settings for localhost:6667
// with root/root authentication and a 64 MiB frame limit. The Go session uses a
// ten-second connection and opening timeout rather than an unbounded open.
// Each call returns its own endpoint slice.
func DefaultSubscriptionSessionConfig() SubscriptionSessionConfig {
	return SubscriptionSessionConfig{
		Connection: ConnectionConfig{
			NodeURLs: []string{"localhost:6667"},
			Username: "root",
			Password: "root",
			ConnectTimeout: 10 * time.Second,
			MaxFrameSize: 64 * 1024 * 1024,
		},
		Dialect: TreeDialect,
	}
}

func normalizeSubscriptionSessionConfig(config SubscriptionSessionConfig) (SubscriptionSessionConfig, error) {
	defaults := DefaultSubscriptionSessionConfig()
	if config.Dialect == "" {
		config.Dialect = defaults.Dialect
	}
	if config.Dialect != TreeDialect && config.Dialect != TableDialect {
		return SubscriptionSessionConfig{}, fmt.Errorf("subscription: unsupported dialect %q", config.Dialect)
	}
	if len(config.Connection.NodeURLs) == 0 {
		config.Connection.NodeURLs = defaults.Connection.NodeURLs
	} else {
		config.Connection.NodeURLs = append([]string(nil), config.Connection.NodeURLs...)
	}
	for _, address := range config.Connection.NodeURLs {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return SubscriptionSessionConfig{}, fmt.Errorf("subscription: invalid node address %q: expected host:port", address)
		}
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return SubscriptionSessionConfig{}, fmt.Errorf("subscription: invalid port in node address %q", address)
		}
	}
	if config.Connection.Username == "" {
		config.Connection.Username = defaults.Connection.Username
	}
	if config.Connection.Password != "" && config.Connection.EncryptedPassword != "" {
		return SubscriptionSessionConfig{}, fmt.Errorf("subscription: Password and EncryptedPassword are mutually exclusive")
	}
	if config.Connection.ConnectTimeout < 0 {
		return SubscriptionSessionConfig{}, fmt.Errorf("subscription: ConnectTimeout must not be negative")
	}
	if config.Connection.ConnectTimeout == 0 {
		config.Connection.ConnectTimeout = defaults.Connection.ConnectTimeout
	}
	if config.Connection.MaxFrameSize < 0 {
		return SubscriptionSessionConfig{}, fmt.Errorf("subscription: MaxFrameSize must not be negative")
	}
	if config.Connection.MaxFrameSize == 0 {
		config.Connection.MaxFrameSize = defaults.Connection.MaxFrameSize
	}
	return config, nil
}
