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

// Package configutil resolves shared connection and dialect settings.
package configutil

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/apache/iotdb-client-go/subscription"
)

// DefaultConnection returns independently owned connection settings.
func DefaultConnection() subscription.ConnectionConfig {
	return subscription.ConnectionConfig{
		NodeURLs: []string{"localhost:6667"},
		Username: "root",
		Password: "root",
		ConnectTimeout: 10 * time.Second,
		MaxFrameSize: 64 * 1024 * 1024,
	}
}

// NormalizeDialect resolves an empty dialect and rejects unsupported models.
func NormalizeDialect(dialect subscription.Dialect) (subscription.Dialect, error) {
	if dialect == "" {
		dialect = subscription.TreeDialect
	}
	if dialect != subscription.TreeDialect && dialect != subscription.TableDialect {
		return "", fmt.Errorf("subscription: unsupported dialect %q", dialect)
	}
	return dialect, nil
}

// NormalizeConnection validates settings and copies endpoints. An explicitly
// empty password is preserved rather than replaced with the default password.
func NormalizeConnection(config subscription.ConnectionConfig) (subscription.ConnectionConfig, error) {
	defaults := DefaultConnection()
	if len(config.NodeURLs) == 0 {
		config.NodeURLs = defaults.NodeURLs
	} else {
		config.NodeURLs = append([]string(nil), config.NodeURLs...)
	}
	for _, address := range config.NodeURLs {
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return subscription.ConnectionConfig{}, fmt.Errorf("subscription: invalid node address %q: expected host:port", address)
		}
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return subscription.ConnectionConfig{}, fmt.Errorf("subscription: invalid port in node address %q", address)
		}
	}
	if config.Username == "" {
		config.Username = defaults.Username
	}
	if config.Password != "" && config.EncryptedPassword != "" {
		return subscription.ConnectionConfig{}, fmt.Errorf("subscription: Password and EncryptedPassword are mutually exclusive")
	}
	if config.ConnectTimeout < 0 {
		return subscription.ConnectionConfig{}, fmt.Errorf("subscription: ConnectTimeout must not be negative")
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = defaults.ConnectTimeout
	}
	if config.MaxFrameSize < 0 {
		return subscription.ConnectionConfig{}, fmt.Errorf("subscription: MaxFrameSize must not be negative")
	}
	if config.MaxFrameSize == 0 {
		config.MaxFrameSize = defaults.MaxFrameSize
	}
	return config, nil
}
