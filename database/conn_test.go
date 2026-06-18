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

package iotdb_go

import (
	"context"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildPoolConfig_PreservesFields verifies that buildPoolConfig copies all
// parsed PoolConfig fields from Options and only overrides Host/Port, rather
// than rebuilding a fresh config that drops them.
func TestBuildPoolConfig_PreservesFields(t *testing.T) {
	opt := &Options{
		PoolConfig: client.PoolConfig{
			UserName:        "user",
			Password:        "pass",
			FetchSize:       1024,
			TimeZone:        "UTC",
			ConnectRetryMax: 5,
			Database:        "db0",
		},
	}

	cfg := buildPoolConfig("h", "6667", opt)

	assert.Equal(t, "h", cfg.Host)
	assert.Equal(t, "6667", cfg.Port)
	assert.Equal(t, "user", cfg.UserName)
	assert.Equal(t, "pass", cfg.Password)
	assert.Equal(t, int32(1024), cfg.FetchSize)
	assert.Equal(t, "UTC", cfg.TimeZone)
	assert.Equal(t, 5, cfg.ConnectRetryMax)
	assert.Equal(t, "db0", cfg.Database)
}

// TestDial_PopulatesTimeZone verifies dial resolves PoolConfig.TimeZone into
// connect.timeZone. dial is lazy (NewSessionPool does not connect), so no
// server is required.
func TestDial_PopulatesTimeZone(t *testing.T) {
	opt := &Options{
		PoolConfig: client.PoolConfig{TimeZone: "America/New_York"},
	}

	conn, err := dial(context.Background(), "127.0.0.1:6667", 0, opt)
	require.NoError(t, err)

	want, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	assert.Equal(t, want, conn.timeZone)
}

// TestDial_InvalidTimeZone verifies dial fails fast on an unparseable time zone.
func TestDial_InvalidTimeZone(t *testing.T) {
	opt := &Options{
		PoolConfig: client.PoolConfig{TimeZone: "Not/AZone"},
	}

	_, err := dial(context.Background(), "127.0.0.1:6667", 0, opt)
	assert.Error(t, err)
}
