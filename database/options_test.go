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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ==================== Basic DSN Parsing ====================

// TestParseDSN_Basic verifies minimal DSN with host and port.
func TestParseDSN_Basic(t *testing.T) {
	opt, err := ParseDSN("iotdb://root:root@127.0.0.1:6667")
	require.NoError(t, err)
	assert.Equal(t, []string{"127.0.0.1:6667"}, opt.Addr)
	assert.Equal(t, "root", opt.PoolConfig.UserName)
	assert.Equal(t, "root", opt.PoolConfig.Password)
}

// TestParseDSN_WithQueryParams verifies DSN parameters are parsed correctly.
func TestParseDSN_WithQueryParams(t *testing.T) {
	opt, err := ParseDSN("iotdb://user:pass@localhost:6667?fetch_size=1024&time_zone=UTC&connect_retry_max=5")
	require.NoError(t, err)
	assert.Equal(t, "user", opt.PoolConfig.UserName)
	assert.Equal(t, "pass", opt.PoolConfig.Password)
	assert.Equal(t, int32(1024), opt.PoolConfig.FetchSize)
	assert.Equal(t, "UTC", opt.PoolConfig.TimeZone)
	assert.Equal(t, 5, opt.PoolConfig.ConnectRetryMax)
}

// TestParseDSN_MultipleAddresses verifies comma-separated addresses are split correctly.
func TestParseDSN_MultipleAddresses(t *testing.T) {
	opt, err := ParseDSN("iotdb://root:root@host1:6667,host2:6667,host3:6667")
	require.NoError(t, err)
	assert.Equal(t, []string{"host1:6667", "host2:6667", "host3:6667"}, opt.Addr)
}

// TestParseDSN_UsernamePasswordOverride verifies query params override URL credentials.
func TestParseDSN_UsernamePasswordOverride(t *testing.T) {
	opt, err := ParseDSN("iotdb://old:old@localhost:6667?username=new_user&password=new_pass")
	require.NoError(t, err)
	assert.Equal(t, "new_user", opt.PoolConfig.UserName)
	assert.Equal(t, "new_pass", opt.PoolConfig.Password)
}

// TestParseDSN_NoCredentials verifies DSN without user info is valid.
func TestParseDSN_NoCredentials(t *testing.T) {
	opt, err := ParseDSN("iotdb://127.0.0.1:6667")
	require.NoError(t, err)
	assert.Equal(t, []string{"127.0.0.1:6667"}, opt.Addr)
	assert.Equal(t, "", opt.PoolConfig.UserName)
	assert.Equal(t, "", opt.PoolConfig.Password)
}

// ==================== DSN Error Cases ====================

// TestParseDSN_EmptyHost returns an error for DSN without host.
func TestParseDSN_EmptyHost(t *testing.T) {
	_, err := ParseDSN("iotdb://")
	assert.Error(t, err)
}

// TestParseDSN_InvalidFetchSize returns an error for non-integer fetch_size.
func TestParseDSN_InvalidFetchSize(t *testing.T) {
	_, err := ParseDSN("iotdb://root:root@localhost:6667?fetch_size=abc")
	assert.Error(t, err)
}

// TestParseDSN_InvalidConnectRetryMax returns an error for non-integer connect_retry_max.
func TestParseDSN_InvalidConnectRetryMax(t *testing.T) {
	_, err := ParseDSN("iotdb://root:root@localhost:6667?connect_retry_max=xyz")
	assert.Error(t, err)
}

// TestParseDSN_InvalidURL returns an error for malformed URLs.
func TestParseDSN_InvalidURL(t *testing.T) {
	_, err := ParseDSN("://not-a-url")
	assert.Error(t, err)
}

// ==================== Unknown params are silently ignored ====================

// TestParseDSN_UnknownParams verifies unknown query params don't cause errors.
func TestParseDSN_UnknownParams(t *testing.T) {
	opt, err := ParseDSN("iotdb://root:root@localhost:6667?unknown_param=value")
	require.NoError(t, err)
	assert.Equal(t, "root", opt.PoolConfig.UserName)
}
