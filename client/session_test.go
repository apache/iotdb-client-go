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

package client

import "testing"

func TestParseNodeURL(t *testing.T) {
	tests := []struct {
		name     string
		nodeURL  string
		wantHost string
		wantPort string
		wantErr  bool
	}{
		{"ipv4", "127.0.0.1:6667", "127.0.0.1", "6667", false},
		{"hostname", "localhost:6667", "localhost", "6667", false},
		{"ipv6 loopback", "[::1]:6667", "::1", "6667", false},
		{"ipv6 full", "[2001:db8::1]:6667", "2001:db8::1", "6667", false},
		// Bare (unbracketed) IPv6 with a port is ambiguous and rejected; the
		// bracketed [ipv6]:port form must be used (see apache/iotdb#18162).
		{"bare ipv6 rejected", "::1:6667", "", "", true},
		{"ipv6 missing port", "[::1]6667", "", "", true},
		{"ipv6 unbalanced bracket", "[::1:6667", "", "", true},
		{"no colon", "nocolon", "", "", true},
		{"empty port bracketed", "[::1]:", "", "", true},
		{"empty port", "host:", "", "", true},
		{"empty host", ":6667", "", "", true},
		{"empty", "", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ep, err := parseNodeURL(tt.nodeURL)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseNodeURL(%q) error = %v, wantErr %v", tt.nodeURL, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if ep.Host != tt.wantHost || ep.Port != tt.wantPort {
				t.Errorf("parseNodeURL(%q) = {host %q, port %q}, want {host %q, port %q}",
					tt.nodeURL, ep.Host, ep.Port, tt.wantHost, tt.wantPort)
			}
		})
	}
}
