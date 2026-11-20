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

// Package identifier validates SQL spelling and parses protocol identifiers.
package identifier

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	subscriptionIdentifierPattern = regexp.MustCompile(`^[a-zA-Z0-9_\x{2E80}-\x{9FFF}]+$`)
	subscriptionNumericIdentifierPattern = regexp.MustCompile(`^[0-9]+([eE][0-9]+)?[fFdD]?$|^[0-9]+[lL]$`)
)

// CheckSQL validates the same identifier forms as Java IdentifierUtils
// while retaining the original SQL spelling, including enclosing backticks.
func CheckSQL(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("subscription: empty identifier is not supported")
	}
	if len(name) >= 2 && strings.HasPrefix(name, "`") && strings.HasSuffix(name, "`") {
		return name, nil
	}
	if !subscriptionIdentifierPattern.MatchString(name) {
		return "", fmt.Errorf("subscription: invalid unquoted identifier %q", name)
	}

	// PathVisitor.isRealNumber removes leading zeroes before checking numeric
	// syntax, retaining one zero before an exponent and preserving all-zero input.
	firstNonzero := 0
	for firstNonzero < len(name) && name[firstNonzero] == '0' {
		firstNonzero++
	}
	numeric := name
	if firstNonzero > 0 && firstNonzero < len(name) {
		if name[firstNonzero] == 'e' || name[firstNonzero] == 'E' {
			numeric = name[firstNonzero-1:]
		} else {
			numeric = name[firstNonzero:]
		}
	}
	if subscriptionNumericIdentifierPattern.MatchString(numeric) {
		return "", fmt.Errorf("subscription: numeric identifier %q must be enclosed in backticks", name)
	}
	return name, nil
}

// Parse returns the unquoted identifier used in protocol
// payloads, while CheckSQL retains SQL spelling for management calls.
func Parse(value string) (string, error) {
	identifier, err := CheckSQL(value)
	if err != nil {
		return "", err
	}
	if len(identifier) >= 2 && strings.HasPrefix(identifier, "`") && strings.HasSuffix(identifier, "`") {
		return strings.ReplaceAll(identifier[1:len(identifier)-1], "``", "`"), nil
	}
	return identifier, nil
}
