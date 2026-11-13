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
	"fmt"
	"sort"
	"strings"
)

// buildTopicAttributesClause quotes attribute names and values for a WITH clause.
// Sorting makes the SQL stable without changing the attributes' spelling.
func buildTopicAttributesClause(attributes map[string]string) string {
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var clause strings.Builder
	clause.WriteByte('(')
	for index, key := range keys {
		if index > 0 {
			clause.WriteByte(',')
		}
		clause.WriteByte('\'')
		clause.WriteString(strings.ReplaceAll(key, "'", "''"))
		clause.WriteString("'='")
		clause.WriteString(strings.ReplaceAll(attributes[key], "'", "''"))
		clause.WriteByte('\'')
	}
	clause.WriteByte(')')
	return clause.String()
}

func validateAlterTopicAttributes(attributes map[string]string, allowOwner bool) error {
	if len(attributes) == 0 {
		return fmt.Errorf("subscription: ALTER TOPIC attributes must not be empty")
	}
	if allowOwner {
		return nil
	}
	// Java's management client checks these exact keys, without trimming or
	// folding case. Preserve that boundary rather than canonicalizing user input.
	for _, key := range []string{"owner-id", "owner-epoch", "max-owner-epoch", "owner-lease-duration-ms"} {
		if _, exists := attributes[key]; exists {
			return fmt.Errorf("subscription: topic owner attributes must be changed with AlterTopicOwner")
		}
	}
	return nil
}
