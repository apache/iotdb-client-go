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
	std_driver "database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/iotdb-client-go/v2/database/driver"
	"github.com/pkg/errors"
)

var (
	ErrInvalidTimezone = errors.New("invalid timezone value")
)

func Named(name string, value any) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

type TimeUnit uint8

const (
	Seconds TimeUnit = iota
	MilliSeconds
	MicroSeconds
	NanoSeconds
)

type GroupSet struct {
	Value []any
}

type ArraySet []any

func DateNamed(name string, value time.Time, scale TimeUnit) driver.NamedDateValue {
	return driver.NamedDateValue{
		Name:  name,
		Value: value,
		Scale: uint8(scale),
	}
}

type sqlLexState byte

const (
	stNormal sqlLexState = iota
	stSingleQuote
	stLineComment
	stBlockComment
)

func sqlScanStep(state sqlLexState, query string, i int) (sqlLexState, int) {
	switch state {
	case stNormal:
		switch query[i] {
		case '\'':
			return stSingleQuote, 0
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				return stLineComment, 1
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				return stBlockComment, 1
			}
		}
		return stNormal, 0
	case stSingleQuote:
		if query[i] == '\\' {
			if i+1 < len(query) && (query[i+1] == '\\' || query[i+1] == '\'') {
				return stSingleQuote, 1
			}
			return stSingleQuote, 0
		}
		if query[i] == '\'' {
			if i+1 < len(query) && query[i+1] == '\'' {
				return stSingleQuote, 1
			}
			return stNormal, 0
		}
		return stSingleQuote, 0
	case stLineComment:
		if query[i] == '\n' || query[i] == '\r' {
			return stNormal, 0
		}
		return stLineComment, 0
	case stBlockComment:
		if query[i] == '*' && i+1 < len(query) && query[i+1] == '/' {
			return stNormal, 1
		}
		return stBlockComment, 0
	default:
		return stNormal, 0
	}
}

func detectPlaceholderKinds(query string) (bool, bool) {
	state := stNormal
	havePositional := false
	haveNumeric := false

	for i := 0; i < len(query); i++ {
		current := state
		var skip int
		state, skip = sqlScanStep(state, query, i)

		if current == stNormal {
			switch query[i] {
			case '?':
				if i == 0 || query[i-1] != '\\' {
					havePositional = true
				}
			case '$':
				j := i + 1
				for j < len(query) && isDigit(query[j]) {
					j++
				}
				if j > i+1 {
					haveNumeric = true
					i = j - 1
				}
			}
		}

		if havePositional && haveNumeric {
			return true, true
		}

		if skip > 0 {
			i += skip
		}
	}

	return havePositional, haveNumeric
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

func isIdentChar(b byte) bool {
	if b >= 'a' && b <= 'z' {
		return true
	}
	if b >= 'A' && b <= 'Z' {
		return true
	}
	if b >= '0' && b <= '9' {
		return true
	}
	return b == '_'
}

func bind(tz *time.Location, query string, args ...any) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	var (
		haveNumeric    bool
		havePositional bool
	)

	allArgumentsNamed, err := checkAllNamedArguments(args...)
	if err != nil {
		return "", err
	}

	if allArgumentsNamed {
		return bindNamed(tz, query, args...)
	}

	havePositional, haveNumeric = detectPlaceholderKinds(query)
	if haveNumeric && havePositional {
		return "", ErrBindMixedParamsFormats
	}
	if haveNumeric {
		return bindNumeric(tz, query, args...)
	}
	return bindPositional(tz, query, args...)
}

func checkAllNamedArguments(args ...any) (bool, error) {
	var (
		haveNamed     bool
		haveAnonymous bool
	)
	for _, v := range args {
		switch v.(type) {
		case driver.NamedValue, driver.NamedDateValue:
			haveNamed = true
		default:
			haveAnonymous = true
		}
		if haveNamed && haveAnonymous {
			return haveNamed, ErrBindMixedParamsFormats
		}
	}
	return haveNamed, nil
}

func bindPositional(tz *time.Location, query string, args ...any) (_ string, err error) {
	state := stNormal
	lastCopy := 0
	argIndex := 0
	missing := 0
	changed := false

	var buf strings.Builder
	buf.Grow(len(query) + len(args)*8)

	for i := 0; i < len(query); i++ {
		current := state
		var skip int
		state, skip = sqlScanStep(state, query, i)

		if current == stNormal && query[i] == '?' {
			if i > 0 && query[i-1] == '\\' {
				buf.WriteString(query[lastCopy : i-1])
				buf.WriteByte('?')
				lastCopy = i + 1
				changed = true
			} else {
				buf.WriteString(query[lastCopy:i])
				if argIndex < len(args) {
					v := args[argIndex]
					if fn, ok := v.(std_driver.Valuer); ok {
						if v, err = fn.Value(); err != nil {
							return "", err
						}
					}

					value, err := format(tz, Seconds, v)
					if err != nil {
						return "", err
					}

					buf.WriteString(value)
					argIndex++
					changed = true
				} else {
					missing++
				}
				lastCopy = i + 1
			}

			if skip > 0 {
				i += skip
			}
			continue
		}

		if skip > 0 {
			i += skip
		}
	}

	buf.WriteString(query[lastCopy:])

	if missing > 0 {
		return "", fmt.Errorf("have no arg for param ? at last %d positions", missing)
	}

	if !changed {
		return query, nil
	}

	return buf.String(), nil
}

func bindNumeric(tz *time.Location, query string, args ...any) (_ string, err error) {
	params := make(map[string]string, len(args))
	for i, v := range args {
		if fn, ok := v.(std_driver.Valuer); ok {
			if v, err = fn.Value(); err != nil {
				return "", err
			}
		}
		val, err := format(tz, Seconds, v)
		if err != nil {
			return "", err
		}
		params[fmt.Sprintf("$%d", i+1)] = val
	}

	state := stNormal
	lastCopy := 0
	changed := false

	var buf strings.Builder
	buf.Grow(len(query) + len(args)*8)

	for i := 0; i < len(query); i++ {
		current := state
		var skip int
		state, skip = sqlScanStep(state, query, i)

		if current == stNormal && query[i] == '$' {
			j := i + 1
			for j < len(query) && isDigit(query[j]) {
				j++
			}
			if j > i+1 {
				key := query[i:j]
				val, ok := params[key]
				if !ok {
					return "", fmt.Errorf("have no arg for %s param", key)
				}
				buf.WriteString(query[lastCopy:i])
				buf.WriteString(val)
				lastCopy = j
				changed = true
				i = j - 1
				continue
			}
		}

		if skip > 0 {
			i += skip
		}
	}

	buf.WriteString(query[lastCopy:])

	if !changed {
		return query, nil
	}

	return buf.String(), nil
}

func bindNamed(tz *time.Location, query string, args ...any) (_ string, err error) {
	params := make(map[string]string, len(args))
	for _, v := range args {
		switch v := v.(type) {
		case driver.NamedValue:
			value := v.Value
			if fn, ok := v.Value.(std_driver.Valuer); ok {
				if value, err = fn.Value(); err != nil {
					return "", err
				}
			}
			val, err := format(tz, Seconds, value)
			if err != nil {
				return "", err
			}
			params["@"+v.Name] = val
		case driver.NamedDateValue:
			val, err := format(tz, TimeUnit(v.Scale), v.Value)
			if err != nil {
				return "", err
			}
			params["@"+v.Name] = val
		}
	}

	state := stNormal
	lastCopy := 0
	changed := false

	var buf strings.Builder
	buf.Grow(len(query) + len(params)*8)

	for i := 0; i < len(query); i++ {
		current := state
		var skip int
		state, skip = sqlScanStep(state, query, i)

		if current == stNormal && query[i] == '@' {
			j := i + 1
			for j < len(query) && isIdentChar(query[j]) {
				j++
			}
			if j > i+1 {
				key := query[i:j]
				val, ok := params[key]
				if !ok {
					return "", fmt.Errorf("have no arg for %q param", key)
				}
				buf.WriteString(query[lastCopy:i])
				buf.WriteString(val)
				lastCopy = j
				changed = true
				i = j - 1
				continue
			}
		}

		if skip > 0 {
			i += skip
		}
	}

	buf.WriteString(query[lastCopy:])

	if !changed {
		return query, nil
	}

	return buf.String(), nil
}

func formatTime(value time.Time) (string, error) {
	str := value.Format(time.DateTime)
	return str, nil
}

var stringQuoteReplacer = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

func format(tz *time.Location, scale TimeUnit, v any) (string, error) {
	quote := func(v string) string {
		return "'" + stringQuoteReplacer.Replace(v) + "'"
	}
	switch v := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return quote(v), nil
	case time.Time:
		return formatTime(v)
	case *time.Time:
		if v == nil {
			return "NULL", nil
		}
		return formatTime(*v)
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case GroupSet:
		val, err := join(tz, scale, v.Value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s)", val), nil
	case []GroupSet:
		val, err := join(tz, scale, v)
		if err != nil {
			return "", err
		}
		return val, err
	case ArraySet:
		val, err := join(tz, scale, v)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("[%s]", val), nil
	case fmt.Stringer:
		if v := reflect.ValueOf(v); v.Kind() == reflect.Pointer && v.IsNil() && v.Type().Elem().Implements(reflect.TypeOf((*fmt.Stringer)(nil)).Elem()) {
			return "NULL", nil
		}
		return quote(v.String()), nil
	}
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.String:
		return quote(v.String()), nil
	case reflect.Slice, reflect.Array:
		values := make([]string, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			val, err := format(tz, scale, v.Index(i).Interface())
			if err != nil {
				return "", err
			}
			values = append(values, val)
		}
		return fmt.Sprintf("[%s]", strings.Join(values, ", ")), nil
	case reflect.Map: // map
		values := make([]string, 0, len(v.MapKeys()))
		for _, key := range v.MapKeys() {
			name := fmt.Sprint(key.Interface())
			if key.Kind() == reflect.String {
				name = fmt.Sprintf("'%s'", name)
			}
			val, err := format(tz, scale, v.MapIndex(key).Interface())
			if err != nil {
				return "", err
			}
			values = append(values, fmt.Sprintf("%s, %s", name, val))
		}
		return "map(" + strings.Join(values, ", ") + ")", nil
	case reflect.Ptr:
		if v.IsNil() {
			return "NULL", nil
		}
		return format(tz, scale, v.Elem().Interface())
	}
	return fmt.Sprint(v), nil
}

func join[E any](tz *time.Location, scale TimeUnit, values []E) (string, error) {
	items := make([]string, len(values), len(values))
	for i := range values {
		val, err := format(tz, scale, values[i])
		if err != nil {
			return "", err
		}
		items[i] = val
	}
	return strings.Join(items, ", "), nil
}

func rebind(in []std_driver.NamedValue) []any {
	args := make([]any, 0, len(in))
	for _, v := range in {
		switch {
		case len(v.Name) != 0:
			args = append(args, driver.NamedValue{
				Name:  v.Name,
				Value: v.Value,
			})

		default:
			args = append(args, v.Value)
		}
	}
	return args
}
