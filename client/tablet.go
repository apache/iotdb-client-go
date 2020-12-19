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

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
)

type MeasurementSchema struct {
	Measurement string
	DataType    TSDataType
	Encoding    TSEncoding
	Compressor  TSCompressionType
	Properties  map[string]string
}

type Tablet struct {
	deviceId   string
	Schemas    []*MeasurementSchema
	timestamps []int64
	values     []interface{}
	RowSize    int
}

func (t *Tablet) SetTimestamp(timestamp int64, rowIndex int) {
	t.timestamps[rowIndex] = timestamp
}

func (t *Tablet) SetValueAt(value interface{}, columnIndex, rowIndex int) error {
	if value == nil {
		return errors.New("Illegal argument value can't be nil")
	}

	if columnIndex < 0 || columnIndex > len(t.Schemas) {
		return fmt.Errorf("Illegal argument columnIndex %d", columnIndex)
	}

	if rowIndex < 0 || rowIndex > int(t.RowSize) {
		return fmt.Errorf("Illegal argument rowIndex %d", rowIndex)
	}

	switch t.Schemas[columnIndex].DataType {
	case BOOLEAN:
		values := t.values[columnIndex].([]bool)
		switch value.(type) {
		case bool:
			values[rowIndex] = value.(bool)
		case *bool:
			values[rowIndex] = *value.(*bool)
		default:
			return fmt.Errorf("Illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case INT32:
		values := t.values[columnIndex].([]int32)
		switch value.(type) {
		case int32:
			values[rowIndex] = value.(int32)
		case *int32:
			values[rowIndex] = *value.(*int32)
		default:
			return fmt.Errorf("Illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case INT64:
		values := t.values[columnIndex].([]int64)
		switch value.(type) {
		case int64:
			values[rowIndex] = value.(int64)
		case *int64:
			values[rowIndex] = *value.(*int64)
		default:
			return fmt.Errorf("Illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case FLOAT:
		values := t.values[columnIndex].([]float32)
		switch value.(type) {
		case float32:
			values[rowIndex] = value.(float32)
		case *float32:
			values[rowIndex] = *value.(*float32)
		default:
			return fmt.Errorf("Illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	case DOUBLE:
		values := t.values[columnIndex].([]float64)
		switch value.(type) {
		case float64:
			values[rowIndex] = value.(float64)
		case *float64:
			values[rowIndex] = *value.(*float64)
		default:
			return fmt.Errorf("Illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	}
	return nil
}

func (t *Tablet) GetTimestampBytes() []byte {
	buff := &bytes.Buffer{}
	for _, v := range t.timestamps {
		binary.Write(buff, binary.BigEndian, v)
	}
	return buff.Bytes()
}

func (t *Tablet) GetMeasurements() []string {
	measurements := make([]string, len(t.Schemas))
	for i, s := range t.Schemas {
		measurements[i] = s.Measurement
	}
	return measurements
}

func (t *Tablet) getDataTypes() []int32 {
	types := make([]int32, len(t.Schemas))
	for i, s := range t.Schemas {
		types[i] = int32(s.DataType)
	}
	return types
}

func (t *Tablet) GetValuesBytes() ([]byte, error) {
	buff := &bytes.Buffer{}
	for i, schema := range t.Schemas {
		switch schema.DataType {
		case BOOLEAN:
			binary.Write(buff, binary.BigEndian, t.values[i].([]bool))
		case INT32:
			binary.Write(buff, binary.BigEndian, t.values[i].([]int32))
		case INT64:
			binary.Write(buff, binary.BigEndian, t.values[i].([]int64))
		case FLOAT:
			binary.Write(buff, binary.BigEndian, t.values[i].([]float32))
		case DOUBLE:
			binary.Write(buff, binary.BigEndian, t.values[i].([]float64))
		case TEXT:
			for _, s := range t.values[i].([]string) {
				binary.Write(buff, binary.BigEndian, int32(len(s)))
				binary.Write(buff, binary.BigEndian, []byte(s))
			}
		default:
			return nil, fmt.Errorf("Illegal datatype %v", schema.DataType)
		}
	}
	return buff.Bytes(), nil
}

func NewTablet(deviceId string, schemas []*MeasurementSchema, size int) (*Tablet, error) {
	tablet := &Tablet{
		deviceId: deviceId,
		Schemas:  schemas,
		RowSize:  size,
	}
	tablet.timestamps = make([]int64, size)
	tablet.values = make([]interface{}, len(schemas))
	for i, schema := range tablet.Schemas {
		switch schema.DataType {
		case BOOLEAN:
			tablet.values[i] = make([]bool, size)
		case INT32:
			tablet.values[i] = make([]int32, size)
		case INT64:
			tablet.values[i] = make([]int64, size)
		case FLOAT:
			tablet.values[i] = make([]float32, size)
		case DOUBLE:
			tablet.values[i] = make([]float64, size)
		case TEXT:
			tablet.values[i] = make([]string, size)
		default:
			return nil, fmt.Errorf("Illegal datatype %v", schema.DataType)
		}
	}
	return tablet, nil
}
