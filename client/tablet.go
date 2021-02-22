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
	"sort"
)

type MeasurementSchema struct {
	Measurement string
	DataType    TSDataType
	Encoding    TSEncoding
	Compressor  TSCompressionType
	Properties  map[string]string
}

type Tablet struct {
	deviceId           string
	measurementSchemas []*MeasurementSchema
	timestamps         []int64
	values             []interface{}
	rowCount           int
}

type SortedSlices struct {
	dataType TSDataType
	length  int
	mainSlice interface{}
	otherSlice  []int64
}

type SortByOther SortedSlices

func (sbo SortByOther) Len() int {
	return sbo.length
}

func (sbo SortByOther) Swap(i, j int) {
	switch sbo.dataType {
	case BOOLEAN:
		sortedSlice := sbo.mainSlice.([]bool)
		sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
	case INT32:
		sortedSlice := sbo.mainSlice.([]int32)
		sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
	case INT64:
		sortedSlice := sbo.mainSlice.([]int64)
		sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
	case FLOAT:
		sortedSlice := sbo.mainSlice.([]float32)
		sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
	case DOUBLE:
		sortedSlice := sbo.mainSlice.([]float64)
		sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
	case TEXT:
		sortedSlice := sbo.mainSlice.([]string)
		sortedSlice[i], sortedSlice[j] = sortedSlice[j], sortedSlice[i]
	}
	sbo.otherSlice[i], sbo.otherSlice[j] = sbo.otherSlice[j], sbo.otherSlice[i]
}

func (sbo SortByOther) Less(i, j int) bool {
	return sbo.otherSlice[i] < sbo.otherSlice[j]
}

func (t *Tablet) SetTimestamp(timestamp int64, rowIndex int) {
	t.timestamps[rowIndex] = timestamp
}

func (t *Tablet) SetValueAt(value interface{}, columnIndex, rowIndex int) error {
	if value == nil {
		return errors.New("Illegal argument value can't be nil")
	}

	if columnIndex < 0 || columnIndex > len(t.measurementSchemas) {
		return fmt.Errorf("Illegal argument columnIndex %d", columnIndex)
	}

	if rowIndex < 0 || rowIndex > int(t.rowCount) {
		return fmt.Errorf("Illegal argument rowIndex %d", rowIndex)
	}

	switch t.measurementSchemas[columnIndex].DataType {
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
	case TEXT:
		values := t.values[columnIndex].([]string)
		switch value.(type) {
		case string:
			values[rowIndex] = value.(string)
		case []byte:
			values[rowIndex] = string(value.([]byte))
		default:
			return fmt.Errorf("Illegal argument value %v %v", value, reflect.TypeOf(value))
		}
	}
	return nil
}

func (t *Tablet) GetRowCount() int {
	return t.rowCount
}

func (t *Tablet) GetValueAt(columnIndex, rowIndex int) (interface{}, error) {
	if columnIndex < 0 || columnIndex > len(t.measurementSchemas) {
		return nil, fmt.Errorf("Illegal argument columnIndex %d", columnIndex)
	}

	if rowIndex < 0 || rowIndex > int(t.rowCount) {
		return nil, fmt.Errorf("Illegal argument rowIndex %d", rowIndex)
	}

	schema := t.measurementSchemas[columnIndex]
	switch schema.DataType {
	case BOOLEAN:
		return t.values[columnIndex].([]bool)[rowIndex], nil
	case INT32:
		return t.values[columnIndex].([]int32)[rowIndex], nil
	case INT64:
		return t.values[columnIndex].([]int64)[rowIndex], nil
	case FLOAT:
		return t.values[columnIndex].([]float32)[rowIndex], nil
	case DOUBLE:
		return t.values[columnIndex].([]float64)[rowIndex], nil
	case TEXT:
		return t.values[columnIndex].([]string)[rowIndex], nil
	default:
		return nil, fmt.Errorf("Illegal datatype %v", schema.DataType)
	}
}

func (t *Tablet) GetTimestampBytes() []byte {
	buff := &bytes.Buffer{}
	for _, v := range t.timestamps {
		binary.Write(buff, binary.BigEndian, v)
	}
	return buff.Bytes()
}

func (t *Tablet) GetMeasurements() []string {
	measurements := make([]string, len(t.measurementSchemas))
	for i, s := range t.measurementSchemas {
		measurements[i] = s.Measurement
	}
	return measurements
}

func (t *Tablet) getDataTypes() []int32 {
	types := make([]int32, len(t.measurementSchemas))
	for i, s := range t.measurementSchemas {
		types[i] = int32(s.DataType)
	}
	return types
}

func (t *Tablet) getValuesBytes() ([]byte, error) {
	buff := &bytes.Buffer{}
	for i, schema := range t.measurementSchemas {
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

func (t *Tablet) Sort() error {
	var otherSlice = make([]int64, len(t.timestamps))
	for i, schema := range t.measurementSchemas {
		copy(otherSlice, t.timestamps)
		sortedSlice := SortedSlices{
			dataType: schema.DataType,
			length: t.rowCount,
			mainSlice:  t.values[i],
			otherSlice: otherSlice,
		}
		sort.Sort(SortByOther(sortedSlice))
	}
	return nil
}

func NewTablet(deviceId string, measurementSchemas []*MeasurementSchema, rowCount int) (*Tablet, error) {
	tablet := &Tablet{
		deviceId:           deviceId,
		measurementSchemas: measurementSchemas,
		rowCount:           rowCount,
	}
	tablet.timestamps = make([]int64, rowCount)
	tablet.values = make([]interface{}, len(measurementSchemas))
	for i, schema := range tablet.measurementSchemas {
		switch schema.DataType {
		case BOOLEAN:
			tablet.values[i] = make([]bool, rowCount)
		case INT32:
			tablet.values[i] = make([]int32, rowCount)
		case INT64:
			tablet.values[i] = make([]int64, rowCount)
		case FLOAT:
			tablet.values[i] = make([]float32, rowCount)
		case DOUBLE:
			tablet.values[i] = make([]float64, rowCount)
		case TEXT:
			tablet.values[i] = make([]string, rowCount)
		default:
			return nil, fmt.Errorf("Illegal datatype %v", schema.DataType)
		}
	}
	return tablet, nil
}
