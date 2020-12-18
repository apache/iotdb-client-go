/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
)

type Field struct {
	DataType string
	boolV    bool
	intV     int32
	longV    int64
	floatV   float32
	doubleV  float64
	binaryV  []byte
}

func Copy(field Field) (Field, error) {
	outField := NewField(field.DataType)
	if outField.DataType != "" {
		dateType := outField.DataType
		switch dateType {
		case "BOOLEAN":
			outField.SetBoolV(field.GetBoolV())
			break
		case "INT32":
			outField.SetIntV(field.GetIntV())
			break
		case "INT64":
			outField.SetLongV(field.GetLongV())
			break
		case "FLOAT":
			outField.SetFloatV(field.GetFloatV())
			break
		case "DOUBLE":
			outField.SetDoubleV(field.GetDoubleV())
			break
		case "TEXT":
			outField.SetBinaryV(field.GetBinaryV())
			break
		default:
			return outField, errors.New("unsupported data type " + dateType)
		}
	}
	return outField, nil
}

func NewField(DataType string) Field {
	return Field{DataType: DataType}
}

func (f *Field) GetBoolV() bool {
	return f.boolV
}

func (f *Field) SetBoolV(boolVal bool) {
	f.boolV = boolVal
}

func (f *Field) GetBinaryV() []byte {
	return f.binaryV
}

func (f *Field) SetBinaryV(binaryVal []byte) {
	f.binaryV = binaryVal
}

func (f *Field) GetFloatV() float32 {
	return f.floatV
}

func (f *Field) SetFloatV(floatVal float32) {
	f.floatV = floatVal
}

func (f *Field) GetDoubleV() float64 {
	return f.doubleV
}

func (f *Field) SetDoubleV(doubleVal float64) {
	f.doubleV = doubleVal
}

func (f *Field) GetIntV() int32 {
	return f.intV
}

func (f *Field) SetIntV(intVal int32) {
	f.intV = intVal
}

func (f *Field) GetLongV() int64 {
	return f.longV
}

func (f *Field) SetLongV(longVal int64) {
	f.longV = longVal
}

func (f *Field) ToString() (string, error) {
	return f.GetStringValue()
}

func (f *Field) GetStringValue() (string, error) {
	if f.DataType == "" {
		return "", nil
	}
	dateType := f.DataType
	buf := bytes.NewBuffer([]byte{})
	switch dateType {
	case "BOOLEAN":
		binary.Write(buf, binary.BigEndian, &f.boolV)
		break
	case "INT32":
		binary.Write(buf, binary.BigEndian, &f.intV)
		break
	case "INT64":
		binary.Write(buf, binary.BigEndian, &f.longV)
		break
	case "FLOAT":
		binary.Write(buf, binary.BigEndian, &f.floatV)
		break
	case "DOUBLE":
		binary.Write(buf, binary.BigEndian, &f.doubleV)
		break
	case "TEXT":
		buf.Write(f.binaryV)
		break
	default:
		return "", errors.New("unsupported data type " + dateType)
	}
	return buf.String(), nil
}

func (f *Field) GetField(value interface{}, dateType string) (*Field, error) {
	if value == nil {
		return nil, nil
	}
	field := NewField(dateType)
	switch dateType {
	case "BOOLEAN":
		field.SetBoolV(value.(bool))
		break
	case "INT32":
		field.SetIntV(int32(value.(int)))
		break
	case "INT64":
		field.SetLongV(int64(value.(int)))
		break
	case "FLOAT":
		field.SetFloatV(float32(value.(float64)))
		break
	case "DOUBLE":
		field.SetDoubleV(value.(float64))
		break
	case "TEXT":
		field.SetBinaryV([]byte(value.(string)))
		break
	default:
		return &field, errors.New("unsupported data type " + dateType)
	}
	return &field, nil
}

func (f *Field) GetObjectValue(dateType string) (interface{}, error) {
	if f.DataType == "" {
		return nil, nil
	}
	switch dateType {
	case "BOOLEAN":
		return f.GetBoolV(), nil
		break
	case "INT32":
		return f.GetIntV(), nil
		break
	case "INT64":
		return f.GetLongV(), nil
		break
	case "FLOAT":
		return f.GetFloatV(), nil
		break
	case "DOUBLE":
		return f.GetDoubleV(), nil
		break
	case "TEXT":
		return f.GetBinaryV(), nil
		break
	default:
		return nil, errors.New("unsupported data type " + dateType)
	}
	return nil, nil
}

func (f *Field) IsNull() bool {
	return f.DataType == ""
}
