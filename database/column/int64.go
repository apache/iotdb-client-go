package column

import (
	"github.com/apache/iotdb-client-go/v2/client"
	"reflect"
)

type Int64 struct {
	name string
}

func (i *Int64) Name() string {
	return i.name
}
func (i *Int64) Type() Type {
	return "INT64"
}
func (i *Int64) Rows() int {
	return 0
}
func (i *Int64) Row(stat *client.SessionDataSet, ptr bool) any {
	if stat == nil {
		if ptr {
			return nil
		}
		return 0
	}
	value, err := stat.GetInt(i.name)
	if err != nil {
		if ptr {
			return nil
		}
		return 0
	}
	if ptr {
		return &value
	}
	return value
}
func (i *Int64) ScanRow(dest any, row int) error {
	return nil
}
func (i *Int64) Append(v any) (nulls []uint8, err error) {
	return nil, nil
}
func (i *Int64) AppendRow(v any) error {
	return nil
}
func (i *Int64) ScanType() reflect.Type {
	return reflect.TypeOf(&String{})
}
func (i *Int64) Reset() {
	return
}
