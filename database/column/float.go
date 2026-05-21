package column

import (
	"github.com/apache/iotdb-client-go/v2/client"
)

type Float struct {
	name string
}

func (f *Float) Name() string {
	return f.name
}
func (f *Float) Type() Type {
	return "FLOAT"
}

func (f *Float) Row(stat *client.SessionDataSet, ptr bool) any {
	if stat == nil {
		if ptr {
			return nil
		}
		return 0
	}
	value, err := stat.GetFloat(f.name)
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
