package column

import "github.com/apache/iotdb-client-go/v2/client"

type Int32 struct {
	name string
}

func (i *Int32) Name() string {
	return i.name
}
func (i *Int32) Type() Type {
	return "INT32"
}
func (i *Int32) Row(stat *client.SessionDataSet, ptr bool) any {
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
