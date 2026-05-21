package column

import (
	"github.com/apache/iotdb-client-go/v2/client"
)

type Double struct {
	name string
}

func (d *Double) Name() string {
	return d.name
}
func (d *Double) Type() Type {
	return "DOUBLE"
}
func (d *Double) Row(stat *client.SessionDataSet, ptr bool) any {
	if stat == nil {
		if ptr {
			return nil
		}
		return 0
	}
	value, err := stat.GetDouble(d.name)
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
