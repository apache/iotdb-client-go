package column

import (
	"github.com/apache/iotdb-client-go/v2/client"
)

type Bool struct {
	name string
}

func (b *Bool) Name() string {
	return b.name
}
func (b *Bool) Type() Type {
	return "BOOLEAN"
}
func (b *Bool) Row(stat *client.SessionDataSet, ptr bool) any {
	if stat == nil {
		if ptr {
			return nil
		}
		return 0
	}
	value, err := stat.GetBoolean(b.name)
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
