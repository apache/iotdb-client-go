package column

import "github.com/apache/iotdb-client-go/v2/client"

type Timestamp struct {
	name string
}

func (t *Timestamp) Name() string {
	return t.name
}
func (t *Timestamp) Type() Type {
	return "TIMESTAMP"
}

func (t *Timestamp) Row(stat *client.SessionDataSet, ptr bool) any {
	if stat == nil {
		if ptr {
			return nil
		}
		return 0
	}
	value, err := stat.GetDate(t.name)
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
