package column

import "github.com/apache/iotdb-client-go/v2/client"

type Date struct {
	name string
}

func (b *Date) Name() string {
	return b.name
}
func (b *Date) Type() Type {
	return "DATE"
}

func (b *Date) Row(stat *client.SessionDataSet, ptr bool) any {
	if stat == nil {
		if ptr {
			return nil
		}
		return 0
	}
	value, err := stat.GetDate(b.name)
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
