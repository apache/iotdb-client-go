package column

import "github.com/apache/iotdb-client-go/v2/client"

type Blob struct {
	name string
}

func (b *Blob) Name() string {
	return b.name
}
func (b *Blob) Type() Type {
	return "BLOB"
}

func (b *Blob) Row(stat *client.SessionDataSet, ptr bool) any {
	if stat == nil {
		if ptr {
			return nil
		}
		return 0
	}
	value, err := stat.GetBlob(b.name)
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
