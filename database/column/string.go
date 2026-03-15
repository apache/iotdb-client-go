package column

import (
	"github.com/apache/iotdb-client-go/v2/client"
)

type String struct {
	name string
}

func (s *String) Name() string {
	return s.name
}
func (s *String) Type() Type {
	return "STRING"
}
func (s *String) Row(stat *client.SessionDataSet, ptr bool) any {
	getString, err := stat.GetString(s.name)
	if err != nil {
		if !ptr {
			return getString
		}
		return &getString
	}
	if !ptr {
		return getString
	}
	return &getString
}

var _ Interface = (*String)(nil)
