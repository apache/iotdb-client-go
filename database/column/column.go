package column

import (
	"github.com/apache/iotdb-client-go/v2/client"
)

type Type string

type Interface interface {
	Name() string
	Type() Type
	Row(stat *client.SessionDataSet, ptr bool) any
}

func GenColumn(dataType string, name string) Interface {
	switch dataType {
	case "BOOLEAN":
		return &Bool{
			name: name,
		}
	case "INT32":
		return &Int32{
			name: name,
		}
	case "INT64":
		{
			return &Int64{
				name: name,
			}
		}
	case "FLOAT":
		return &Float{
			name: name,
		}
	case "DOUBLE":
		return &Double{
			name: name,
		}
	case "TEXT":
		{
			return &String{
				name: name,
			}
		}
	case "TIMESTAMP":
		break
	case "DATE":
		return &Date{
			name: name,
		}
	case "BLOB":
		return &Blob{
			name: name,
		}
	case "STRING":
		return &String{
			name: name,
		}
	}
	return nil
}
