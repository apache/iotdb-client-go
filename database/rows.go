package iotdb_go

import (
	"errors"
	"github.com/apache/iotdb-client-go/v2/client"
	"github.com/apache/iotdb-client-go/v2/database/column"
)

type rows struct {
	set     *client.SessionDataSet
	columns []column.Interface
}

func (r *rows) Next() (bool, error) {
	if r.set == nil {
		return false, errors.New("rows is nil")
	}

	return r.set.Next()
}
