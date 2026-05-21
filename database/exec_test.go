package iotdb_go

import (
	"context"
	"fmt"
	"testing"
)

func TestConnectionPool_ExecContext(t *testing.T) {
	execContext, err := conn.ExecContext(context.Background(), "insert into root.ln.wf02.wt02(timestamp,status) values(?,?)", 2, true)
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
		return
	}
	id, err := execContext.LastInsertId()
	if err != nil {
		return
	}
	fmt.Println(id)
	affected, err := execContext.RowsAffected()
	if err != nil {
		return
	}
	fmt.Println(affected)
}
