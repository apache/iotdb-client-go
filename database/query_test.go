package iotdb_go

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConnectionPool_QueryContext(t *testing.T) {

	start, err := time.Parse(time.DateTime, "2026-01-01 00:00:00")
	if err != nil {
		t.Fatalf("parse start time error: %v", err)
		return
	}

	end, err := time.Parse(time.DateTime, "2026-01-01 00:00:00")
	if err != nil {
		t.Fatalf("parse end time error: %v", err)
		return
	}
	row, err := conn.QueryContext(context.Background(), "select count(value) from root.device.** where time > ? and time < ?  order by count(value) desc limit 10 ALIGN BY DEVICE;",
		start, end)
	if err != nil {
		t.Fatalf("query error: %v", err)
		return
	}
	type Device struct {
		Device string
		Count  int64
	}
	for {
		var de Device
		next := row.Next()
		if !next {
			break
		}
		err := row.Scan(&de.Device, &de.Count)
		if err != nil {
			t.Fatalf("scan device error: %v", err)
			return
		}
		assert.NotEqual(t, "", de.Device)
	}

}

func TestConnectionPool_QueryRowContext(t *testing.T) {
	start, err := time.Parse(time.DateTime, "2026-01-01 00:00:00")
	if err != nil {
		t.Fatalf("parse start time error: %v", err)
		return
	}

	end, err := time.Parse(time.DateTime, "2026-01-01 00:00:00")
	if err != nil {
		t.Fatalf("parse end time error: %v", err)
		return
	}

	row := conn.QueryRowContext(context.Background(), "select sum(value) from root.device.dev10033642143439443283;",
		start, end)
	var count float64
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("scan device error: %v", err)
		return
	}
	return
}
