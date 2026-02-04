package iotdb_go

import (
	"database/sql"
)

var conn *sql.DB

func init() {
	var err error
	conn, err = sql.Open("iotdb", "iotdb://192.168.2.100:6667?username=root&password=root")
	if err != nil {
		panic(err)
		return
	}
}
