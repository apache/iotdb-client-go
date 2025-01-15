package main

import (
	"flag"
	"fmt"
	"github.com/apache/iotdb-client-go/client"
	"log"
)

func main() {
	flag.StringVar(&host, "host", "127.0.0.1", "--host=192.168.1.100")
	flag.StringVar(&port, "port", "6667", "--port=6667")
	flag.StringVar(&user, "user", "root", "--user=root")
	flag.StringVar(&password, "password", "root", "--password=root")
	flag.Parse()
	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}
	session = client.NewSession(config)
	if err := session.Open(false, 0); err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	insertRecordsAnke()

	fmt.Println("Query data: ")
	executeQueryStatement("select * from root.db_go.** align by device")

	fmt.Println("\r\nDelete data: delete from root.db_go.d1.* where time = 1\r\n")
	session.ExecuteStatement("delete from root.db_go.d1.* where time = 1")

	fmt.Println("Query data after delete: ")
	executeQueryStatement("select * from root.db_go.** align by device")
}

func insertRecordsAnke() {
	var (
		deviceId     = []string{"root.db_go.d1", "root.db_go.d2"}
		measurements = [][]string{{"s1", "s2"}, {"s1", "s2"}}
		dataTypes    = [][]client.TSDataType{{client.INT32, client.INT32}, {client.INT32, client.INT32}}
		values       = [][]interface{}{{int32(1), int32(1)}, {int32(2), int32(2)}}
		timestamp    = []int64{1, 2}
	)
	checkError(session.InsertRecords(deviceId, measurements, dataTypes, values, timestamp))
}
