package client

import (
	"log"
	"testing"
)

func TestSessionDataSet_Next(t *testing.T) {
	session := NewSession(&Config{
		Host:     "127.0.0.1",
		Port:     "6667",
		UserName: "root",
		Password: "root",
	})
	err := session.Open(false, 40000)
	if err != nil {
		log.Fatalf("err is %v", err)
		return
	}
	var timeout int64 = 10000
	sessionDataSet, err := session.ExecuteQueryStatement("select * from root.test.d1", &timeout)
	if err != nil {
		log.Fatalf("err is %v", err)
		return
	}
	for {
		hasNext, err := sessionDataSet.Next()
		if err != nil {
			log.Fatalf("err is %v", err)
		}
		if !hasNext {
			break
		}
		t, _ := sessionDataSet.GetLong("Time")
		log.Printf("time is %v", t)
	}
	session.Close()
}
