# iotdb-client-go
Apache IoTDB Client for GoLang</br>

## Prerequisites

golang >= 1.13

## Get started

With go mod

```sh
export GO111MODULE=on
export GOPROXY=https://goproxy.io

mkdir session_example && cd session_example

curl -ss -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/session_example.go

go mod init session_example
go run session_example.go
```

Without go mod

```sh
# get thrift 0.13.0
go get github.com/apache/thrift
cd $GOPATH/src/github.com/apache/thrift
git checkout 0.13.0

mkdir -p $GOPATH/src/iotdb-client-go-example/session_example
cd $GOPATH/src/iotdb-client-go-example/session_example

curl -ss -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/session_example.go
go run session_example.go
```
