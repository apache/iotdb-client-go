module github.com/apache/iotdb-client-go/example

go 1.25

require (
	github.com/apache/iotdb-client-go/subscription v0.0.0
	github.com/apache/iotdb-client-go/v2 v2.0.0-20260914074454-a25f68aa9681
)

require (
	github.com/apache/thrift v0.24.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
)

replace github.com/apache/iotdb-client-go/v2 => ..

replace github.com/apache/iotdb-client-go/subscription => ../subscription
