module github.com/apache/iotdb-client-go/test/e2e

go 1.25

require (
	github.com/apache/iotdb-client-go/subscription v0.0.0
	github.com/apache/iotdb-client-go/v2 v2.0.0-20260914074454-a25f68aa9681
	github.com/stretchr/testify v1.12.1
)

require (
	github.com/apache/thrift v0.24.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)

replace github.com/apache/iotdb-client-go/v2 => ../..

replace github.com/apache/iotdb-client-go/subscription => ../../subscription
