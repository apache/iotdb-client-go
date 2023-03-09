<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
[English](./README.md) | [中文](./README_ZH.md)

# Apache IoTDB

Apache IoTDB (Database for Internet of Things) is an IoT native database with high performance for 
data management and analysis, deployable on the edge and the cloud. Due to its light-weight 
architecture, high performance and rich feature set together with its deep integration with 
Apache Hadoop, Spark and Flink, Apache IoTDB can meet the requirements of massive data storage, 
high-speed data ingestion and complex data analysis in the IoT industrial fields.

# Apache IoTDB Client for Go

[![E2E Tests](https://github.com/apache/iotdb-client-go/actions/workflows/e2e.yml/badge.svg)](https://github.com/apache/iotdb-client-go/actions/workflows/e2e.yml)
[![GitHub release](https://img.shields.io/github/release/apache/iotdb-client-go.svg)](https://github.com/apache/iotdb-client-go/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb-client-go.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macos%20%7C%20linux-yellow.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)

## Overview

This is the GoLang client of Apache IoTDB.

Apache IoTDB website: https://iotdb.apache.org
Apache IoTDB Github: https://github.com/apache/iotdb

## Prerequisites

    golang >= 1.13

## How to Use the Client (Quick Start)

With go mod

```sh
export GO111MODULE=on
export GOPROXY=https://goproxy.io

mkdir session_example && cd session_example

curl -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/example/session_example.go

go mod init session_example
go run session_example.go
```

Without go mod

```sh
# get thrift 0.15.0
go get github.com/apache/thrift
cd $GOPATH/src/github.com/apache/thrift
git checkout 0.15.0

mkdir -p $GOPATH/src/iotdb-client-go-example/session_example
cd $GOPATH/src/iotdb-client-go-example/session_example

curl -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/example/session_example.go
go run session_example.go
```

## How to Use the SessionPool
SessionPool is a wrapper of a Session Set. Using SessionPool, the user do not need to consider how to reuse a session connection.
If there is no available connections and the pool reaches its max size, the all methods will hang until there is a available connection.
The PutBack method must be called after use

### New sessionPool
standalone

```golang

config := &client.PoolConfig{
    Host:     host,
    Port:     port,
    UserName: user,
    Password: password,
}
sessionPool = client.NewSessionPool(config, 3, 60000, 60000, false)

```
cluster or doubleLive

```golang

config := &client.PoolConfig{
		UserName: user,
		Password: password,
		NodeUrls: strings.Split("127.0.0.1:6667,127.0.0.1:6668", ","),
	}
sessionPool = client.NewSessionPool(config, 3, 60000, 60000, false)

```

### Get session through sessionPool, putback after use

set storage group

```golang

session, err := sessionPool.GetSession()
defer sessionPool.PutBack(session)
if err == nil {
    session.SetStorageGroup(sg)
}

```

query statement

```golang

var timeout int64 = 1000
session, err := sessionPool.GetSession()
defer sessionPool.PutBack(session)
if err != nil {
    log.Print(err)
    return
}
sessionDataSet, err := session.ExecuteQueryStatement(sql, &timeout)
if err == nil {
    defer sessionDataSet.Close()
    printDataSet1(sessionDataSet)
} else {
    log.Println(err)
}

```


## Developer environment requirements for iotdb-client-go

### OS

* Linux, Macos or other unix-like OS
* Windows+bash(WSL, cygwin, Git Bash)

### Command Line Tools

* golang >= 1.13
* make >= 3.0
* curl >= 7.1.1
* thrift 0.15.0

## Troubleshooting

### Thrift version compatibility issues

In the branch `rel/0.13` and earlier versions, the version of apache/thrift is `v0.14.1`.
In the latest version, apache/thrift has been upgraded to `v0.15.0`.

The two versions are not compatible on some interfaces. Using mismatched version will cause compilation errors.

The interfaces changed in the two versions are as follows:

1. `NewTSocketConf`. This function returns two values in the version `v0.14.1` and only one value in the version `v0.15.0`.
2. `NewTFramedTransport` has been deprecated, use `NewTFramedTransportConf` instead.

For more details, please take a look at this PR: [update thrift to 0.15.0 to fit IoTDB 0.13.0](https://github.com/apache/iotdb-client-go/pull/41)

### Parameter name mismatch with actual usage in function 'Open'

The implementation of the function ```client/session.go/Open()``` is mismatched with the description.
The parameter `connectionTimeoutInMs` represents connection timeout in milliseconds.
However, in the older version, this function did not implement correctly, regarding it as nanosecond instead.
The bug is now fixed.
Positive value of this parameter means connection timeout in milliseconds.
Set 0 for no timeout.
