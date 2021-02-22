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

# Apache IoTDB Go语言客户端

## 概览

本仓库是Apache IoTDB的Go语言客户端.

Apache IoTDB website: https://iotdb.apache.org
Apache IoTDB Github: https://github.com/apache/iotdb

## 如何编译

## 环境准备

    golang >= 1.13

## 如何使用 (快速上手)

使用go mod

```sh
export GO111MODULE=on
export GOPROXY=https://goproxy.io

mkdir session_example && cd session_example

curl -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/example/session_example.go

go mod init session_example
go run session_example.go
```

不使用go mod，采用GOPATH

```sh
# get thrift 0.13.0
go get github.com/apache/thrift
cd $GOPATH/src/github.com/apache/thrift
git checkout 0.13.0

mkdir -p $GOPATH/src/iotdb-client-go-example/session_example
cd $GOPATH/src/iotdb-client-go-example/session_example
curl -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/example/session_example.go
go run session_example.go
```

## iotdb-client-go的开发者环境要求

### 操作系统

* Linux、Macos或其他类unix系统
* Windows+bash(WSL、cygwin、Git Bash)

### 命令行工具

* golang >= 1.13
* make   >= 3.0
* curl   >= 7.1.1
* thrift 0.13.x
