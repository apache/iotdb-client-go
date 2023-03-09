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

Apache IoTDB（物联网数据库）是一个物联网原生数据库，在数据管理和分析方面表现良好，可部署在边缘设备和云上。
由于其轻量级架构、高性能和丰富的功能集，以及与Apache Hadoop、Spark和Flink的深度集成，
Apache IoTDB可以满足物联网工业领域的海量数据存储、高速数据摄取和复杂数据分析的要求。

# Apache IoTDB Go语言客户端

[![E2E Tests](https://github.com/apache/iotdb-client-go/actions/workflows/e2e.yml/badge.svg)](https://github.com/apache/iotdb-client-go/actions/workflows/e2e.yml)
[![GitHub release](https://img.shields.io/github/release/apache/iotdb-client-go.svg)](https://github.com/apache/iotdb-client-go/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
![](https://github-size-badge.herokuapp.com/apache/iotdb-client-go.svg)
![](https://img.shields.io/badge/platform-win10%20%7C%20macos%20%7C%20linux-yellow.svg)
[![IoTDB Website](https://img.shields.io/website-up-down-green-red/https/shields.io.svg?label=iotdb-website)](https://iotdb.apache.org/)

## 概览

本仓库是Apache IoTDB的Go语言客户端.

Apache IoTDB website: https://iotdb.apache.org
Apache IoTDB Github: https://github.com/apache/iotdb

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
# get thrift 0.15.0
go get github.com/apache/thrift
cd $GOPATH/src/github.com/apache/thrift
git checkout 0.15.0

mkdir -p $GOPATH/src/iotdb-client-go-example/session_example
cd $GOPATH/src/iotdb-client-go-example/session_example
curl -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/example/session_example.go
go run session_example.go
```

## SessionPool
通过SessionPool管理session，用户不需要考虑如何重用session，当到达pool的最大值时，获取session的请求会阻塞
注意：session使用完成后需要调用PutBack方法

### 创建sessionPool

单实例
```golang

config := &client.PoolConfig{
    Host:     host,
    Port:     port,
    UserName: user,
    Password: password,
}
sessionPool = client.NewSessionPool(config, 3, 60000, 60000, false)

```

分布式或双活

```golang

config := &client.PoolConfig{
		UserName: user,
		Password: password,
		NodeUrls: strings.Split("127.0.0.1:6667,127.0.0.1:6668", ","),
	}
sessionPool = client.NewSessionPool(config, 3, 60000, 60000, false)

```


### 使用sessionPool获取session，使用完手动调用PutBack

例1：设置存储组

```golang

session, err := sessionPool.GetSession()
defer sessionPool.PutBack(session)
if err == nil {
    session.SetStorageGroup(sg)
}

```

例2：查询

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


## iotdb-client-go的开发者环境要求

### 操作系统

* Linux、Macos或其他类unix系统
* Windows+bash(WSL、cygwin、Git Bash)

### 命令行工具

* golang >= 1.13
* make   >= 3.0
* curl   >= 7.1.1
* thrift 0.15.0

## 疑难解答

### thrift 版本兼容性问题

分支`rel/0.13`以及更早前的版本中，apache/thrift的版本为`v0.14.1`。
在更新的版本中，apache/thrift已经升级为`v0.15.0`。
这两个版本在一些接口上并不兼容，使用不对应的版本，会导致编译报错。

两个版本中有改动的接口如下：

1. `NewTSocketConf`。该接口在`v0.14.1`版本中返回2个值，在`v0.15.0`版本中返回1个值。
2. `NewTFramedTransport`已弃用，改用为`NewTFramedTransportConf`。

更多相关的内容可以参考这个PR：[update thrift to 0.15.0 to fit IoTDB 0.13.0](https://github.com/apache/iotdb-client-go/pull/41)

### Open函数参数名称与实际功能不匹配的问题

函数```client/session.go/Open()```有一个参数`connectionTimeoutInMs`，表示了以毫秒为单位的连接超时时间。
但旧版本中，该函数在实现时并未正确进行单位转换，而是将其看作了纳秒。现在该问题已修复。
当该参数为0时，表示不设置超时时间；当设置为正数时表示以毫秒为单位的超时时间。
