# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

all: generate

generate:
	@if ! command -v curl &> /dev/null; then \
		echo "curl could not be found, please install curl."; \
		exit 1; \
	fi

        @if [ -f "../../iotdb-protocol/thrift-commons/src/main/thrift/common.thrift" ]; then \
		thrift -out . -gen go ../../iotdb-protocol/thrift-commons/src/main/thrift/common.thrift; \
	else \
		curl -o common.thrift https://raw.githubusercontent.com/apache/iotdb/master/iotdb-protocol/thrift-commons/src/main/thrift/common.thrift; \
		thrift -out . -gen go common.thrift; \
		rm -f common.thrift; \
	fi

	@if [ -f "../../iotdb-protocol/thrift-datanode/src/main/thrift/client.thrift" ]; then \
		thrift -out . -gen go ../../iotdb-protocol/thrift-datanode/src/main/thrift/client.thrift; \
	else \
		curl -o client.thrift https://raw.githubusercontent.com/apache/iotdb/master/iotdb-protocol/thrift-datanode/src/main/thrift/client.thrift; \
		thrift -out . -gen go client.thrift; \
		rm -f client.thrift; \
	fi

.PHONY: generate all test e2e_test e2e_test_clean

test:
	go test -v ./client/...

e2e_test:
	sh -c "cd /tmp/ && rm -rf iotdb && git clone https://github.com/apache/iotdb.git && cd iotdb && mvn clean package -pl distribution -am -DskipTests"
	mkdir -p target/iotdb
	unzip -o -q /tmp/iotdb/distribution/target/apache-iotdb-*-all-bin.zip -d target
	mv target/*/* target/iotdb
	docker-compose -f test/e2e/docker-compose.yml up --build --abort-on-container-exit --remove-orphans

e2e_test_clean:
	rm -rf /tmp/iotdb target
	docker-compose -f test/e2e/docker-compose.yml down

#only used for project structure that the iotdb main project is in the parent folder of this project.
e2e_test_for_parent_git_repo:
	mkdir -p target/iotdb
	unzip -o -q ../../distribution/target/apache-iotdb-*-all-bin.zip -d target
	mv target/*/* target/iotdb
	docker-compose -f test/e2e/docker-compose.yml up --build --abort-on-container-exit --remove-orphans

e2e_test_clean_for_parent_git_repo:
	rm -rf target
	docker-compose -f test/e2e/docker-compose.yml down
