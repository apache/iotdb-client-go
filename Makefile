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
	@curl -o rpc.thrift https://raw.githubusercontent.com/apache/iotdb/master/thrift/src/main/thrift/rpc.thrift
	@thrift -out . -gen go rpc.thrift
	@rm -rf rpc/t_s_i_service-remote rpc.thrift

.PHONY: generate all test e2e_test e2e_test_clean

test:
	go test -v ./client/...

e2e_test:
	sh -c "cd /tmp/ && rm -rf iotdb && git clone https://github.com/apache/iotdb.git && cd iotdb && mvn -Dmaven.test.skip=true package -am -pl server"
	mkdir -p docker-context/iotdb
	unzip -o -q /tmp/iotdb/server/target/iotdb-server-*.zip -d docker-context/iotdb
	docker-compose -f test/e2e/docker-compose.yml up --build --abort-on-container-exit --remove-orphans

e2e_test_clean:
	rm -rf iotdb docker-context
	docker-compose -f test/e2e/docker-compose.yml down
