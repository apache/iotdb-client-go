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


UNAME_S := $(shell uname -s)
UNAME_P := $(shell uname -p)

ifeq ($(UNAME_S),Linux)
	ifeq ($(UNAME_P),x86_64)
		OS_CLASSIFIER := linux-x86_64
		THRIFT_EXEC := thrift/bin/thrift
	endif
	ifeq ($(UNAME_P),aarch64)
		OS_CLASSIFIER := linux-aarch64
		THRIFT_EXEC := thrift/bin/thrift
	endif
endif
ifeq ($(UNAME_S),Darwin)
	ifeq ($(UNAME_P),x86_64)
		OS_CLASSIFIER := mac-x86_64
		THRIFT_EXEC := thrift/bin/thrift
	endif
	ifeq ($(UNAME_P),arm)
		OS_CLASSIFIER := mac-aarch64
		THRIFT_EXEC := thrift/bin/thrift
	endif
endif
ifeq ($(UNAME_S),CYGWIN)
	OS_CLASSIFIER := windows-x86_64
	THRIFT_EXEC := thrift/bin/Release/thrift.exe
endif
ifeq ($(UNAME_S),MINGW64)
	OS_CLASSIFIER := windows-x86_64
	THRIFT_EXEC := thrift/bin/Release/thrift.exe
endif

all: generate

generate:
	@if ! command -v curl &> /dev/null; then \
	    echo "curl could not be found, please install curl."; \
	    exit 1; \
	fi


	@if [ -f "../../iotdb-protocol/thrift-commons/src/main/thrift/common.thrift" ]; then \
		cd ../..; \
		mvn clean package -pl iotdb-protocol/thrift-datanode -am; \
		cd iotdb-client/client-go; \
		cp -r ../../iotdb-protocol/thrift-commons/target/generated-sources-go/common common; \
		cp -r ../../iotdb-protocol/thrift-datanode/target/generated-sources-go/rpc rpc; \
	else \
		echo "Downloading and unpacking iotdb-tools-thrift-0.14.1.0-$(OS_CLASSIFIER).zip"; \
		rm -rf thrift; \
		mkdir -p thrift; \
		curl -L -o thrift/iotdb-tools-thrift.zip https://repo1.maven.org/maven2/org/apache/iotdb/tools/iotdb-tools-thrift/0.14.1.0/iotdb-tools-thrift-0.14.1.0-$(OS_CLASSIFIER).zip; \
		unzip -o thrift/iotdb-tools-thrift.zip -d thrift; \
		curl -o common.thrift https://raw.githubusercontent.com/apache/iotdb/master/iotdb-protocol/thrift-commons/src/main/thrift/common.thrift; \
		$(THRIFT_EXEC) -out . -gen go common.thrift; \
		curl -o client.thrift https://raw.githubusercontent.com/apache/iotdb/master/iotdb-protocol/thrift-datanode/src/main/thrift/client.thrift; \
		$(THRIFT_EXEC) -out . -gen go client.thrift; \
		rm -f common.thrift; \
		rm -f client.thrift; \
	fi
	@rm -rf rpc/i_client_r_p_c_service-remote

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
