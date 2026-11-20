#!/bin/bash
#
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
#
sed -i 's/dn_rpc_address=127.0.0.1/dn_rpc_address=0.0.0.0/g' /usr/local/iotdb/conf/iotdb-system.properties
sed -i 's/^subscription_enabled=.*/subscription_enabled=true/' /usr/local/iotdb/conf/iotdb-system.properties
sed -i 's/^chunk_timeseriesmeta_free_memory_proportion=.*/chunk_timeseriesmeta_free_memory_proportion=1:100:200:50:200:200:200:50:250/' /usr/local/iotdb/conf/iotdb-system.properties
nohup /usr/local/iotdb/sbin/start-confignode.sh > /dev/null 2>&1 &
sleep 5
ls /usr/local/iotdb/sbin
/usr/local/iotdb/sbin/start-datanode.sh
