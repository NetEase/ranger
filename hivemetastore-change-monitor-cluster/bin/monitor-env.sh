#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BASE_PATH=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)
echo "parameter: $1"

export metastore_namespaces=hive-cluster3,hive-cluster4
export keytab="$BASE_PATH"/../conf/hive.keytab
export principal=hive/app-20.photo.163.org@HADOOP.HZ.NETEASE.COM
export hiveserver2="hzabj-backend-hiveserver0.server.163.org:10000/default"
export hiveserver2_principal=hive/app-20.photo.163.org@HADOOP.HZ.NETEASE.COM
export HIVE_SERVER2_THRIFT_BIND_HOST=hadoop707.lt.163.org

#for python script
export hiveBin="/home/hadoop/hive-current/bin"


export ZOOKEEPER_CLI_PATH="/home/hadoop/zookeeper-current/bin/zkCli.sh"
export ZOOKEEPER_SERVER="hadoop943.hz.163.org,hadoop944.hz.163.org,hadoop945.hz.163.org"
export METASTORE_CHANGE_PATH="/hive-metastore-changelog/hive-cluster3/maxid,/hive-metastore-changelog/hive-cluster6/maxid"
export MAX_ID_FILES="hive-cluster3-maxid.txt,hive-cluster6-maxid.txt"