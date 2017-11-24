#!/usr/bin/env bash

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

# $1:zookeeperQueum
# $2:command
# $3:znode path
# $4:params

metastore_change_path=$1

BASE_PATH=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)

# get config parameters
. ${BASE_PATH}/monitor-env.sh

if [[ $# -eq 0 ]]; then
  echo "$BASH_SOURCE:$LINENO 请输入执行参数!"
  exit;
fi
# echo $1 $2 $3 $4

> "$BASE_PATH"/../metastore_result.txt
${ZOOKEEPER_CLI_PATH} -server ${ZOOKEEPER_SERVER} <<EOF >"$BASE_PATH"/../metastore_result.txt
get ${metastore_change_path}
quit
EOF
