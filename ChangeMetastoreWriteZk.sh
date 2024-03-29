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


# 如果 Zookeeper 配置了 Acl 权限控制，需要先 kinit
# kinit -kt /etc/security/keytabs/hive.service.keytab hive/hadoop710.lt.163.org@TEST.AMBARI.NETEASE.COM

if [[ $# -eq 0 ]]; then
  echo "$BASH_SOURCE:$LINENO 请输入执行参数!"
  exit;
fi
# echo $1 $2 $3 $4

if [ $2 = 'delete' ]; then
echo $1 $2 $3
/usr/ndp/current/zookeeper_client/bin/zkCli.sh -server $1 <<EOF
$2 $3
quit
EOF
else
echo $1 $2 $3 $4
/usr/ndp/current/zookeeper_client/bin/zkCli.sh -server $1 <<EOF
$2 $3 $4
quit
EOF
fi