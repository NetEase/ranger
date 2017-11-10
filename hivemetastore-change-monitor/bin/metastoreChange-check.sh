#!/usr/bin/env bash

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
${ZOOKEEPER_CLI_PATH} -server ${ZOOKEEPER_SERVER} <<EOF
get ${metastore_change_path}
EOF
>> "$BASE_PATH"/../metastore_result.txt
