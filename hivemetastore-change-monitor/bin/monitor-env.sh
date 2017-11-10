#!/bin/bash

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
export ZOOKEEPER_SERVER="holmes-mm-5.hz.infra.mail"
export METASTORE_CHANGE_PATH=/hive-metastore-changelog/metastore1/maxid