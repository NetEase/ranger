#!/usr/bin/env bash

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