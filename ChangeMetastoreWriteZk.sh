#!/usr/bin/env bash

# 如果 Zookeeper 配置了 Acl 权限控制，需要先 kinit
# kinit -kt /etc/security/keytabs/hive.service.keytab hive/hadoop710.lt.163.org@TEST.AMBARI.NETEASE.COM

if [[ $# -eq 0 ]]; then
  echo "$BASH_SOURCE:$LINENO 请输入执行参数!"
  exit;
fi
# echo $1 $2 $3

if [ $1 = 'delete' ]; then
echo $1 $2
/usr/ndp/current/zookeeper_server/bin/zkCli.sh -server hadoop710.lt.163.org:2181 <<EOF
delete $2
quit
EOF
else
echo $1 $2 $3
/usr/ndp/current/zookeeper_server/bin/zkCli.sh -server hadoop710.lt.163.org:2181 <<EOF
$1 $2 $3
quit
EOF
fi