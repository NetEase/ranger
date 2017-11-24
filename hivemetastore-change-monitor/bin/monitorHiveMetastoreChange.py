# -*- coding: utf-8 -*-
#!/usr/bin/env python
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

import os
import time
import logging
import subprocess as sp
import sys
import alertUtils
import logging.handlers

baseHome = os.path.abspath(sys.path[0] + '/..')
int_preMaxId=999999999
int_currentMaxId=0


#logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S',filename=baseHome+'/logs/hiveMetastoreMonitor.log',filemode='a')

def log_setup():
    log_handler = logging.handlers.TimedRotatingFileHandler(baseHome+'/logs/hiveMetastoreMonitorChange.log',when="midnight")
    formatter = logging.Formatter(
        '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        '%a, %d %b %Y %H:%M:%S')
    log_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)


def check(metastore_change_path):
    global int_preMaxId
    global int_currentMaxId

    logging.info('now checking hive metastore change maxid' + metastore_change_path)
    filePreMaxid = open(baseHome + '/maxid.txt','r+')
    preMaxId = filePreMaxid.read()
    if preMaxId.strip() == '':
        preMaxId = '0'
    int_preMaxId = int(preMaxId)

    os.system(baseHome + "/bin/metastoreChange-check.sh " + metastore_change_path)
    f = open(baseHome + '/metastore_result.txt','r')
    lines = f.readlines()
    lines_length = len(lines)
    if lines_length >= 5:
        currentMaxId = lines[lines_length-5];
        int_currentMaxId = int(currentMaxId)

        print 'int_preMaxId = %d' % int_preMaxId
        print 'int_currentMaxId = %d' % int_currentMaxId
        logging.info('int_preMaxId = ' + str(int_preMaxId))
        logging.info('int_currentMaxId = ' + str(int_currentMaxId))

        if int_currentMaxId > int_preMaxId:
            filePreMaxid.seek(0,0)
            filePreMaxid.write(currentMaxId)
            filePreMaxid.close()
            return True
        else:
            return False
    else:
        return False


def getEnvValFromScript(script):
    proc = sp.Popen(['bash', '-c', 'source {} && env'.format(script)], stdout=sp.PIPE)
    source_env = {tup[0].strip(): tup[1].strip() for tup in map(lambda s: s.strip().split('=', 1), proc.stdout)}
    return source_env

log_setup()
source_env = getEnvValFromScript(baseHome + "/bin/monitor-env.sh")
metastore_change_paths = source_env['METASTORE_CHANGE_PATH'].split(",")

for change_path in metastore_change_paths:
    if check(change_path):
        logging.info('The hive metastore, of which namespace is ' + change_path + ', is so good ~ ')
    else:
        logging.error('The hive metastore, of which namespace is ' + change_path + ', is dead! Send alert')
        alertUtils.sendAlert("mail","MetaStoreChange error ", change_path + ", int_preMaxId=" + str(int_preMaxId) + ", int_currentMaxId=" + str(int_currentMaxId))

logging.info(' -----------------------------\n')