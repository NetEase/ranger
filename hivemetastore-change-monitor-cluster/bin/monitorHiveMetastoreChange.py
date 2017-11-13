# -*- coding: utf-8 -*-
#!/usr/bin/env python
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


def check(metastore_change_path, maxid_file):
    global int_preMaxId
    global int_currentMaxId

    logging.info('now checking hive metastore change maxid' + metastore_change_path)
    logging.info('maxid_file = ' + maxid_file)
    filePreMaxid = open(baseHome + '/' + maxid_file,'r+')
    preMaxId = filePreMaxid.read()
    if preMaxId.strip() == '':
        preMaxId = '0'

    int_preMaxId = int(preMaxId)

    os.system(baseHome + "/bin/metastoreChange-check.sh " + metastore_change_path)
    f = open(baseHome + '/metastore_result.txt','r')
    lines = f.readlines()
    f.close()
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
maxid_files = source_env['MAX_ID_FILES'].split(",")

index = -1
strBody = ""

for change_path in metastore_change_paths:
    index = index+1

    if check(change_path, maxid_files[index]):
        logging.info('The hive metastore change ' + change_path + ', is so good ~ ')
    else:
        strBody = strBody + change_path + ", int_preMaxId=" + str(int_preMaxId) + ", int_currentMaxId=" + str(int_currentMaxId) + "\n--------\n"
        logging.error('The hive metastore change ' + change_path + ', is dead! Send alert')

if strBody != "":
        alertUtils.sendAlert("mail","MetaStoreChange error", strBody)
        alertUtils.sendAlert("sms","MetaStoreChange error", strBody)


logging.info(' -----------------------------\n')
