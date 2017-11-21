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

import httplib, urllib, urllib2, logging, os, time, sys

baseHome = os.path.abspath(sys.path[0] + '/..')

alertModes1 = {"mail": "http://bigdata0.photo.163.org:8080/sentry/sendmail2.jsp", "sms": "http://bigdata0.photo.163.org:8080/sentry/sendsms.jsp"}
alertModes = {"mail": "http://172.16.87.242:8080/omnew/alert/sendMultiAlert", "sms": "http://bigdata0.photo.163.org:8080/sentry/sendsms.jsp"}
receivers2 = {"mail": "hzliuxun@corp.netease.com", "sms": "13777495757"}
receivers = {"mail": "hzliuxun@corp.netease.com,chenju@corp.netease.com", "sms": "13777495757,13867471941"}

def sendAlert(alertType, subject, body):
        url = alertModes[alertType]
        body_value = {"account": receivers[alertType] ,"subject": subject, "mobile": receivers["sms"], "emailMsg": body, "popoMsg": body, "mobileMsg": body, "product":"holmes_alarm", "type":7}

        print(body_value)

#       body_value  = urllib.urlencode(body_value)
        request = urllib2.Request(url, str(body_value))
        request.add_header("Content-type", "application/json")
        request.add_header("Accept", "text/plain")
        resp = urllib2.urlopen(request )
        logging.info( "send alters to " + receivers[alertType] + " by "+ alertType + ". Got response code " + bytes(resp.getcode()))

'''subject = "testAlertFunction"
body = "get alert from hadoop707"
to = receivers["mail"]
sendAlert("mail",  subject, body)'''