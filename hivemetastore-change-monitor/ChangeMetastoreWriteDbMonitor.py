# -*- coding: utf-8 -*-
#!/usr/bin/env python

import httplib, urllib, urllib2, logging, os, time, sys

baseHome = os.path.abspath(sys.path[0] + '/..')

alertModes1 = {"mail": "http://bigdata0.photo.163.org:8080/sentry/sendmail2.jsp", "sms": "http://bigdata0.photo.163.org:8080/sentry/sendsms.jsp"}
alertModes = {"mail": "http://172.16.87.242:8080/omnew/alert/sendMultiAlert", "sms": "http://bigdata0.photo.163.org:8080/sentry/sendsms.jsp"}
receivers2 = {"mail": "hzliuxun@corp.netease.com", "sms": "13777495757"}
receivers = {"mail": "hzliuxun@corp.netease.com,chenju@corp.netease.com,hzgengxin@corp.netease.com", "sms": "13777495757,13867471941,15558132155"}

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

sendAlert("mail", "YX MetaStoreChangeWriteDb error!", "YX MetaStoreChangeWriteDb error!")