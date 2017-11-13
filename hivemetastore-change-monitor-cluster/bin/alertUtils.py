# -*- coding: utf-8 -*-
#!/usr/bin/env python

import httplib, urllib, urllib2, logging, os, time, sys

baseHome = os.path.abspath(sys.path[0] + '/..')

alertModes = {"mail": "http://bigdata0.photo.163.org:8080/sentry/sendmail2.jsp", "sms": "http://bigdata0.photo.163.org:8080/sentry/sendsms.jsp"}
receivers = {"mail": "hzliuxun@corp.netease.com", "sms": "13777495757"}

def sendAlert(alertType, subject, body):
        url = alertModes[alertType]
        body_value = {"to": receivers[alertType] ,"subject": subject, "body": body}
        body_value  = urllib.urlencode(body_value)
        request = urllib2.Request(url, body_value)
        request.add_header("Content-type", "application/x-www-form-urlencoded")
        request.add_header("Accept", "text/plain")
        resp = urllib2.urlopen(request )
        logging.info( "send alters to " + receivers[alertType] + " by "+ alertType + ". Got response code " + bytes(resp.getcode()))