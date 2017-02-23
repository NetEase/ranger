/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.plugin.util;


import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;

/**
 * Since this class does not retain any state.  It isn't a singleton for testability. 
 *
 */
public class RangerRESTUtils {

	private static final Log LOG = LogFactory.getLog(RangerRESTUtils.class);

	public static final String REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED = "/service/plugins/policies/download/";
	public static final String REST_URL_POLICY_GET_SERVICE_INI_IF_UPDATED = "/service/plugins/policies/download/ini/";
	public static final String REST_URL_POLICY_SYNC_HDFS                  = "/service/plugins/policies/sync_hdfs/";
	public static final String REST_URL_SERVICE_GRANT_ACCESS              = "/service/plugins/services/grant/";
	public static final String REST_URL_SERVICE_REVOKE_ACCESS             = "/service/plugins/services/revoke/";

	public static final String REST_EXPECTED_MIME_TYPE = "application/json" ;
	public static final String REST_MIME_TYPE_JSON     = "application/json" ;

	public static final String REST_PARAM_LAST_KNOWN_POLICY_VERSION = "lastKnownVersion";
	public static final String REST_PARAM_PLUGIN_ID                 = "pluginId";

	private static final int MAX_PLUGIN_ID_LEN = 255 ;


	public String getPolicyRestUrl(String propertyPrefix) {
		String url = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.url");
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerRESTUtils.getPolicyRestUrl(" + url + ")");
		}

		return url;
	}
	
	public String getSsslConfigFileName(String propertyPrefix) {
		String sslConfigFileName = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.ssl.config.file");

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerRESTUtils.getSsslConfigFileName(" + sslConfigFileName + ")");
		}

		return sslConfigFileName;
	}
	
	public String getUrlForPolicyUpdate(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceName;
		
		return url;
	}

	public boolean isSsl(String _baseUrl) {
		return StringUtils.isEmpty(_baseUrl) ? false : _baseUrl.toLowerCase().startsWith("https");
	}

	public String getUrlForGrantAccess(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_SERVICE_GRANT_ACCESS + serviceName;
		
		return url;
	}

	public String getUrlForRevokeAccess(String baseUrl, String serviceName) {
		String url = baseUrl + REST_URL_SERVICE_REVOKE_ACCESS + serviceName;
		
		return url;
	}

    public String getPluginId(String serviceName, String appId) {
        String hostName = null;

        try {
            hostName = InetAddress.getLocalHost().getHostName() ;
        } catch (UnknownHostException e) {
            LOG.error("ERROR: Unable to find hostname for the agent ", e);
            hostName = "unknownHost" ;
        }

        String ret  = hostName + "-" + serviceName ;

        if(! StringUtils.isEmpty(appId)) {
        	ret = appId + "@" + ret;
        }

        if (ret.length() > MAX_PLUGIN_ID_LEN ) {
        	ret = ret.substring(0,MAX_PLUGIN_ID_LEN) ;
        }

        return ret  ;
    }
}
