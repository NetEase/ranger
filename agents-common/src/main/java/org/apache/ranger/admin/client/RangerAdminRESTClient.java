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

 package org.apache.ranger.admin.client;


import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.util.*;

public class RangerAdminRESTClient implements RangerAdminClient {
	private static final Log LOG = LogFactory.getLog(RangerAdminRESTClient.class);
 
	private String           serviceName = null;
	private String           pluginId    = null;
	private RangerRESTClient restClient  = null;
	private RangerRESTUtils  restUtils   = new RangerRESTUtils();
	private String 					 sslConfigFileName = null;
	private int 					 	 restClientConnTimeOutMs = 120 * 1000;
	private int 					   restClientReadTimeOutMs = 30 * 1000;
	private String 					 serviceUrls[];

	public RangerAdminRESTClient() {
	}

	@Override
	public void init(String serviceName, String appId, String propertyPrefix) {
		this.serviceName = serviceName;
		this.pluginId    = restUtils.getPluginId(serviceName, appId);

		String url               			= RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.url");
		this.serviceUrls = url.split(",");
		this.sslConfigFileName 				= RangerConfiguration.getInstance().get(propertyPrefix + ".policy.rest.ssl.config.file");
		this.restClientConnTimeOutMs	= RangerConfiguration.getInstance().getInt(propertyPrefix + ".policy.rest.client.connection.timeoutMs", 120 * 1000);
		this.restClientReadTimeOutMs	= RangerConfiguration.getInstance().getInt(propertyPrefix + ".policy.rest.client.read.timeoutMs", 30 * 1000);
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + ")");
		}

		ServicePolicies ret = null;

		int index = 0;
		do {
			try {
				if (null == restClient) {
					restClient = init(serviceUrls[index], sslConfigFileName, restClientConnTimeOutMs, restClientReadTimeOutMs);
				}
				WebResource webResource = restClient.getResource(RangerRESTUtils.REST_URL_POLICY_GET_FOR_SERVICE_IF_UPDATED + serviceName)
						.queryParam(RangerRESTUtils.REST_PARAM_LAST_KNOWN_POLICY_VERSION, Long.toString(lastKnownVersion))
						.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
				ClientResponse response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);

				if(response != null && response.getStatus() == 200) {
					ret = response.getEntity(ServicePolicies.class);
					break;
				} else if(response != null && response.getStatus() == 304) {
					// no change
					break;
				} else {
					RESTResponse resp = RESTResponse.fromClientResponse(response);
					LOG.error("Error getting policies. request=" + webResource.toString()
							+ ", response=" + resp.toString() + ", serviceName=" + serviceName);
					throw new Exception(resp.getMessage());
				}
			} catch (Exception e) {
				restClient = null;
				LOG.error("Error getting policies. request=" + serviceUrls[index++] + ", serviceName=" + serviceName);
				e.printStackTrace();
			}
		} while(index < serviceUrls.length);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.getServicePoliciesIfUpdated(" + lastKnownVersion + "): " + ret);
		}

		return ret;
	}

	@Override
	public void grantAccess(GrantRevokeRequest request) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + request + ")");
		}

		int index = 0;
		do {
			try {
				if (null == restClient) {
					restClient = init(serviceUrls[index], sslConfigFileName, restClientConnTimeOutMs, restClientReadTimeOutMs);
				}
				WebResource webResource = restClient.getResource(RangerRESTUtils.REST_URL_SERVICE_GRANT_ACCESS + serviceName)
						.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
				ClientResponse response = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE)
						.type(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));

				if(response != null && response.getStatus() != 200) {
					LOG.error("grantAccess() failed: HTTP status=" + response.getStatus());
					throw new Exception("HTTP " + response.getStatus());
				} else if(response == null) {
					throw new Exception("unknown error during grantAccess. serviceName="  + serviceName);
				} else {
					break;
				}
			} catch (Exception e) {
				restClient = null;
				LOG.error("Error getting policies. request=" + serviceUrls[index++] + ", serviceName=" + serviceName);
				e.printStackTrace();
			}
		} while(index < serviceUrls.length);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantAccess(" + request + ")");
		}
	}

	@Override
	public void revokeAccess(GrantRevokeRequest request) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.revokeAccess(" + request + ")");
		}

		int index = 0;
		do {
			try {
				if (null == restClient) {
					restClient = init(serviceUrls[index], sslConfigFileName, restClientConnTimeOutMs, restClientReadTimeOutMs);
				}
				WebResource webResource = restClient.getResource(RangerRESTUtils.REST_URL_SERVICE_REVOKE_ACCESS + serviceName)
						.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId);
				ClientResponse response = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE)
						.type(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));

				if (response != null && response.getStatus() != 200) {
					LOG.error("revokeAccess() failed: HTTP status=" + response.getStatus());
					throw new Exception("HTTP " + response.getStatus());
				} else if (response == null) {
					throw new Exception("unknown error. revokeAccess(). serviceName=" + serviceName);
				} else {
					break;
				}
			} catch (Exception e) {
				restClient = null;
				LOG.error("Error getting policies. request=" + serviceUrls[index++] + ", serviceName=" + serviceName);
				e.printStackTrace();
			}
		} while(index < serviceUrls.length);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.revokeAccess(" + request + ")");
		}
	}

	@Override
	public void consistentRules(GrantRevokeRequest request, HiveOperationType hiveOperationType) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.consistentRules(" + request + ")");
		}

		int index = 0;
		do {
			try {
				if (null == restClient) {
					restClient = init(serviceUrls[index], sslConfigFileName, restClientConnTimeOutMs, restClientReadTimeOutMs);
				}
				WebResource webResource = restClient.getResource(RangerRESTUtils.REST_URL_SERVICE_CONSISTENCY_RULES + serviceName)
						.queryParam(RangerRESTUtils.REST_PARAM_PLUGIN_ID, pluginId)
						.queryParam("hiveOperationType", hiveOperationType.name());
				ClientResponse response = webResource.accept(RangerRESTUtils.REST_EXPECTED_MIME_TYPE)
						.type(RangerRESTUtils.REST_EXPECTED_MIME_TYPE).post(ClientResponse.class, restClient.toJson(request));

				if (response != null && response.getStatus() != 200) {
					LOG.error("consistentRules() failed: HTTP status=" + response.getStatus());
					throw new Exception("HTTP " + response.getStatus());
				} else if (response == null) {
					throw new Exception("unknown error. consistentRules(). serviceName=" + serviceName);
				} else {
					break;
				}
			} catch (Exception e) {
				restClient = null;
				LOG.error("Error getting policies. request=" + serviceUrls[index++] + ", serviceName=" + serviceName);
				e.printStackTrace();
			}
		} while(index < serviceUrls.length);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.consistentRules(" + request + ")");
		}
	}

	private RangerRESTClient init(String url, String sslConfigFileName, int restClientConnTimeOutMs,
																int restClientReadTimeOutMs ) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.init(" + url + ", " + sslConfigFileName + ")");
		}

		RangerRESTClient restClient = new RangerRESTClient(url, sslConfigFileName);
		restClient.setRestClientConnTimeOutMs(restClientConnTimeOutMs);
		restClient.setRestClientReadTimeOutMs(restClientReadTimeOutMs);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.init(" + url + ", " + sslConfigFileName + ")");
		}

		return restClient;
	}
}
