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

package org.apache.ranger.services.yarn.client;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.services.yarn.client.YarnClient;
import org.apache.ranger.services.yarn.client.json.model.YarnSchedulerResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class YarnClient extends BaseClient {

	public static final Logger LOG = Logger.getLogger(YarnClient.class) ;

	private static final String EXPECTED_MIME_TYPE = "application/json";
	
	private static final String YARN_LIST_API_ENDPOINT = "/ws/v1/cluster/scheduler" ;
	
	private static final String errMessage =  " You can still save the repository and start creating "
											  + "policies, but you would not be able to use autocomplete for "
											  + "resource names. Check xa_portal.log for more info.";

	
	String yarnQUrl;
	String userName;
	String password;

	public  YarnClient(String serviceName, Map<String, String> configs) {

		super(serviceName,configs,"yarn-client") ;

		this.yarnQUrl = configs.get("yarn.url");
		this.userName = configs.get("username");
		this.password = configs.get("password");
		
		if (this.yarnQUrl == null || this.yarnQUrl.isEmpty()) {
			LOG.error("No value found for configuration 'yarn.url'. YARN resource lookup will fail");
        } 
		if (this.userName == null || this.userName.isEmpty()) {
            LOG.error("No value found for configuration 'usename'. YARN resource lookup will fail");
        } 
		if (this.password == null || this.password.isEmpty()) {
            LOG.error("No value found for configuration 'password'. YARN resource lookup will fail");
        }

		if (LOG.isDebugEnabled()) {
			LOG.debug("Yarn Client is build with url [" + this.yarnQUrl + "] user: [" + this.userName + "], password: [" + "*********" + "]");
		}
	}
	
	public List<String> getQueueList(final String queueNameMatching, final List<String> existingQueueList) {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting Yarn queue list for queueNameMatching : " + queueNameMatching);
		}
		final String errMsg 	= errMessage;
		
		List<String> ret = null;

		Callable<List<String>> callableYarnQListGetter = new Callable<List<String>>() {

			@Override
			public List<String> call() {
				List<String> yarnQueueListGetter = null;

				Subject subj = getLoginSubject();

				if (subj != null) {
					yarnQueueListGetter = Subject.doAs(subj, new PrivilegedAction<List<String>>() {

					@Override
					public List<String> run() {

						List<String> lret = new ArrayList<String>();

						String url = yarnQUrl + YARN_LIST_API_ENDPOINT ;

						Client client = null ;

						ClientResponse response = null ;

						try {
							client = Client.create() ;

							WebResource webResource = client.resource(url);

							response = webResource.accept(EXPECTED_MIME_TYPE)
								    .get(ClientResponse.class);

							if (LOG.isDebugEnabled()) {
								LOG.debug("getQueueList():calling " + url);
							}

							if (response != null) {
								if (LOG.isDebugEnabled()) {
									LOG.debug("getQueueList():response.getStatus()= " + response.getStatus());	
								}
								if (response.getStatus() == 200) {
									String jsonString = response.getEntity(String.class);
									Gson gson = new GsonBuilder().setPrettyPrinting().create();
									YarnSchedulerResponse yarnQResponse = gson.fromJson(jsonString, YarnSchedulerResponse.class);
									if (yarnQResponse != null) {
										List<String>  yarnQueueList = yarnQResponse.getQueueNames();
										if (yarnQueueList != null) {
											for ( String yarnQueueName : yarnQueueList) {
												if ( existingQueueList != null && existingQueueList.contains(yarnQueueName)) {
										        	continue;
										        }
												if (queueNameMatching == null || queueNameMatching.isEmpty()
														|| yarnQueueName.startsWith(queueNameMatching)) {
														if (LOG.isDebugEnabled()) {
															LOG.debug("getQueueList():Adding yarnQueue " + yarnQueueName);
														}
														lret.add(yarnQueueName) ;
													}
												}
											}
										}
								 } else{
									LOG.info("getQueueList():response.getStatus()= " + response.getStatus() + " for URL " + url + ", so returning null list");
									String jsonString = response.getEntity(String.class);
									LOG.info(jsonString);
									lret = null;
								}
							} else {
								lret = null;
								String msgDesc = "Unable to get a valid response for "
										+ "expected mime type : [" + EXPECTED_MIME_TYPE
										+ "] URL : " + url + " - got null response.";
								LOG.error(msgDesc);
								HadoopException hdpException = new HadoopException(msgDesc);
								hdpException.generateResponseDataMap(false, msgDesc,
										msgDesc + errMsg, null, null);
								throw hdpException;
							}
						} catch (HadoopException he) {
							lret = null;
							throw he;
						} catch (Throwable t) {
							lret = null;
							String msgDesc = "Exception while getting Yarn Queue List."
									+ " URL : " + url;
							HadoopException hdpException = new HadoopException(msgDesc,
										t);

							LOG.error(msgDesc, t);

							hdpException.generateResponseDataMap(false,
									BaseClient.getMessage(t), msgDesc + errMsg, null,
									null);
							throw hdpException;

						} finally {
							if (response != null) {
								response.close();
							}

							if (client != null) {
								client.destroy(); 
							}
						}
						return lret ;
					}
				  } );
				}
				return yarnQueueListGetter;
			  }
			};
		
		try {
			ret = timedTask(callableYarnQListGetter, 5, TimeUnit.SECONDS);
		} catch ( Throwable t) {
			LOG.error("Unable to get Yarn Queue list from [" + yarnQUrl + "]", t) ;
			String msgDesc = "Unable to get a valid response for "
					+ "expected mime type : [" + EXPECTED_MIME_TYPE
					+ "] URL : " + yarnQUrl;
			HadoopException hdpException = new HadoopException(msgDesc,
					t);
			LOG.error(msgDesc, t);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(t), msgDesc + errMsg, null,
					null);
			throw hdpException;
		}
		return ret;
	}
	
	
	
	
	
	public static HashMap<String, Object> testConnection(String serviceName,
			Map<String, String> configs) {

		List<String> strList = new ArrayList<String>();
		String errMsg = errMessage;
		boolean connectivityStatus = false;
		HashMap<String, Object> responseData = new HashMap<String, Object>();

		YarnClient yarnClient = getYarnClient(serviceName,
				configs);
		strList = getYarnResource(yarnClient, "",null);

		if (strList != null && strList.size() > 0 ) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("TESTING list size" + strList.size() + " Yarn Queues");
			}
			connectivityStatus = true;
		}

		if (connectivityStatus) {
			String successMsg = "TestConnection Successful";
			BaseClient.generateResponseDataMap(connectivityStatus, successMsg,
					successMsg, null, null, responseData);
		} else {
			String failureMsg = "Unable to retrieve any Yarn Queues using given parameters.";
			BaseClient.generateResponseDataMap(connectivityStatus, failureMsg,
					failureMsg + errMsg, null, null, responseData);
		}

		return responseData;
	}

	public static YarnClient getYarnClient(String serviceName,
			Map<String, String> configs) {
		YarnClient yarnClient = null;
		if (LOG.isDebugEnabled()) {
			LOG.debug("Getting YarnClient for datasource: " + serviceName);
		}
		String errMsg = errMessage;
		if (configs == null || configs.isEmpty()) {
			String msgDesc = "Could not connect as Connection ConfigMap is empty.";
			LOG.error(msgDesc);
			HadoopException hdpException = new HadoopException(msgDesc);
			hdpException.generateResponseDataMap(false, msgDesc, msgDesc
					+ errMsg, null, null);
			throw hdpException;
		} else {
			yarnClient = new YarnClient (serviceName, configs);
		}
		return yarnClient;
	}

	public static List<String> getYarnResource (final YarnClient yarnClient,
			String yanrQname, List<String> existingQueueName) {

		List<String> resultList = new ArrayList<String>();
		String errMsg = errMessage;

		try {
			if (yarnClient == null) {
				String msgDesc = "Unable to get Yarn Queue : YarnClient is null.";
				LOG.error(msgDesc);
				HadoopException hdpException = new HadoopException(msgDesc);
				hdpException.generateResponseDataMap(false, msgDesc, msgDesc
						+ errMsg, null, null);
				throw hdpException;
			}

			if (yanrQname != null) {
				String finalyarnQueueName = yanrQname.trim();
				resultList = yarnClient
						.getQueueList(finalyarnQueueName,existingQueueName);
				if (resultList != null) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Returning list of " + resultList.size() + " Yarn Queues");
					}
				}
			}
		}
		catch (HadoopException he) {
			throw he;
		}
		catch (Throwable t) {
			String msgDesc = "getYarnResource: Unable to get Yarn resources.";
			LOG.error(msgDesc, t);
			HadoopException hdpException = new HadoopException(msgDesc);

			hdpException.generateResponseDataMap(false,
					BaseClient.getMessage(t), msgDesc + errMsg, null, null);
			throw hdpException;
		}
		return resultList;
	}
	
	public static <T> T timedTask(Callable<T> callableObj, long timeout,
			TimeUnit timeUnit) throws Exception {
		return callableObj.call();
	}
}
