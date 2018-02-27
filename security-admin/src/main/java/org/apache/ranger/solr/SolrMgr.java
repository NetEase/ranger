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

package org.apache.ranger.solr;

import org.apache.log4j.Logger;
import org.apache.ranger.audit.utils.InMemoryJAASConfiguration;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Properties;

/**
 * This class initializes Solr
 *
 */
@Component
public class SolrMgr {

	static final Logger logger = Logger.getLogger(SolrMgr.class);

	@Autowired
	RangerBizUtil rangerBizUtil;

	static final Object lock = new Object();

	SolrClient solrClient = null;
	Date lastConnectTime = null;
	volatile boolean initDone = false;

	final static String SOLR_URLS_PROP = "ranger.audit.solr.urls";
	final static String SOLR_ZK_HOSTS = "ranger.audit.solr.zookeepers";
	final static String SOLR_COLLECTION_NAME = "ranger.audit.solr.collection.name";
	final static String PROP_SOLR_KERBEROS   = "java.security.auth.login.config";

	public static final String DEFAULT_COLLECTION_NAME = "ranger_audits";

	public SolrMgr() {
		init();
	}

	void connect() {
		if (!initDone) {
			synchronized (lock) {
				if (!initDone) {
					if (rangerBizUtil.getAuditDBType().equalsIgnoreCase("solr")) {
						String zkHosts = PropertiesUtil
								.getProperty(SOLR_ZK_HOSTS);
						if (zkHosts == null) {
							zkHosts = PropertiesUtil
									.getProperty("ranger.audit.solr.zookeeper");
						}
						if (zkHosts == null) {
							zkHosts = PropertiesUtil
									.getProperty("ranger.solr.zookeeper");
						}

						String solrURL = PropertiesUtil
								.getProperty(SOLR_URLS_PROP);

						if (solrURL == null) {
							// Try with url
							solrURL = PropertiesUtil
									.getProperty("ranger.audit.solr.url");
						}
						if (solrURL == null) {
							// Let's try older property name
							solrURL = PropertiesUtil
									.getProperty("ranger.solr.url");
						}

						if (zkHosts != null && !zkHosts.trim().equals("")
								&& !zkHosts.trim().equalsIgnoreCase("none")) {
							zkHosts = zkHosts.trim();
							String collectionName = PropertiesUtil
									.getProperty(SOLR_COLLECTION_NAME);
							if (collectionName == null
									|| collectionName.equalsIgnoreCase("none")) {
								collectionName = DEFAULT_COLLECTION_NAME;
							}

							logger.info("Solr zkHosts=" + zkHosts
									+ ", collectionName=" + collectionName);

							try {
								// Instantiate
								HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
								CloudSolrClient solrCloudClient = new CloudSolrClient(
										zkHosts);
								solrCloudClient
										.setDefaultCollection(collectionName);
								solrClient = solrCloudClient;
							} catch (Throwable t) {
								logger.fatal(
										"Can't connect to Solr server. ZooKeepers="
												+ zkHosts + ", collection="
												+ collectionName, t);
							}

						} else {
							if (solrURL == null || solrURL.isEmpty()
									|| solrURL.equalsIgnoreCase("none")) {
								logger.fatal("Solr ZKHosts and URL for Audit are empty. Please set property "
										+ SOLR_ZK_HOSTS
										+ " or "
										+ SOLR_URLS_PROP);
							} else {
								try {
									HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
									solrClient = new HttpSolrClient(solrURL);
									if (solrClient == null) {
										logger.fatal("Can't connect to Solr. URL="
												+ solrURL);
									} else {
										if (solrClient instanceof HttpSolrClient) {
											HttpSolrClient httpSolrClient = (HttpSolrClient) solrClient;
											httpSolrClient
													.setAllowCompression(true);
											httpSolrClient
													.setConnectionTimeout(1000);
											// httpSolrClient.setSoTimeout(10000);
											httpSolrClient.setMaxRetries(1);
											httpSolrClient
													.setRequestWriter(new BinaryRequestWriter());
										}
										initDone = true;
									}

								} catch (Throwable t) {
									logger.fatal(
											"Can't connect to Solr server. URL="
													+ solrURL, t);
								}
							}
						}
					}

				}
			}
		}
	}

	private void init() {
		logger.info("==>SolrMgr.init()" );
		Properties props = PropertiesUtil.getProps();
		try {
			// SolrJ requires "java.security.auth.login.config"  property to be set to identify itself that it is kerberized. So using a dummy property for it
			// Acutal solrclient JAAS configs are read from the ranger-admin-site.xml in ranger admin config folder and set by InMemoryJAASConfiguration
			// Refer InMemoryJAASConfiguration doc for JAAS Configuration
			if ( System.getProperty(PROP_SOLR_KERBEROS) == null ) {
				System.setProperty(PROP_SOLR_KERBEROS, "/dev/null") ;
			}
			logger.info("Loading SolrClient JAAS config from Ranger audit config if present...");
			InMemoryJAASConfiguration.init(props);
		} catch (Exception e) {
			logger.error("ERROR: Unable to load SolrClient JAAS config from ranger admin config file. Audit to Kerberized Solr will fail...");
			if (logger.isDebugEnabled()) {
				logger.debug("ERROR: Unable to load SolrClient JAAS config from ranger admin file. Audit to Kerberized Solr will fail...", e);
			}
		}
		logger.info("<==SolrMgr.init()" );
	}

	public SolrClient getSolrClient() {
		if (solrClient == null) {
			connect();
		}
		return solrClient;
	}

}
