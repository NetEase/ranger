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

package org.apache.ranger.plugin.service;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.policyengine.RangerResourceAccessInfo;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyEvaluator;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.PolicyRefresher;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;


public class RangerBasePlugin {
	private static final Log LOG = LogFactory.getLog(RangerBasePlugin.class);

	private String                    serviceType  = null;
	private String                    appId        = null;
	private String                    serviceName  = null;
	private PolicyRefresher           refresher    = null;
	private RangerPolicyEngine        policyEngine = null;
	private RangerPolicyEngineOptions policyEngineOptions = new RangerPolicyEngineOptions();
	private RangerAccessResultProcessor resultProcessor = null;
	private RangerAdminClient         rangerAdminClient = null;
	private Timer 					  policyEngineRefreshTimer;

	Map<String, LogHistory> logHistoryList = new Hashtable<String, RangerBasePlugin.LogHistory>();
	int logInterval = 30000; // 30 seconds


	public RangerBasePlugin(String serviceType, String appId) {
		this.serviceType = serviceType;
		this.appId       = appId;
	}

	public RangerAdminClient getRangerAdminClient() { return rangerAdminClient; }

	public String getServiceType() {
		return serviceType;
	}

	public RangerServiceDef getServiceDef() {
		RangerPolicyEngine policyEngine = this.policyEngine;

		return policyEngine != null ? policyEngine.getServiceDef() : null;
	}

	public int getServiceDefId() {
		RangerServiceDef serviceDef = getServiceDef();

		return serviceDef != null && serviceDef.getId() != null ? serviceDef.getId().intValue() : -1;
	}

	public String getAppId() {
		return appId;
	}
	public String getServiceName() {
		return serviceName;
	}

	public void init(String serviceType, String appId) {
		this.serviceType = serviceType;
		this.appId       = appId;
		init();
	}


	public void init() {
		cleanup();

		RangerConfiguration.getInstance().addResourcesForServiceType(serviceType);
		RangerConfiguration.getInstance().initAudit(appId);

		String propertyPrefix    = "ranger.plugin." + serviceType;
		long   pollingIntervalMs = RangerConfiguration.getInstance().getLong(propertyPrefix + ".policy.pollIntervalMs", 30 * 1000);
		String cacheDir          = RangerConfiguration.getInstance().get(propertyPrefix + ".policy.cache.dir");

		serviceName = RangerConfiguration.getInstance().get(propertyPrefix + ".service.name");

		policyEngineOptions.evaluatorType           = RangerConfiguration.getInstance().get(propertyPrefix + ".policyengine.option.evaluator.type", RangerPolicyEvaluator.EVALUATOR_TYPE_AUTO);
		policyEngineOptions.cacheAuditResults       = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.cache.audit.results", true);
		policyEngineOptions.disableContextEnrichers = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.context.enrichers", false);
		policyEngineOptions.disableCustomConditions = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.custom.conditions", false);
		//policyEngineOptions.disableTrieLookupPrefilter = RangerConfiguration.getInstance().getBoolean(propertyPrefix + ".policyengine.option.disable.trie.lookup.prefilter", false);
		long policyReorderIntervalMs = RangerConfiguration.getInstance().getLong(propertyPrefix + ".policy.policyReorderInterval", 60 * 1000);
		if (policyReorderIntervalMs >= 0 && policyReorderIntervalMs < 15 * 1000) {
			policyReorderIntervalMs = 15 * 1000;
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(propertyPrefix + ".policy.policyReorderInterval:" + policyReorderIntervalMs);
		}

		rangerAdminClient = createAdminClient(propertyPrefix);

		refresher = new PolicyRefresher(this, serviceType, appId, serviceName, rangerAdminClient, pollingIntervalMs, cacheDir);

		refresher.startRefresher();

		if (policyReorderIntervalMs > 0) {
			policyEngineRefreshTimer = new Timer("PolicyEngineRefreshTimer", true);
			try {
				policyEngineRefreshTimer.schedule(new PolicyEngineRefresher(this), policyReorderIntervalMs, policyReorderIntervalMs);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Scheduled PolicyEngineRefresher to reorder policies nbased on number of evaluations in and every " + policyReorderIntervalMs + " milliseconds");
				}
			} catch (IllegalStateException exception) {
				LOG.error("Error scheduling policyEngineRefresher:", exception);
				LOG.error("*** PolicyEngine will NOT be reorderd based on number of evaluations every " + policyReorderIntervalMs + " milliseconds ***");
				policyEngineRefreshTimer = null;
			}
		} else {
			LOG.info("Policies will NOT be reordered based on number of evaluations because "
					+ propertyPrefix + ".policy.policyReorderInterval is set to a negative number[" + policyReorderIntervalMs +"]");
		}

	}

	public void setPolicies(ServicePolicies policies) {
		// guard against catastrophic failure during policy engine Initialization or
		try {
			RangerPolicyEngine policyEngine = new RangerPolicyEngineImpl(policies, policyEngineOptions);

			this.policyEngine = policyEngine;
		} catch (Exception e) {
			LOG.error("setPolicies: policy engine initialization failed!  Leaving current policy engine as-is.");
		}
	}

	public void cleanup() {
		PolicyRefresher refresher = this.refresher;
		Timer policyEngineRefreshTimer = this.policyEngineRefreshTimer;

		this.serviceName  = null;
		this.policyEngine = null;
		this.refresher    = null;
		this.policyEngineRefreshTimer = null;

		if(refresher != null) {
			refresher.stopRefresher();
		}
		if (policyEngineRefreshTimer != null) {
			policyEngineRefreshTimer.cancel();
		}

	}

	public void setResultProcessor(RangerAccessResultProcessor resultProcessor) {
		this.resultProcessor = resultProcessor;
	}

	public RangerAccessResultProcessor getResultProcessor() {
		return this.resultProcessor;
	}

	public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
		return isAccessAllowed(request, resultProcessor);
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests) {
		return isAccessAllowed(requests, resultProcessor);
	}

	public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(request);

			return policyEngine.isAccessAllowed(request, resultProcessor);
		}

		return null;
	}

	public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(requests);

			return policyEngine.isAccessAllowed(requests, resultProcessor);
		}

		return null;
	}

	public RangerResourceAccessInfo getResourceAccessInfo(RangerAccessRequest request) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			policyEngine.preProcess(request);

			return policyEngine.getResourceAccessInfo(request);
		}

		return null;
	}

	public RangerAccessResult createAccessResult(RangerAccessRequest request) {
		RangerPolicyEngine policyEngine = this.policyEngine;

		if(policyEngine != null) {
			return policyEngine.createAccessResult(request);
		}

		return null;
	}

	public void grantAccess(GrantRevokeRequest request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.grantAccess(" + request + ")");
		}

		PolicyRefresher   refresher = this.refresher;
		RangerAdminClient admin     = refresher == null ? null : refresher.getRangerAdminClient();
		boolean           isSuccess = false;

		try {
			if(admin == null) {
				throw new Exception("ranger-admin client is null");
			}

			admin.grantAccess(request);

			isSuccess = true;
		} finally {
			auditGrantRevoke(request, "grant", isSuccess, resultProcessor);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.grantAccess(" + request + ")");
		}
	}

	public void revokeAccess(GrantRevokeRequest request, RangerAccessResultProcessor resultProcessor) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.revokeAccess(" + request + ")");
		}

		PolicyRefresher   refresher = this.refresher;
		RangerAdminClient admin     = refresher == null ? null : refresher.getRangerAdminClient();
		boolean           isSuccess = false;

		try {
			if(admin == null) {
				throw new Exception("ranger-admin client is null");
			}

			admin.revokeAccess(request);

			isSuccess = true;
		} finally {
			auditGrantRevoke(request, "revoke", isSuccess, resultProcessor);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.revokeAccess(" + request + ")");
		}
	}


	private RangerAdminClient createAdminClient(String propertyPrefix) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerAdminRESTClient.createAdminClient(" + propertyPrefix + ")");
		}

		RangerAdminClient ret = null;

		String propertyName = propertyPrefix + ".policy.source.impl";
		String policySourceImpl = RangerConfiguration.getInstance().get(propertyName);

		if(StringUtils.isEmpty(policySourceImpl)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Value for property[%s] was null or empty. Unxpected! Will use policy source of type[%s]", propertyName, RangerAdminRESTClient.class.getName()));
			}
		} else {
			if (LOG.isDebugEnabled()) {
				LOG.debug(String.format("Value for property[%s] was [%s].", propertyName, policySourceImpl));
			}
			try {
				@SuppressWarnings("unchecked")
				Class<RangerAdminClient> adminClass = (Class<RangerAdminClient>)Class.forName(policySourceImpl);
				
				ret = adminClass.newInstance();
			} catch (Exception excp) {
				LOG.error("failed to instantiate policy source of type '" + policySourceImpl + "'. Will use policy source of type '" + RangerAdminRESTClient.class.getName() + "'", excp);
			}
		}

		if(ret == null) {
			ret = new RangerAdminRESTClient();
		}

		ret.init(serviceName, appId, propertyPrefix);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerAdminRESTClient.createAdminClient(" + propertyPrefix + "): policySourceImpl=" + policySourceImpl + ", client=" + ret);
		}
		return ret;
	}

	private void auditGrantRevoke(GrantRevokeRequest request, String action, boolean isSuccess, RangerAccessResultProcessor resultProcessor) {
		if(request != null && resultProcessor != null) {
			RangerAccessRequestImpl accessRequest = new RangerAccessRequestImpl();
	
			accessRequest.setResource(new RangerAccessResourceImpl(request.getResource()));
			accessRequest.setUser(request.getGrantor());
			accessRequest.setAccessType(RangerPolicyEngine.ADMIN_ACCESS);
			accessRequest.setAction(action);
			accessRequest.setClientIPAddress(request.getClientIPAddress());
			accessRequest.setClientType(request.getClientType());
			accessRequest.setRequestData(request.getRequestData());
			accessRequest.setSessionId(request.getSessionId());

			// call isAccessAllowed() to determine if audit is enabled or not
			RangerAccessResult accessResult = isAccessAllowed(accessRequest, null);

			if(accessResult != null && accessResult.getIsAudited()) {
				accessRequest.setAccessType(action);
				accessResult.setIsAllowed(isSuccess);

				if(! isSuccess) {
					accessResult.setPolicyId(-1);
				}

				resultProcessor.processResult(accessResult);
			}
		}
	}
	
	public boolean logErrorMessage(String message) {
		LogHistory log = logHistoryList.get(message);
		if (log == null) {
			log = new LogHistory();
			logHistoryList.put(message, log);
		}
		if ((System.currentTimeMillis() - log.lastLogTime) > logInterval) {
			log.lastLogTime = System.currentTimeMillis();
			int counter = log.counter;
			log.counter = 0;
			if( counter > 0) {
				message += ". Messages suppressed before: " + counter;
			}
			LOG.error(message);
			return true;
		} else {
			log.counter++;
		}
		return false;
	}

	static class LogHistory {
		long lastLogTime = 0;
		int counter=0;
	}



	private class PolicyEngineRefresher extends TimerTask {
		private final RangerBasePlugin plugin;

		PolicyEngineRefresher(RangerBasePlugin plugin) {
			this.plugin = plugin;
		}

		@Override
		public void run() {
			RangerPolicyEngine policyEngine = plugin.policyEngine;
			if (policyEngine != null) {
				policyEngine.reorderPolicyEvaluators();
			}
		}
	}

}
