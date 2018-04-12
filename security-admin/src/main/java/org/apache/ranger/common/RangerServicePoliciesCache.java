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

package org.apache.ranger.common;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class RangerServicePoliciesCache {
	private static final Log LOG = LogFactory.getLog(RangerServicePoliciesCache.class);

	private static final int MAX_WAIT_TIME_FOR_UPDATE = 10;

	private static volatile RangerServicePoliciesCache sInstance = null;
	private final boolean useServicePoliciesCache;
	private final int waitTimeInSeconds;

	private final Map<String, ServicePoliciesWrapper> servicePoliciesMap = new HashMap<String, ServicePoliciesWrapper>();

	public static RangerServicePoliciesCache getInstance() {
		if (sInstance == null) {
			synchronized (RangerServicePoliciesCache.class) {
				if (sInstance == null) {
					sInstance = new RangerServicePoliciesCache();
				}
			}
		}
		return sInstance;
	}

	private RangerServicePoliciesCache() {
		useServicePoliciesCache = RangerConfiguration.getInstance().getBoolean("ranger.admin.policy.download.usecache", true);
		waitTimeInSeconds = RangerConfiguration.getInstance().getInt("ranger.admin.policy.download.cache.max.waittime.for.update", MAX_WAIT_TIME_FOR_UPDATE);
	}

	public void dump() {

		if (useServicePoliciesCache) {
			Set<String> serviceNames = null;

			synchronized (this) {
				serviceNames = servicePoliciesMap.keySet();
			}

			if (CollectionUtils.isNotEmpty(serviceNames)) {
				ServicePoliciesWrapper cachedServicePoliciesWrapper = null;

				for (String serviceName : serviceNames) {
					cachedServicePoliciesWrapper = servicePoliciesMap.get(serviceName);
					if (LOG.isDebugEnabled()) {
						LOG.debug("serviceName:" + serviceName + ", Cached-MetaData:" + cachedServicePoliciesWrapper);
					}
				}
			}
		}
	}

	public ServicePolicies getServicePolicies(String serviceName) {

		ServicePolicies ret = null;

		if (useServicePoliciesCache && StringUtils.isNotBlank(serviceName)) {
			ServicePoliciesWrapper cachedServicePoliciesWrapper = null;
			synchronized (this) {
				cachedServicePoliciesWrapper = servicePoliciesMap.get(serviceName);
			}
			if (cachedServicePoliciesWrapper != null) {
				ret = cachedServicePoliciesWrapper.getServicePolicies();
			}
		}

		return ret;
	}

	public ServicePolicies getServicePolicies(String serviceName, ServiceStore serviceStore) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerServicePoliciesCache.getServicePolicies(" + serviceName + ")");
		}

		ServicePolicies ret = null;

		if (StringUtils.isNotBlank(serviceName)) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("useServicePoliciesCache=" + useServicePoliciesCache);
			}

			ServicePolicies servicePolicies = null;

			if (!useServicePoliciesCache) {
				if (serviceStore != null) {
					try {
						servicePolicies = serviceStore.getServicePolicies(serviceName);
					} catch (Exception exception) {
						LOG.error("getServicePolicies(" + serviceName + "): failed to get latest policies from service-store", exception);
					}
				} else {
					LOG.error("getServicePolicies(" + serviceName + "): failed to get latest policies as service-store is null!");
				}
			} else {
				ServicePoliciesWrapper servicePoliciesWrapper = null;

				synchronized (this) {
					servicePoliciesWrapper = servicePoliciesMap.get(serviceName);

					if (servicePoliciesWrapper == null) {
						servicePoliciesWrapper = new ServicePoliciesWrapper();
						servicePoliciesMap.put(serviceName, servicePoliciesWrapper);
					}
				}

				if (serviceStore != null) {
					boolean refreshed = servicePoliciesWrapper.getLatestOrCached(serviceName, serviceStore);

					if(LOG.isDebugEnabled()) {
						LOG.debug("getLatestOrCached returned " + refreshed);
					}
				} else {
					LOG.error("getServicePolicies(" + serviceName + "): failed to get latest policies as service-store is null!");
				}

				servicePolicies = servicePoliciesWrapper.getServicePolicies();
			}

			ret = servicePolicies;

		} else {
			LOG.error("getServicePolicies() failed to get policies as serviceName is null or blank!");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerServicePoliciesCache.getServicePolicies(" + serviceName + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		return ret;
	}

	private class ServicePoliciesWrapper {
		ServicePolicies servicePolicies;
		Date updateTime = null;
		long longestDbLoadTimeInMs = -1;

		ReentrantLock lock = new ReentrantLock();

		ServicePoliciesWrapper() {
			servicePolicies = null;
		}

		ServicePolicies getServicePolicies() {
			return servicePolicies;
		}

		Date getUpdateTime() {
			return updateTime;
		}

		long getLongestDbLoadTimeInMs() {
			return longestDbLoadTimeInMs;
		}

		boolean getLatestOrCached(String serviceName, ServiceStore serviceStore) throws Exception {
			boolean ret = false;

			try {
				ret = lock.tryLock(waitTimeInSeconds, TimeUnit.SECONDS);
				if (ret) {
					LOG.info(Thread.currentThread().getName() + " obtain lock here ...");
					getLatest(serviceName, serviceStore);
				}
			} catch (InterruptedException exception) {
				LOG.error("getLatestOrCached:lock got interrupted..", exception);
			} finally {
				if (ret) {
					//LOG.info(Thread.currentThread().getName() + " release lock here ...");
					lock.unlock();
				}
			}

			return ret;
		}

		void getLatest(String serviceName, ServiceStore serviceStore) throws Exception {

			if (LOG.isDebugEnabled()) {
				LOG.debug("==> ServicePoliciesWrapper.getLatest(" + serviceName + ")");
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Found ServicePolicies in-cache : " + (servicePolicies != null));
			}

			Long servicePolicyVersionInDb = serviceStore.getServicePolicyVersion(serviceName);


			if (servicePolicies == null || servicePolicyVersionInDb == null || !servicePolicyVersionInDb.equals(servicePolicies.getPolicyVersion())) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("loading servicePolicies from db ... cachedServicePoliciesVersion=" + (servicePolicies != null ? servicePolicies.getPolicyVersion() : null) + ", servicePolicyVersionInDb=" + servicePolicyVersionInDb);
				}

				long startTimeMs = System.currentTimeMillis();

				ServicePolicies servicePoliciesFromDb = serviceStore.getServicePolicies(serviceName);

				long dbLoadTime = System.currentTimeMillis() - startTimeMs;

				if (dbLoadTime > longestDbLoadTimeInMs) {
					longestDbLoadTimeInMs = dbLoadTime;
				}
				updateTime = new Date();

				if (servicePoliciesFromDb != null) {
					if (servicePoliciesFromDb.getPolicyVersion() == null) {
						servicePoliciesFromDb.setPolicyVersion(0L);
					}
					servicePolicies = servicePoliciesFromDb;
					// do not use this function any more, return description etc.
					// pruneUnusedAttributes();
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("<== ServicePoliciesWrapper.getLatest(" + serviceName + ")");
			}
		}

		private void pruneUnusedAttributes() {
			if (servicePolicies != null) {
				pruneUnusedPolicyAttributes(servicePolicies.getPolicies());
			}
		}

		private void pruneUnusedPolicyAttributes(List<RangerPolicy> policies) {

			// Null out attributes not required by plug-ins
			if (CollectionUtils.isNotEmpty(policies)) {
				for (RangerPolicy policy : policies) {
					policy.setCreatedBy(null);
					policy.setCreateTime(null);
					policy.setUpdatedBy(null);
					policy.setUpdateTime(null);
					policy.setGuid(null);
					// policy.setName(null); /* this is used by GUI in policy list page */
					policy.setDescription(null);
					policy.setResourceSignature(null);
				}
			}
		}

		StringBuilder toString(StringBuilder sb) {
			sb.append("RangerServicePoliciesWrapper={");

			sb.append("updateTime=").append(updateTime)
					.append(", longestDbLoadTimeInMs=").append(longestDbLoadTimeInMs)
					.append(", Service-Version:").append(servicePolicies != null ? servicePolicies.getPolicyVersion() : "null")
					.append(", Number-Of-Policies:").append(servicePolicies != null ? servicePolicies.getPolicies().size() : 0);

			sb.append("} ");

			return sb;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		void updatePolicy (String action, RangerPolicy policy, ServiceStore serviceStore) {

			Long servicePolicyVersionInDb = serviceStore.getServicePolicyVersion(policy.getService());

			// update policy in cache
			if (servicePolicies != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("loading servicePolicies from db ... cachedServicePoliciesVersion=" + (servicePolicies != null ? servicePolicies.getPolicyVersion() : null) + ", servicePolicyVersionInDb=" + servicePolicyVersionInDb);
				}

				if (action.equals("create")) {
					servicePolicies.getPolicies().add(policy);
				} else {

					boolean shouldAddPolicy = false;
					Iterator<RangerPolicy> iter = servicePolicies.getPolicies().iterator();

					while (iter.hasNext()) {
						RangerPolicy oldPolicy = iter.next();
						if (oldPolicy.getId().equals(policy.getId())) {
							if (action.equals("update")) {
								iter.remove();
								shouldAddPolicy = true;
							} else if (action.equals("delete")) {
								iter.remove();
							}

							break;
						}
					}

					if (shouldAddPolicy) {
						servicePolicies.getPolicies().add(policy);
					}
				}

				servicePolicies.setPolicyVersion(servicePolicyVersionInDb);
			}
		}
	}

	public void updatePolicyInCache (String action, RangerPolicy policy, ServiceStore serviceStore) {

		String serviceName = policy.getService();

		ServicePoliciesWrapper servicePoliciesWrapper = null;

		synchronized (this) {
			servicePoliciesWrapper = servicePoliciesMap.get(serviceName);

			if (servicePoliciesWrapper == null) {
				servicePoliciesWrapper = new ServicePoliciesWrapper();
				servicePoliciesMap.put(serviceName, servicePoliciesWrapper);
			}

			servicePoliciesWrapper.updatePolicy(action, policy, serviceStore);
		}
	}
}

