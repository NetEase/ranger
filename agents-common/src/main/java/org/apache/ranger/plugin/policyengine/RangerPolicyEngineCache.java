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

package org.apache.ranger.plugin.policyengine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RangerPolicyEngineCache {
	private static final Log LOG = LogFactory.getLog(RangerPolicyEngineCache.class);

	private static final RangerPolicyEngineCache sInstance = new RangerPolicyEngineCache();

	private final Map<String, RangerPolicyEngine> policyEngineCache = Collections.synchronizedMap(new HashMap<String, RangerPolicyEngine>());

	private RangerPolicyEngineOptions options = null;

	public static RangerPolicyEngineCache getInstance() {
		return sInstance;
	}

	public RangerPolicyEngine getPolicyEngine(String serviceName, ServiceStore svcStore) {
		RangerPolicyEngine ret = null;

		if(serviceName != null) {
			ret = policyEngineCache.get(serviceName);

			long policyVersion = ret != null ? ret.getPolicyVersion() : -1;

			if(svcStore != null) {
				try {
					ServicePolicies policies = svcStore.getServicePoliciesIfUpdated(serviceName, policyVersion);

					if(policies != null) {
						if(ret == null) {
							ret = addPolicyEngine(policies);
						} else if(policies.getPolicyVersion() != null && !policies.getPolicyVersion().equals(policyVersion)) {
							ret = addPolicyEngine(policies);
						}
					}
				} catch(Exception excp) {
					LOG.error("getPolicyEngine(" + serviceName + "): failed to get latest policies from service-store", excp);
				}
			}
		}

		return ret;
	}

	public RangerPolicyEngineOptions getPolicyEngineOptions() {
		return options;
	}

	public void setPolicyEngineOptions(RangerPolicyEngineOptions options) {
		this.options = options;
	}

	private RangerPolicyEngine addPolicyEngine(ServicePolicies policies) {
		RangerPolicyEngine ret = new RangerPolicyEngineImpl(policies, options);

		policyEngineCache.put(policies.getServiceName(), ret);

		return ret;
	}
}
