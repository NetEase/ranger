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

package org.apache.ranger.plugin.store;

import java.util.List;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;

public interface ServiceStore {
	void init() throws Exception;

	RangerServiceDef createServiceDef(RangerServiceDef serviceDef) throws Exception;

	RangerServiceDef updateServiceDef(RangerServiceDef serviceDef) throws Exception;

	void deleteServiceDef(Long id) throws Exception;

	RangerServiceDef getServiceDef(Long id) throws Exception;

	RangerServiceDef getServiceDefByName(String name) throws Exception;

	List<RangerServiceDef> getServiceDefs(SearchFilter filter) throws Exception;


	RangerService createService(RangerService service) throws Exception;

	RangerService updateService(RangerService service) throws Exception;

	void deleteService(Long id) throws Exception;

	RangerService getService(Long id) throws Exception;

	RangerService getServiceByName(String name) throws Exception;

	List<RangerService> getServices(SearchFilter filter) throws Exception;


	RangerPolicy createPolicy(RangerPolicy policy) throws Exception;

	RangerPolicy updatePolicy(RangerPolicy policy) throws Exception;

	void deletePolicy(Long id) throws Exception;

	RangerPolicy getPolicy(Long id) throws Exception;

	List<RangerPolicy> getPolicies(SearchFilter filter) throws Exception;

	List<RangerPolicy> getPoliciesByResourceSignature(String serviceName, String policySignature, Boolean isPolicyEnabled) throws Exception;

	List<RangerPolicy> getServicePolicies(Long serviceId, SearchFilter filter) throws Exception;

	List<RangerPolicy> getServicePolicies(String serviceName, SearchFilter filter) throws Exception;

	ServicePolicies getServicePoliciesIfUpdated(String serviceName, Long lastKnownVersion) throws Exception;


	Long getServicePolicyVersion(String serviceName);

	ServicePolicies getServicePolicies(String serviceName) throws Exception;

	void setPopulateExistingBaseFields(Boolean populateExistingBaseFields);

	Boolean getPopulateExistingBaseFields();
}
