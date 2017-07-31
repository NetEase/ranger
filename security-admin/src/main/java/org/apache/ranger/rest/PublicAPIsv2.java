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

package org.apache.ranger.rest;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.biz.ServiceDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerServicePoliciesCache;
import org.apache.ranger.common.annotation.RangerAnnotationJSMgrName;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.service.RangerPolicyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestBody;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import jline.internal.Log;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("public/v2")
@Component
@Scope("request")
@RangerAnnotationJSMgrName("PublicMgr")
@Transactional(propagation = Propagation.REQUIRES_NEW)
public class PublicAPIsv2 {
	static Logger logger = Logger.getLogger(PublicAPIsv2.class);

	@Autowired
	ServiceREST serviceREST;

	@Autowired
	RESTErrorUtil restErrorUtil;
	
	@Autowired
	ServiceDBStore svcStore;
	
	@Autowired
	RangerPolicyService policyService;

	/*
	* ServiceDef Manipulation APIs
	 */

	@GET
	@Path("/api/servicedef/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef getServiceDef(@PathParam("id") Long id) {
		return serviceREST.getServiceDef(id);
	}

	@GET
	@Path("/api/servicedef/name/{name}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef getServiceDefByName(@PathParam("name") String name) {
		return serviceREST.getServiceDefByName(name);
	}

	@GET
	@Path("/api/servicedef/")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public List<RangerServiceDef> searchServiceDefs(@Context HttpServletRequest request) {
		return serviceREST.getServiceDefs(request).getServiceDefs();
	}

	@POST
	@Path("/api/servicedef/")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) {
		return serviceREST.createServiceDef(serviceDef);
	}

	@PUT
	@Path("/api/servicedef/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef, @PathParam("id") Long id) {
		// if serviceDef.id is specified, it should be same as param 'id'
		if(serviceDef.getId() == null) {
			serviceDef.setId(id);
		} else if(!serviceDef.getId().equals(id)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "serviceDef id mismatch", true);
		}

		return serviceREST.updateServiceDef(serviceDef);
	}


	@PUT
	@Path("/api/servicedef/name/{name}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef updateServiceDefByName(RangerServiceDef serviceDef,
	                                     @PathParam("name") String name) {
		// serviceDef.name is immutable
		// if serviceDef.name is specified, it should be same as the param 'name'
		if(serviceDef.getName() == null) {
			serviceDef.setName(name);
		} else if(!serviceDef.getName().equals(name)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "serviceDef name mismatch", true);
		}

		// ignore serviceDef.id - if specified. Retrieve using the given name and use id from the retrieved object
		RangerServiceDef existingServiceDef = getServiceDefByName(name);
		serviceDef.setId(existingServiceDef.getId());
		if(StringUtils.isEmpty(serviceDef.getGuid())) {
			serviceDef.setGuid(existingServiceDef.getGuid());
		}

		return serviceREST.updateServiceDef(serviceDef);
	}

	/*
	* Should add this back when guid is used for search and delete operations as well
	@PUT
	@Path("/api/servicedef/guid/{guid}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerServiceDef updateServiceDefByGuid(RangerServiceDef serviceDef,
	                                               @PathParam("guid") String guid) {
		// ignore serviceDef.id - if specified. Retrieve using the given guid and use id from the retrieved object
		RangerServiceDef existingServiceDef = getServiceDefByGuid(guid);
		serviceDef.setId(existingServiceDef.getId());
		if(StringUtils.isEmpty(serviceDef.getGuid())) {
			serviceDef.setGuid(existingServiceDef.getGuid());
		}

		return serviceREST.updateServiceDef(serviceDef);
	}
	*/


	@DELETE
	@Path("/api/servicedef/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteServiceDef(@PathParam("id") Long id, @Context HttpServletRequest request) {
		serviceREST.deleteServiceDef(id, request);
	}

	@DELETE
	@Path("/api/servicedef/name/{name}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteServiceDefByName(@PathParam("name") String name, @Context HttpServletRequest request) {
		RangerServiceDef serviceDef = serviceREST.getServiceDefByName(name);
		serviceREST.deleteServiceDef(serviceDef.getId(), request);
	}

	/*
	* Service Manipulation APIs
	 */

	@GET
	@Path("/api/service/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerService getService(@PathParam("id") Long id) {
		return serviceREST.getService(id);
	}

	@GET
	@Path("/api/service/name/{name}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerService getServiceByName(@PathParam("name") String name) {
		return serviceREST.getServiceByName(name);
	}

	@GET
	@Path("/api/service/")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public List<RangerService> searchServices(@Context HttpServletRequest request) {
		return serviceREST.getServices(request).getServices();
	}

	@POST
	@Path("/api/service/")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerService createService(RangerService service) {
		return serviceREST.createService(service);
	}

	@PUT
	@Path("/api/service/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerService updateService(RangerService service, @PathParam("id") Long id) {
		// if service.id is specified, it should be same as the param 'id'
		if(service.getId() == null) {
			service.setId(id);
		} else if(!service.getId().equals(id)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "service id mismatch", true);
		}

		return serviceREST.updateService(service);
	}


	@PUT
	@Path("/api/service/name/{name}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerService updateServiceByName(RangerService service,
	                                               @PathParam("name") String name) {
		// ignore service.id - if specified. Retrieve using the given name and use id from the retrieved object
		RangerService existingService = getServiceByName(name);
		service.setId(existingService.getId());
		if(StringUtils.isEmpty(service.getGuid())) {
			service.setGuid(existingService.getGuid());
		}
		if (StringUtils.isEmpty(service.getName())) {
			service.setName(existingService.getName());
		}

		return serviceREST.updateService(service);
	}

	/*
	 * Should add this back when guid is used for search and delete operations as well
	@PUT
	@Path("/api/service/guid/{guid}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	@Produces({ "application/json", "application/xml" })
	public RangerService updateServiceByGuid(RangerService service,
	                                               @PathParam("guid") String guid) {
		// ignore service.id - if specified. Retrieve using the given guid and use id from the retrieved object
		RangerService existingService = getServiceByGuid(guid);
		service.setId(existingService.getId());
		if(StringUtils.isEmpty(service.getGuid())) {
			service.setGuid(existingService.getGuid());
		}

		return serviceREST.updateService(service);
	}
	*/

	@DELETE
	@Path("/api/service/{id}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteService(@PathParam("id") Long id) {
		serviceREST.deleteService(id);
	}

	@DELETE
	@Path("/api/service/name/{name}")
	@PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
	public void deleteServiceByName(@PathParam("name") String name) {
		RangerService service = serviceREST.getServiceByName(name);
		serviceREST.deleteService(service.getId());
	}

	/*
	* Policy Manipulation APIs
	 */

	@GET
	@Path("/api/policy/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy getPolicy(@PathParam("id") Long id) {
		return serviceREST.getPolicy(id);
	}

	@GET
	@Path("/api/service/{servicename}/policy/{policyname}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy getPolicyByName(@PathParam("servicename") String serviceName,
	                                    @PathParam("policyname") String policyName,
	                                    @Context HttpServletRequest request) {
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIsv2.getPolicyByName(" + serviceName + "," + policyName + ")");
		}

		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.SERVICE_NAME, serviceName);
		filter.setParam(SearchFilter.POLICY_NAME, policyName);
		List<RangerPolicy> policies = serviceREST.getPolicies(filter);

		if (policies.size() != 1) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, "Not found", true);
		}
		RangerPolicy policy = policies.get(0);

		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIsv2.getPolicyByName(" + serviceName + "," + policyName + ")" + policy);
		}
		return policy;
	}

	@GET
	@Path("/api/service/{servicename}/policy/")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> searchPolicies(@PathParam("servicename") String serviceName,
	                                         @Context HttpServletRequest request) {
		return serviceREST.getServicePoliciesByName(serviceName, request).getPolicies();
	}

	@POST
	@Path("/api/policy/")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy createPolicy(RangerPolicy policy) {
		return serviceREST.createPolicy(policy);
	}

	@PUT
	@Path("/api/policy/{id}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy updatePolicy(RangerPolicy policy, @PathParam("id") Long id) {
		// if policy.id is specified, it should be same as the param 'id'
		if(policy.getId() == null) {
			policy.setId(id);
		} else if(!policy.getId().equals(id)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "policyID mismatch", true);
		}

		return serviceREST.updatePolicy(policy);
	}


	@PUT
	@Path("/api/service/{servicename}/policy/{policyname}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy updatePolicyByName(RangerPolicy policy,
	                                               @PathParam("servicename") String serviceName,
	                                               @PathParam("policyname") String policyName,
	                                               @Context HttpServletRequest request) {
		if (policy.getService() == null || !policy.getService().equals(serviceName)) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "service name mismatch", true);
		}
		RangerPolicy oldPolicy = getPolicyByName(serviceName, policyName, request);

		// ignore policy.id - if specified. Retrieve using the given serviceName+policyName and use id from the retrieved object
		policy.setId(oldPolicy.getId());
		if(StringUtils.isEmpty(policy.getGuid())) {
			policy.setGuid(oldPolicy.getGuid());
		}
		if(StringUtils.isEmpty(policy.getName())) {
			policy.setName(oldPolicy.getName());
		}

		return serviceREST.updatePolicy(policy);
	}


	/* Should add this back when guid is used for search and delete operations as well
	@PUT
	@Path("/api/policy/guid/{guid}")
	@Produces({ "application/json", "application/xml" })
	public RangerPolicy updatePolicyByGuid(RangerPolicy policy,
	                                               @PathParam("guid") String guid) {
		// ignore policy.guid - if specified. Retrieve using the given guid and use id from the retrieved object
		RangerPolicy existingPolicy = getPolicyByGuid(name);
		policy.setId(existingPolicy.getId());
		if(StringUtils.isEmpty(policy.getGuid())) {
			policy.setGuid(existingPolicy.getGuid());
		}

		return serviceREST.updatePolicy(policy);
	}
	*/


	@DELETE
	@Path("/api/policy/{id}")
	public void deletePolicy(@PathParam("id") Long id) {
		serviceREST.deletePolicy(id);
	}

	@DELETE
	@Path("/api/policy")
	public void deletePolicyByName(@QueryParam("servicename") String serviceName,
	                               @QueryParam("policyname") String policyName,
	                               @Context HttpServletRequest request) {
		if(logger.isDebugEnabled()) {
			logger.debug("==> PublicAPIsv2.deletePolicyByName(" + serviceName + "," + policyName + ")");
		}

		if (serviceName == null || policyName == null) {
			throw restErrorUtil.createRESTException(HttpServletResponse.SC_BAD_REQUEST , "Invalid service name or policy name", true);
		}
		RangerPolicy policy = getPolicyByName(serviceName, policyName, request);
		serviceREST.deletePolicy(policy.getId());
		if(logger.isDebugEnabled()) {
			logger.debug("<== PublicAPIsv2.deletePolicyByName(" + serviceName + "," + policyName + ")");
		}
	}
	
	@POST
	@Path("/api/policies/modify")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> modifyPolicies(@RequestBody Map<String, List<RangerPolicy>> policies) {
		
		List<RangerPolicy> createPolicies = policies.get("createPolicies");
		List<RangerPolicy> updatePolicies = policies.get("updatePolicies");
		List<RangerPolicy> deletePolicies = policies.get("deletePolicies");
		
		List<RangerPolicy> retPolicies = new ArrayList();
		
		if (createPolicies != null) {
			for (RangerPolicy policy : createPolicies) {
				retPolicies.add(serviceREST.createPolicy(policy));
			}
		}
		
		if (updatePolicies != null) {
			for (RangerPolicy policy : updatePolicies) {
				retPolicies.add(serviceREST.updatePolicy(policy));
			}
		}
		
		if (deletePolicies != null) {
			for (RangerPolicy policy : deletePolicies) {
				serviceREST.deletePolicy(policy.getId());
			}
		}
		
		return retPolicies;
	}
	
	/**
	 * 此接口每个policy会作为多个事务更新policy，将失败的policy返回给猛犸
	 * @param params
	 * @return
	 * @throws Exception
	 */
	@POST
	@Path("/api/hivepolicies/modifyBatch")
	@Produces({ "application/json", "application/xml" })
	public List<RangerPolicy> modifyHivePoliciesBatch(@RequestBody JSONObject params) throws Exception {
		
		String serviceName = params.getString("serviceName");
		JSONArray objs = params.getJSONArray("policies");
		logger.info("### batch modify size is " + objs.size());
		
		// 一次性查询出service对应的所有policy
		long startSearchTime = System.currentTimeMillis();
		List<RangerPolicy> oldPolicies = RangerServicePoliciesCache.getInstance().
				getServicePolicies(serviceName, svcStore).getPolicies();
		long endSearchTime = System.currentTimeMillis();
		if (oldPolicies == null) {
			logger.error("### error happend when get service policies");
			return null;
		}
		logger.info("search policy cost time = " + (endSearchTime - startSearchTime) +
				" , policy num is " + oldPolicies.size());
		
		long startTime = System.currentTimeMillis();
		List<RangerPolicy> failedPolicies = new ArrayList();
		for (int index = 0; index < objs.size(); ++index) {
			RangerPolicy policy = objs.getObject(index, RangerPolicy.class);
			logger.info("### modify policy values is = " + policy.toString());
			
			Map<String, RangerPolicyResource> resources = policy.getResources();
			if (resources.get("database").getValues().size() > 1 ||
				resources.get("table").getValues().size() > 1 ||
				resources.get("column").getValues().size() > 1) {
				logger.error("### policy format not support, resources = " + resources.toString());
				continue;
			}
			
			String database = resources.get("database").getValues().get(0);
			String table = resources.get("table").getValues().get(0);
			String column = resources.get("column").getValues().get(0);
			
			SearchFilter filter = new SearchFilter();
			filter.setParam("resource:database", database);
			filter.setParam("resource:table", table);
			filter.setParam("resource:column", column);
			filter.setParam(SearchFilter.SERVICE_NAME, serviceName);
			
			List<RangerPolicyItem> newPolicyItems = policy.getPolicyItems();
				
			// 资源对应的policy是否已经存在，不存在则需要createPolicy
			boolean resourceUsed = false;
			
			// <db, *, *> 会匹配出多条policy， 需要根据resource严格匹配
			for (RangerPolicy oldPolicy : oldPolicies) {
				
				Map<String, RangerPolicyResource> innerResources = oldPolicy.getResources();
				if (innerResources.get("database") == null ||
					innerResources.get("table") == null ||
					innerResources.get("column") == null ||
					innerResources.get("database").getValues().size() > 1 ||
					innerResources.get("table").getValues().size() > 1 ||
					innerResources.get("column").getValues().size() > 1) {
					logger.error("### policy format not support, resources = " + innerResources.toString());
						continue;
				}
				
				String innerDatabase = innerResources.get("database").getValues().get(0);
				String innerTable = innerResources.get("table").getValues().get(0);
				String innerColumn = innerResources.get("column").getValues().get(0);
				if (!innerDatabase.equals(database) || !innerTable.equals(table) || !innerColumn.equals(column)) {
					continue;
				}
				
				// 资源对应的policy已经存在
				resourceUsed = true;
				
				// policy with resource <db,table,column> has already exist, merge
				Long policyId = oldPolicy.getId();
				logger.info("### resource existed in policy " + policyId);
				
				List<RangerPolicyItem> policyItems = oldPolicy.getPolicyItems();
				for (RangerPolicyItem policyItem : policyItems) {
					List<String> groups = policyItem.getGroups();
					if (groups == null || groups.isEmpty()) {
						Log.info("### no group exists in current policyItem " + policyItem.toString());
						continue;
					}
					
					// 老的policy中组是否存在新的policy：若存在，以新的policy中为准；不存在，叠加老的policy
					boolean groupExist = false;
					for (RangerPolicyItem newPolicyItem : newPolicyItems) {
						if (newPolicyItem.getGroups().contains(groups.get(0))) {
							groupExist = true;
							break;
						}
					}
					
					// 老policy的组不存在于新的policy
					if (!groupExist) {
						newPolicyItems.add(policyItem);
					} else {
						// 老的组存在于新的policy，用新的policy覆盖
						logger.info("### group permission replace by new policy");
					}
				}
				
				policy.setId(policyId);
				try {
					long updateStartTime = System.currentTimeMillis();
					serviceREST.updatePolicy(policy);
					logger.info("### update policy cost time = " + 
							(System.currentTimeMillis() - updateStartTime));
				} catch (Exception e) {
					logger.error("### error happened when update policy " + policy.toString());
					logger.error(e.getMessage(), e);
					failedPolicies.add(policy);
				}
			
				// 资源对应的policy只有一条，找到对应的之后，后面的policy肯定不会完全匹配，没必要再遍历
				break;
			}
			
			// 资源对应的policy不存在，create
			if (!resourceUsed) {
				try {
					long createStartTime = System.currentTimeMillis();
					serviceREST.createPolicy(policy);
					logger.info("### create policy cost time = " + 
							(System.currentTimeMillis() - createStartTime));
				} catch (Exception e) {
					logger.error("### error happened when create policy " + policy.toString());
					logger.error(e.getMessage(), e);
					failedPolicies.add(policy);
				}
			}			
		}	
		
		long endTime = System.currentTimeMillis();
		logger.info("total cost time = " + (endTime - startTime));
		
		return failedPolicies;
	}
}
