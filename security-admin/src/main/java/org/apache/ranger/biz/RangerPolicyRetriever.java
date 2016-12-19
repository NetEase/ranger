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

package org.apache.ranger.biz;

import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPolicyConditionDef;
import org.apache.ranger.entity.XXPolicyItem;
import org.apache.ranger.entity.XXPolicyItemAccess;
import org.apache.ranger.entity.XXPolicyItemCondition;
import org.apache.ranger.entity.XXPolicyItemGroupPerm;
import org.apache.ranger.entity.XXPolicyItemUserPerm;
import org.apache.ranger.entity.XXPolicyResource;
import org.apache.ranger.entity.XXPolicyResourceMap;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyevaluator.RangerPolicyItemEvaluator;
import org.apache.ranger.plugin.util.RangerPerfTracer;


public class RangerPolicyRetriever {
	static final Log LOG      = LogFactory.getLog(RangerPolicyRetriever.class);
	static final Log PERF_LOG = RangerPerfTracer.getPerfLogger("db.RangerPolicyRetriever");

	final RangerDaoManager daoMgr;
	final LookupCache      lookupCache;

	public RangerPolicyRetriever(RangerDaoManager daoMgr) {
		this.daoMgr      = daoMgr;
		this.lookupCache = new LookupCache();
	}

	public List<RangerPolicy> getServicePolicies(Long serviceId) {
		List<RangerPolicy> ret = null;

		if(serviceId != null) {
			XXService xService = getXXService(serviceId);

			if(xService != null) {
				ret = getServicePolicies(xService);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyRetriever.getServicePolicies(serviceId=" + serviceId + "): service not found");
				}
			}
		}

		return ret;
	}

	public List<RangerPolicy> getServicePolicies(String serviceName) {
		List<RangerPolicy> ret = null;

		if(serviceName != null) {
			XXService xService = getXXService(serviceName);

			if(xService != null) {
				ret = getServicePolicies(xService);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyRetriever.getServicePolicies(serviceName=" + serviceName + "): service not found");
				}
			}
		}

		return ret;
	}

	public List<RangerPolicy> getServicePolicies(XXService xService) {
		String serviceName = xService == null ? null : xService.getName();
		Long   serviceId   = xService == null ? null : xService.getId();

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyRetriever.getServicePolicies(serviceName=" + serviceName + ", serviceId=" + serviceId + ")");
		}

		List<RangerPolicy> ret  = null;
		RangerPerfTracer   perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerPolicyRetriever.getServicePolicies(serviceName=" + serviceName + ", serviceId=" + serviceId + ")");
		}

		if(xService != null) {
			RetrieverContext ctx = new RetrieverContext(xService);

			ret = ctx.getAllPolicies();
		} else {
			if(LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyRetriever.getServicePolicies(xService=" + xService + "): invalid parameter");
			}
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyRetriever.getServicePolicies(serviceName=" + serviceName + ", serviceId=" + serviceId + "): policyCount=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	public RangerPolicy getPolicy(Long policyId) {
		RangerPolicy ret = null;

		if(policyId != null) {
			XXPolicy xPolicy = getXXPolicy(policyId);

			if(xPolicy != null) {
				ret = getPolicy(xPolicy);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyRetriever.getPolicy(policyId=" + policyId + "): policy not found");
				}
			}

		}

		return ret;
	}

	public RangerPolicy getPolicy(XXPolicy xPolicy) {
		RangerPolicy ret = null;

		if(xPolicy != null) {
			XXService xService = getXXService(xPolicy.getService());

			if(xService != null) {
				ret = getPolicy(xPolicy, xService);
			} else {
				if(LOG.isDebugEnabled()) {
					LOG.debug("RangerPolicyRetriever.getPolicy(policyId=" + xPolicy.getId() + "): service not found (serviceId=" + xPolicy.getService() + ")");
				}
			}
		}

		return ret;
	}

	public RangerPolicy getPolicy(XXPolicy xPolicy, XXService xService) {
		Long policyId = xPolicy == null ? null : xPolicy.getId();

		if(LOG.isDebugEnabled()) {
			LOG.debug("==> RangerPolicyRetriever.getPolicy(" + policyId + ")");
		}

		RangerPolicy     ret  = null;
		RangerPerfTracer perf = null;

		if(RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
			perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "RangerPolicyRetriever.getPolicy(policyId=" + policyId + ")");
		}

		if(xPolicy != null && xService != null) {
			RetrieverContext ctx = new RetrieverContext(xPolicy, xService);

			ret = ctx.getNextPolicy();
		} else {
			if(LOG.isDebugEnabled()) {
				LOG.debug("RangerPolicyRetriever.getPolicy(xPolicy=" + xPolicy + ", xService=" + xService + "): invalid parameter(s)");
			}
		}

		RangerPerfTracer.log(perf);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== RangerPolicyRetriever.getPolicy(" + policyId + "): " + ret);
		}

		return ret;
	}

	private XXService getXXService(Long serviceId) {
		XXService ret = null;

		if(serviceId != null) {
			ret = daoMgr.getXXService().getById(serviceId);
		}

		return ret;
	}

	private XXService getXXService(String serviceName) {
		XXService ret = null;

		if(serviceName != null) {
			ret = daoMgr.getXXService().findByName(serviceName);
		}

		return ret;
	}

	private XXPolicy getXXPolicy(Long policyId) {
		XXPolicy ret = null;

		if(policyId != null) {
			ret = daoMgr.getXXPolicy().getById(policyId);
		}

		return ret;
	}

	class LookupCache {
		final Map<Long, String> userNames       = new HashMap<Long, String>();
		final Map<Long, String> userScreenNames = new HashMap<Long, String>();
		final Map<Long, String> groupNames      = new HashMap<Long, String>();
		final Map<Long, String> accessTypes     = new HashMap<Long, String>();
		final Map<Long, String> conditions      = new HashMap<Long, String>();
		final Map<Long, String> resourceDefs    = new HashMap<Long, String>();

		String getUserName(Long userId) {
			String ret = null;

			if(userId != null) {
				ret = userNames.get(userId);

				if(ret == null) {
					XXUser user = daoMgr.getXXUser().getById(userId);

					if(user != null) {
						ret = user.getName(); // Name is `loginId`

						userNames.put(userId,  ret);
					}
				}
			}

			return ret;
		}

		String getUserScreenName(Long userId) {
			String ret = null;

			if(userId != null) {
				ret = userScreenNames.get(userId);

				if(ret == null) {
					XXPortalUser user = daoMgr.getXXPortalUser().getById(userId);

					if(user != null) {
						ret = user.getPublicScreenName();

						if (StringUtil.isEmpty(ret)) {
							ret = user.getFirstName();

							if(StringUtil.isEmpty(ret)) {
								ret = user.getLoginId();
							} else {
								if(!StringUtil.isEmpty(user.getLastName())) {
									ret += (" " + user.getLastName());
								}
							}
						}

						if(ret != null) {
							userScreenNames.put(userId, ret);
						}
					}
				}
			}

			return ret;
		}

		String getGroupName(Long groupId) {
			String ret = null;

			if(groupId != null) {
				ret = groupNames.get(groupId);

				if(ret == null) {
					XXGroup group = daoMgr.getXXGroup().getById(groupId);

					if(group != null) {
						ret = group.getName();

						groupNames.put(groupId,  ret);
					}
				}
			}

			return ret;
		}

		String getAccessType(Long accessTypeId) {
			String ret = null;

			if(accessTypeId != null) {
				ret = accessTypes.get(accessTypeId);

				if(ret == null) {
					XXAccessTypeDef xAccessType = daoMgr.getXXAccessTypeDef().getById(accessTypeId);

					if(xAccessType != null) {
						ret = xAccessType.getName();

						accessTypes.put(accessTypeId,  ret);
					}
				}
			}

			return ret;
		}

		String getConditionType(Long conditionDefId) {
			String ret = null;

			if(conditionDefId != null) {
				ret = conditions.get(conditionDefId);

				if(ret == null) {
					XXPolicyConditionDef xPolicyConditionDef = daoMgr.getXXPolicyConditionDef().getById(conditionDefId);

					if(xPolicyConditionDef != null) {
						ret = xPolicyConditionDef.getName();

						conditions.put(conditionDefId,  ret);
					}
				}
			}

			return ret;
		}

		String getResourceName(Long resourceDefId) {
			String ret = null;

			if(resourceDefId != null) {
				ret = resourceDefs.get(resourceDefId);

				if(ret == null) {
					XXResourceDef xResourceDef = daoMgr.getXXResourceDef().getById(resourceDefId);

					if(xResourceDef != null) {
						ret = xResourceDef.getName();

						resourceDefs.put(resourceDefId,  ret);
					}
				}
			}

			return ret;
		}
	}

	static List<XXPolicy> asList(XXPolicy policy) {
		List<XXPolicy> ret = new ArrayList<XXPolicy>();

		if(policy != null) {
			ret.add(policy);
		}

		return ret;
	}

	class RetrieverContext {
		final XXService                           service;
		final ListIterator<XXPolicy>              iterPolicy;
		final ListIterator<XXPolicyResource>      iterResources;
		final ListIterator<XXPolicyResourceMap>   iterResourceMaps;
		final ListIterator<XXPolicyItem>          iterPolicyItems;
		final ListIterator<XXPolicyItemUserPerm>  iterUserPerms;
		final ListIterator<XXPolicyItemGroupPerm> iterGroupPerms;
		final ListIterator<XXPolicyItemAccess>    iterAccesses;
		final ListIterator<XXPolicyItemCondition> iterConditions;

		RetrieverContext(XXService xService) {
			Long serviceId = xService == null ? null : xService.getId();

			List<XXPolicy>              xPolicies     = daoMgr.getXXPolicy().findByServiceId(serviceId);
			List<XXPolicyResource>      xResources    = daoMgr.getXXPolicyResource().findByServiceId(serviceId);
			List<XXPolicyResourceMap>   xResourceMaps = daoMgr.getXXPolicyResourceMap().findByServiceId(serviceId);
			List<XXPolicyItem>          xPolicyItems  = daoMgr.getXXPolicyItem().findByServiceId(serviceId);
			List<XXPolicyItemUserPerm>  xUserPerms    = daoMgr.getXXPolicyItemUserPerm().findByServiceId(serviceId);
			List<XXPolicyItemGroupPerm> xGroupPerms   = daoMgr.getXXPolicyItemGroupPerm().findByServiceId(serviceId);
			List<XXPolicyItemAccess>    xAccesses     = daoMgr.getXXPolicyItemAccess().findByServiceId(serviceId);
			List<XXPolicyItemCondition> xConditions   = daoMgr.getXXPolicyItemCondition().findByServiceId(serviceId);

			this.service          = xService;
			this.iterPolicy       = xPolicies.listIterator();
			this.iterResources    = xResources.listIterator();
			this.iterResourceMaps = xResourceMaps.listIterator();
			this.iterPolicyItems  = xPolicyItems.listIterator();
			this.iterUserPerms    = xUserPerms.listIterator();
			this.iterGroupPerms   = xGroupPerms.listIterator();
			this.iterAccesses     = xAccesses.listIterator();
			this.iterConditions   = xConditions.listIterator();
		}

		RetrieverContext(XXPolicy xPolicy) {
			this(xPolicy, getXXService(xPolicy.getService()));
		}

		RetrieverContext(XXPolicy xPolicy, XXService xService) {
			Long policyId = xPolicy == null ? null : xPolicy.getId();

			List<XXPolicy>              xPolicies     = asList(xPolicy);
			List<XXPolicyResource>      xResources    = daoMgr.getXXPolicyResource().findByPolicyId(policyId);
			List<XXPolicyResourceMap>   xResourceMaps = daoMgr.getXXPolicyResourceMap().findByPolicyId(policyId);
			List<XXPolicyItem>          xPolicyItems  = daoMgr.getXXPolicyItem().findByPolicyId(policyId);
			List<XXPolicyItemUserPerm>  xUserPerms    = daoMgr.getXXPolicyItemUserPerm().findByPolicyId(policyId);
			List<XXPolicyItemGroupPerm> xGroupPerms   = daoMgr.getXXPolicyItemGroupPerm().findByPolicyId(policyId);
			List<XXPolicyItemAccess>    xAccesses     = daoMgr.getXXPolicyItemAccess().findByPolicyId(policyId);
			List<XXPolicyItemCondition> xConditions   = daoMgr.getXXPolicyItemCondition().findByPolicyId(policyId);

			this.service          = xService;
			this.iterPolicy       = xPolicies.listIterator();
			this.iterResources    = xResources.listIterator();
			this.iterResourceMaps = xResourceMaps.listIterator();
			this.iterPolicyItems  = xPolicyItems.listIterator();
			this.iterUserPerms    = xUserPerms.listIterator();
			this.iterGroupPerms   = xGroupPerms.listIterator();
			this.iterAccesses     = xAccesses.listIterator();
			this.iterConditions   = xConditions.listIterator();
		}

		RangerPolicy getNextPolicy() {
			RangerPolicy ret = null;

			if(iterPolicy.hasNext()) {
				XXPolicy xPolicy = iterPolicy.next();

				if(xPolicy != null) {
					ret = new RangerPolicy();

					ret.setId(xPolicy.getId());
					ret.setGuid(xPolicy.getGuid());
					ret.setIsEnabled(xPolicy.getIsEnabled());
					ret.setCreatedBy(lookupCache.getUserScreenName(xPolicy.getAddedByUserId()));
					ret.setUpdatedBy(lookupCache.getUserScreenName(xPolicy.getUpdatedByUserId()));
					ret.setCreateTime(xPolicy.getCreateTime());
					ret.setUpdateTime(xPolicy.getUpdateTime());
					ret.setVersion(xPolicy.getVersion());
					ret.setService(service == null ? null : service.getName());
					ret.setName(xPolicy.getName());
					ret.setPolicyType(xPolicy.getPolicyType());
					ret.setDescription(xPolicy.getDescription());
					ret.setResourceSignature(xPolicy.getResourceSignature());
					ret.setIsAuditEnabled(xPolicy.getIsAuditEnabled());

					getResource(ret);
					getPolicyItems(ret);
				}
			}

			return ret;
		}

		List<RangerPolicy> getAllPolicies() {
			List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

			while(iterPolicy.hasNext()) {
				RangerPolicy policy = getNextPolicy();

				if(policy != null) {
					ret.add(policy);
				}
			}

			if(! hasProcessedAll()) {
				LOG.warn("getAllPolicies(): perhaps one or more policies got updated during retrieval. Falling back to secondary method");

				ret = getAllPoliciesBySecondary();
			}

			return ret;
		}

		List<RangerPolicy> getAllPoliciesBySecondary() {
			List<RangerPolicy> ret = null;

			if(service != null) {
				List<XXPolicy> xPolicies = daoMgr.getXXPolicy().findByServiceId(service.getId());

				if(CollectionUtils.isNotEmpty(xPolicies)) {
					ret = new ArrayList<RangerPolicy>(xPolicies.size());

					for(XXPolicy xPolicy : xPolicies) {
						RetrieverContext ctx = new RetrieverContext(xPolicy, service);

						RangerPolicy policy = ctx.getNextPolicy();

						if(policy != null) {
							ret.add(policy);
						}
					}
				}
			}

			return ret;
		}

		private boolean hasProcessedAll() {
			boolean moreToProcess =    iterPolicy.hasNext()
									|| iterResources.hasNext()
									|| iterResourceMaps.hasNext()
									|| iterPolicyItems.hasNext()
									|| iterUserPerms.hasNext()
									|| iterGroupPerms.hasNext()
									|| iterAccesses.hasNext()
									|| iterConditions.hasNext();

			return !moreToProcess;
		}

		private void getResource(RangerPolicy policy) {
			while(iterResources.hasNext()) {
				XXPolicyResource xResource = iterResources.next();

				if(xResource.getPolicyid().equals(policy.getId())) {
					RangerPolicyResource resource = new RangerPolicyResource();

					resource.setIsExcludes(xResource.getIsexcludes());
					resource.setIsRecursive(xResource.getIsrecursive());

					while(iterResourceMaps.hasNext()) {
						XXPolicyResourceMap xResourceMap = iterResourceMaps.next();

						if(xResourceMap.getResourceid().equals(xResource.getId())) {
							resource.getValues().add(xResourceMap.getValue());
						} else {
							if(iterResourceMaps.hasPrevious()) {
								iterResourceMaps.previous();
							}
							break;
						}
					}

					policy.getResources().put(lookupCache.getResourceName(xResource.getResdefid()), resource);
				} else if(xResource.getPolicyid().compareTo(policy.getId()) > 0) {
					if(iterResources.hasPrevious()) {
						iterResources.previous();
					}
					break;
				}
			}
		}

		private void getPolicyItems(RangerPolicy policy) {
			List<XXPortalUser> xxPortalUserList = daoMgr.getXXPortalUser().findAllXPortalUser();

			while(iterPolicyItems.hasNext()) {
				XXPolicyItem xPolicyItem = iterPolicyItems.next();

				if(xPolicyItem.getPolicyid().equals(policy.getId())) {
					RangerPolicyItem policyItem = new RangerPolicyItem();

					policyItem.setDelegateAdmin(xPolicyItem.getDelegateAdmin());

					while(iterUserPerms.hasNext()) {
						XXPolicyItemUserPerm xUserPerm = iterUserPerms.next();

						if(xUserPerm.getPolicyitemid().equals(xPolicyItem.getId())) {
							policyItem.getUsers().add(lookupCache.getUserName(xUserPerm.getUserid()));

							// add user password
							for(XXPortalUser portalUser : xxPortalUserList){
								if(portalUser.getId() == xUserPerm.getUserid()) {
									policyItem.getUserPasswds().add(portalUser.getPassword());
								}
							}
						} else {
							if(iterUserPerms.hasPrevious()) {
								iterUserPerms.previous();
							}
							break;
						}
					}

					while(iterGroupPerms.hasNext()) {
						XXPolicyItemGroupPerm xGroupPerm = iterGroupPerms.next();

						if(xGroupPerm.getPolicyitemid().equals(xPolicyItem.getId())) {
							policyItem.getGroups().add(lookupCache.getGroupName(xGroupPerm.getGroupid()));

							// add contains user name
							List<String> userGroups = daoMgr.getXXGroupUser().findUserNamesByGroupId(xGroupPerm.getGroupid());
							policyItem.getGroupMember().put(lookupCache.getGroupName(xGroupPerm.getGroupid()), userGroups);
						} else {
							if(iterGroupPerms.hasPrevious()) {
								iterGroupPerms.previous();
							}
							break;
						}
					}

					while(iterAccesses.hasNext()) {
						XXPolicyItemAccess xAccess = iterAccesses.next();

						if(xAccess.getPolicyitemid().equals(xPolicyItem.getId())) {
							policyItem.getAccesses().add(new RangerPolicyItemAccess(lookupCache.getAccessType(xAccess.getType()), xAccess.getIsallowed()));
						} else {
							if(iterAccesses.hasPrevious()) {
								iterAccesses.previous();
							}
							break;
						}
					}

					RangerPolicyItemCondition condition         = null;
					Long                      prevConditionType = null;
					while(iterConditions.hasNext()) {
						XXPolicyItemCondition xCondition = iterConditions.next();

						if(xCondition.getPolicyitemid().equals(xPolicyItem.getId())) {
							if(! xCondition.getType().equals(prevConditionType)) {
								condition = new RangerPolicyItemCondition();
								condition.setType(lookupCache.getConditionType(xCondition.getType()));
								condition.getValues().add(xCondition.getValue());

								policyItem.getConditions().add(condition);

								prevConditionType = xCondition.getType();
							} else {
								condition.getValues().add(xCondition.getValue());
							}
						} else {
							if(iterConditions.hasPrevious()) {
								iterConditions.previous();
							}
							break;
						}
					}

					policy.getPolicyItems().add(policyItem);
				} else if(xPolicyItem.getPolicyid().compareTo(policy.getId()) > 0) {
					if(iterPolicyItems.hasPrevious()) {
						iterPolicyItems.previous();
					}
					break;
				}
			}
		}
	}
}

