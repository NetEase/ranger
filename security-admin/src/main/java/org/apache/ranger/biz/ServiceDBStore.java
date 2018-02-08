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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PasswordUtils;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerFactory;
import org.apache.ranger.common.RangerServicePoliciesCache;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAccessTypeDefDao;
import org.apache.ranger.db.XXAccessTypeDefGrantsDao;
import org.apache.ranger.db.XXContextEnricherDefDao;
import org.apache.ranger.db.XXEnumDefDao;
import org.apache.ranger.db.XXEnumElementDefDao;
import org.apache.ranger.db.XXPolicyConditionDefDao;
import org.apache.ranger.db.XXPolicyItemAccessDao;
import org.apache.ranger.db.XXPolicyItemConditionDao;
import org.apache.ranger.db.XXPolicyItemDao;
import org.apache.ranger.db.XXPolicyItemGroupPermDao;
import org.apache.ranger.db.XXPolicyItemUserPermDao;
import org.apache.ranger.db.XXPolicyResourceDao;
import org.apache.ranger.db.XXPolicyResourceMapDao;
import org.apache.ranger.db.XXResourceDefDao;
import org.apache.ranger.db.XXServiceConfigDefDao;
import org.apache.ranger.db.XXServiceConfigMapDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.entity.XXAccessTypeDef;
import org.apache.ranger.entity.XXAccessTypeDefGrants;
import org.apache.ranger.entity.XXContextEnricherDef;
import org.apache.ranger.entity.XXDBBase;
import org.apache.ranger.entity.XXDataHist;
import org.apache.ranger.entity.XXEnumDef;
import org.apache.ranger.entity.XXEnumElementDef;
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
import org.apache.ranger.entity.XXResourceDef;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceConfigDef;
import org.apache.ranger.entity.XXServiceConfigMap;
import org.apache.ranger.entity.XXServiceDef;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemCondition;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.ServicePredicateUtil;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.service.RangerAuditFields;
import org.apache.ranger.service.RangerDataHistService;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.RangerPolicyWithAssignedIdService;
import org.apache.ranger.service.RangerServiceDefService;
import org.apache.ranger.service.RangerServiceDefWithAssignedIdService;
import org.apache.ranger.service.RangerServiceService;
import org.apache.ranger.service.RangerServiceWithAssignedIdService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.RangerPolicyList;
import org.apache.ranger.view.RangerServiceDefList;
import org.apache.ranger.view.RangerServiceList;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


@Component
public class ServiceDBStore implements ServiceStore {
	private static final Log LOG = LogFactory.getLog(ServiceDBStore.class);

	@Autowired
	RangerServiceDefService serviceDefService;

	@Autowired
	RangerDaoManager daoMgr;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	RangerServiceService svcService;

	@Autowired
	StringUtil stringUtil;

	@Autowired
	RangerAuditFields<XXDBBase> rangerAuditFields;

	@Autowired
	RangerPolicyService policyService;

	@Autowired
	XUserService xUserService;

	@Autowired
	XUserMgr xUserMgr;

	@Autowired
	RangerDataHistService dataHistService;

	@Autowired
	@Qualifier(value = "transactionManager")
	PlatformTransactionManager txManager;

	@Autowired
	RangerBizUtil bizUtil;

	@Autowired
	RangerPolicyWithAssignedIdService assignedIdPolicyService;

	@Autowired
	RangerServiceWithAssignedIdService svcServiceWithAssignedId;

	@Autowired
	RangerServiceDefWithAssignedIdService svcDefServiceWithAssignedId;

	@Autowired
	RangerFactory factory;


	private static volatile boolean legacyServiceDefsInitDone = false;
	private Boolean populateExistingBaseFields = false;

	public static final String HIDDEN_PASSWORD_STR = "*****";
	public static final String CONFIG_KEY_PASSWORD = "password";

	private ServicePredicateUtil predicateUtil = null;


	@Override
	public void init() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.init()");
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.init()");
		}
	}

	@PostConstruct
	public void initStore() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.initStore()");
		}

		if(! legacyServiceDefsInitDone) {
			synchronized(ServiceDBStore.class) {
				if(!legacyServiceDefsInitDone) {
					TransactionTemplate txTemplate = new TransactionTemplate(txManager);

					final ServiceDBStore dbStore = this;
					predicateUtil = new ServicePredicateUtil(dbStore);


					txTemplate.execute(new TransactionCallback<Object>() {
						@Override
						public Object doInTransaction(TransactionStatus status) {
							EmbeddedServiceDefsUtil.instance().init(dbStore);

							return null;
						}
					});

					legacyServiceDefsInitDone = true;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.initStore()");
		}
	}

	@Override
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.createServiceDef(" + serviceDef + ")");
		}

		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(
				serviceDef.getName());
		if (xServiceDef != null) {
			throw restErrorUtil.createRESTException("service-def with name: "
							+ serviceDef.getName() + " already exists",
					MessageEnums.ERROR_DUPLICATE_OBJECT);
		}

		List<RangerServiceConfigDef> configs = serviceDef.getConfigs();
		List<RangerResourceDef> resources = serviceDef.getResources();
		List<RangerAccessTypeDef> accessTypes = serviceDef.getAccessTypes();
		List<RangerPolicyConditionDef> policyConditions = serviceDef.getPolicyConditions();
		List<RangerContextEnricherDef> contextEnrichers = serviceDef.getContextEnrichers();
		List<RangerEnumDef> enums = serviceDef.getEnums();


		// While creating, value of version should be 1.
		serviceDef.setVersion(new Long(1));

		if (populateExistingBaseFields) {
			svcDefServiceWithAssignedId.setPopulateExistingBaseFields(true);
			daoMgr.getXXServiceDef().setIdentityInsert(true);

			svcDefServiceWithAssignedId.create(serviceDef);

			svcDefServiceWithAssignedId.setPopulateExistingBaseFields(false);
			daoMgr.getXXServiceDef().updateSequence();
			daoMgr.getXXServiceDef().setIdentityInsert(false);
		} else {
			// following fields will be auto populated
			serviceDef.setId(null);
			serviceDef.setCreateTime(null);
			serviceDef.setUpdateTime(null);
			serviceDef = serviceDefService.create(serviceDef);
		}
		Long serviceDefId = serviceDef.getId();
		XXServiceDef createdSvcDef = daoMgr.getXXServiceDef().getById(serviceDefId);

		XXServiceConfigDefDao xxServiceConfigDao = daoMgr.getXXServiceConfigDef();
		for(int i = 0; i < configs.size(); i++) {
			RangerServiceConfigDef config = configs.get(i);

			XXServiceConfigDef xConfig = new XXServiceConfigDef();
			xConfig = serviceDefService.populateRangerServiceConfigDefToXX(config, xConfig, createdSvcDef,
					RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xConfig.setOrder(i);
			xConfig = xxServiceConfigDao.create(xConfig);
		}

		XXResourceDefDao xxResDefDao = daoMgr.getXXResourceDef();
		for(int i = 0; i < resources.size(); i++) {
			RangerResourceDef resource = resources.get(i);

			XXResourceDef parent = xxResDefDao.findByNameAndServiceDefId(resource.getParent(), serviceDefId);
			Long parentId = (parent != null) ? parent.getId() : null;

			XXResourceDef xResource = new XXResourceDef();
			xResource = serviceDefService.populateRangerResourceDefToXX(resource, xResource, createdSvcDef,
					RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xResource.setOrder(i);
			xResource.setParent(parentId);
			xResource = xxResDefDao.create(xResource);
		}

		XXAccessTypeDefDao xxATDDao = daoMgr.getXXAccessTypeDef();
		for(int i = 0; i < accessTypes.size(); i++) {
			RangerAccessTypeDef accessType = accessTypes.get(i);

			XXAccessTypeDef xAccessType = new XXAccessTypeDef();
			xAccessType = serviceDefService.populateRangerAccessTypeDefToXX(accessType, xAccessType, createdSvcDef,
					RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xAccessType.setOrder(i);
			xAccessType = xxATDDao.create(xAccessType);

			Collection<String> impliedGrants = accessType.getImpliedGrants();
			XXAccessTypeDefGrantsDao xxATDGrantDao = daoMgr.getXXAccessTypeDefGrants();
			for(String impliedGrant : impliedGrants) {
				XXAccessTypeDefGrants xImpliedGrant = new XXAccessTypeDefGrants();
				xImpliedGrant.setAtdId(xAccessType.getId());
				xImpliedGrant.setImpliedGrant(impliedGrant);
				xImpliedGrant = xxATDGrantDao.create(xImpliedGrant);
			}
		}

		XXPolicyConditionDefDao xxPolCondDao = daoMgr.getXXPolicyConditionDef();
		for (int i = 0; i < policyConditions.size(); i++) {
			RangerPolicyConditionDef policyCondition = policyConditions.get(i);

			XXPolicyConditionDef xPolicyCondition = new XXPolicyConditionDef();
			xPolicyCondition = serviceDefService
					.populateRangerPolicyConditionDefToXX(policyCondition,
							xPolicyCondition, createdSvcDef, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xPolicyCondition.setOrder(i);
			xPolicyCondition = xxPolCondDao.create(xPolicyCondition);
		}

		XXContextEnricherDefDao xxContextEnricherDao = daoMgr.getXXContextEnricherDef();
		for (int i = 0; i < contextEnrichers.size(); i++) {
			RangerContextEnricherDef contextEnricher = contextEnrichers.get(i);

			XXContextEnricherDef xContextEnricher = new XXContextEnricherDef();
			xContextEnricher = serviceDefService
					.populateRangerContextEnricherDefToXX(contextEnricher,
							xContextEnricher, createdSvcDef, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xContextEnricher.setOrder(i);
			xContextEnricher = xxContextEnricherDao.create(xContextEnricher);
		}

		XXEnumDefDao xxEnumDefDao = daoMgr.getXXEnumDef();
		for(RangerEnumDef vEnum : enums) {
			XXEnumDef xEnum = new XXEnumDef();
			xEnum = serviceDefService.populateRangerEnumDefToXX(vEnum, xEnum, createdSvcDef, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
			xEnum = xxEnumDefDao.create(xEnum);

			List<RangerEnumElementDef> elements = vEnum.getElements();
			XXEnumElementDefDao xxEnumEleDefDao = daoMgr.getXXEnumElementDef();
			for(int i = 0; i < elements.size(); i++) {
				RangerEnumElementDef element = elements.get(i);

				XXEnumElementDef xElement = new XXEnumElementDef();
				xElement = serviceDefService.populateRangerEnumElementDefToXX(element, xElement, xEnum, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xElement.setOrder(i);
				xElement = xxEnumEleDefDao.create(xElement);
			}
		}
		RangerServiceDef createdServiceDef = serviceDefService.getPopulatedViewObject(createdSvcDef);
		dataHistService.createObjectDataHistory(createdServiceDef, RangerDataHistService.ACTION_CREATE);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.createServiceDef(" + serviceDef + "): " + createdServiceDef);
		}

		return createdServiceDef;
	}

	@Override
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.updateServiceDef(" + serviceDef + ")");
		}

		Long serviceDefId = serviceDef.getId();

		XXServiceDef existing = daoMgr.getXXServiceDef().getById(serviceDefId);
		if (existing == null) {
			throw restErrorUtil.createRESTException("no service-def exists with ID=" + serviceDef.getId(),
					MessageEnums.DATA_NOT_FOUND);
		}

		String existingName = existing.getName();

		boolean renamed = !StringUtils.equalsIgnoreCase(serviceDef.getName(), existingName);

		if (renamed) {
			XXServiceDef renamedSVCDef = daoMgr.getXXServiceDef().findByName(serviceDef.getName());

			if (renamedSVCDef != null) {
				throw restErrorUtil.createRESTException(
						"another service-def already exists with name '" + serviceDef.getName() + "'. ID="
								+ renamedSVCDef.getId(), MessageEnums.DATA_NOT_UPDATABLE);
			}
		}

		List<RangerServiceConfigDef> configs 			= serviceDef.getConfigs() != null 			? serviceDef.getConfigs()   		  : new ArrayList<RangerServiceConfigDef>();
		List<RangerResourceDef> resources 				= serviceDef.getResources() != null  		? serviceDef.getResources() 		  : new ArrayList<RangerResourceDef>();
		List<RangerAccessTypeDef> accessTypes 			= serviceDef.getAccessTypes() != null 		? serviceDef.getAccessTypes() 	  	  : new ArrayList<RangerAccessTypeDef>();
		List<RangerPolicyConditionDef> policyConditions = serviceDef.getPolicyConditions() != null 	? serviceDef.getPolicyConditions() 	  : new ArrayList<RangerPolicyConditionDef>();
		List<RangerContextEnricherDef> contextEnrichers = serviceDef.getContextEnrichers() != null 	? serviceDef.getContextEnrichers() 	  : new ArrayList<RangerContextEnricherDef>();
		List<RangerEnumDef> enums 						= serviceDef.getEnums() != null 			? serviceDef.getEnums() 			  : new ArrayList<RangerEnumDef>();

		serviceDef.setCreateTime(existing.getCreateTime());
		serviceDef.setGuid(existing.getGuid());
		serviceDef.setVersion(existing.getVersion());

		serviceDef = serviceDefService.update(serviceDef);
		XXServiceDef createdSvcDef = daoMgr.getXXServiceDef().getById(serviceDefId);

		updateChildObjectsOfServiceDef(createdSvcDef, configs, resources, accessTypes, policyConditions, contextEnrichers, enums);

		RangerServiceDef updatedSvcDef = getServiceDef(serviceDefId);
		dataHistService.createObjectDataHistory(updatedSvcDef, RangerDataHistService.ACTION_UPDATE);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.updateServiceDef(" + serviceDef + "): " + serviceDef);
		}

		return updatedSvcDef;
	}

	public void updateChildObjectsOfServiceDef(XXServiceDef createdSvcDef, List<RangerServiceConfigDef> configs,
											   List<RangerResourceDef> resources, List<RangerAccessTypeDef> accessTypes,
											   List<RangerPolicyConditionDef> policyConditions, List<RangerContextEnricherDef> contextEnrichers,
											   List<RangerEnumDef> enums) {

		Long serviceDefId = createdSvcDef.getId();

		List<XXServiceConfigDef> xxConfigs = daoMgr.getXXServiceConfigDef().findByServiceDefId(serviceDefId);
		List<XXResourceDef> xxResources = daoMgr.getXXResourceDef().findByServiceDefId(serviceDefId);
		List<XXAccessTypeDef> xxAccessTypes = daoMgr.getXXAccessTypeDef().findByServiceDefId(serviceDefId);
		List<XXPolicyConditionDef> xxPolicyConditions = daoMgr.getXXPolicyConditionDef().findByServiceDefId(
				serviceDefId);
		List<XXContextEnricherDef> xxContextEnrichers = daoMgr.getXXContextEnricherDef().findByServiceDefId(
				serviceDefId);
		List<XXEnumDef> xxEnums = daoMgr.getXXEnumDef().findByServiceDefId(serviceDefId);

		XXServiceConfigDefDao xxServiceConfigDao = daoMgr.getXXServiceConfigDef();
		for (int i = 0; i < configs.size(); i++) {
			RangerServiceConfigDef config = configs.get(i);
			boolean found = false;
			for (XXServiceConfigDef xConfig : xxConfigs) {
				if (config.getItemId() != null && config.getItemId().equals(xConfig.getItemId())) {
					found = true;
					xConfig = serviceDefService.populateRangerServiceConfigDefToXX(config, xConfig, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xConfig.setOrder(i);
					xConfig = xxServiceConfigDao.update(xConfig);
					config = serviceDefService.populateXXToRangerServiceConfigDef(xConfig);
					break;
				}
			}
			if (!found) {
				XXServiceConfigDef xConfig = new XXServiceConfigDef();
				xConfig = serviceDefService.populateRangerServiceConfigDefToXX(config, xConfig, createdSvcDef,
						RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xConfig.setOrder(i);
				xConfig = xxServiceConfigDao.create(xConfig);
				config = serviceDefService.populateXXToRangerServiceConfigDef(xConfig);
			}
		}
		for (XXServiceConfigDef xConfig : xxConfigs) {
			boolean found = false;
			for (RangerServiceConfigDef config : configs) {
				if (xConfig.getItemId() != null && xConfig.getItemId().equals(config.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				xxServiceConfigDao.remove(xConfig);
			}
		}

		XXResourceDefDao xxResDefDao = daoMgr.getXXResourceDef();
		for (RangerResourceDef resource : resources) {
			boolean found = false;
			for (XXResourceDef xRes : xxResources) {
				if (resource.getItemId() != null && resource.getItemId().equals(xRes.getItemId())) {
					found = true;
					xRes = serviceDefService.populateRangerResourceDefToXX(resource, xRes, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xxResDefDao.update(xRes);
					resource = serviceDefService.populateXXToRangerResourceDef(xRes);
					break;
				}
			}
			if (!found) {
				XXResourceDef parent = xxResDefDao.findByNameAndServiceDefId(resource.getParent(), serviceDefId);
				Long parentId = (parent != null) ? parent.getId() : null;

				XXResourceDef xResource = new XXResourceDef();
				xResource = serviceDefService.populateRangerResourceDefToXX(resource, xResource, createdSvcDef,
						RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xResource.setParent(parentId);
				xResource = xxResDefDao.create(xResource);
			}
		}
		for (XXResourceDef xRes : xxResources) {
			boolean found = false;
			for (RangerResourceDef resource : resources) {
				if (xRes.getItemId() != null && xRes.getItemId().equals(resource.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				List<XXPolicyResource> policyResList = daoMgr.getXXPolicyResource().findByResDefId(xRes.getId());
				if (!stringUtil.isEmpty(policyResList)) {
					throw restErrorUtil.createRESTException("Policy/Policies are referring to this resource: "
									+ xRes.getName() + ". Please remove such references from policy before updating service-def.",
							MessageEnums.DATA_NOT_UPDATABLE);
				}
				deleteXXResourceDef(xRes);
			}
		}

		XXAccessTypeDefDao xxATDDao = daoMgr.getXXAccessTypeDef();
		for(int i = 0; i < accessTypes.size(); i++) {
			RangerAccessTypeDef access = accessTypes.get(i);
			boolean found = false;
			for (XXAccessTypeDef xAccess : xxAccessTypes) {
				if (access.getItemId() != null && access.getItemId().equals(xAccess.getItemId())) {
					found = true;
					xAccess = serviceDefService.populateRangerAccessTypeDefToXX(access, xAccess, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xAccess.setOrder(i);
					xAccess = xxATDDao.update(xAccess);

					Collection<String> impliedGrants = access.getImpliedGrants();
					XXAccessTypeDefGrantsDao xxATDGrantDao = daoMgr.getXXAccessTypeDefGrants();
					List<String> xxImpliedGrants = xxATDGrantDao.findImpliedGrantsByATDId(xAccess.getId());
					for (String impliedGrant : impliedGrants) {
						boolean foundGrant = false;
						for (String xImpliedGrant : xxImpliedGrants) {
							if (StringUtils.equalsIgnoreCase(impliedGrant, xImpliedGrant)) {
								foundGrant = true;
								break;
							}
						}
						if (!foundGrant) {
							XXAccessTypeDefGrants xImpliedGrant = new XXAccessTypeDefGrants();
							xImpliedGrant.setAtdId(xAccess.getId());
							xImpliedGrant.setImpliedGrant(impliedGrant);
							xImpliedGrant = xxATDGrantDao.create(xImpliedGrant);
						}
					}
					for (String xImpliedGrant : xxImpliedGrants) {
						boolean foundGrant = false;
						for (String impliedGrant : impliedGrants) {
							if (StringUtils.equalsIgnoreCase(xImpliedGrant, impliedGrant)) {
								foundGrant = true;
								break;
							}
						}
						if (!foundGrant) {
							XXAccessTypeDefGrants xATDGrant = xxATDGrantDao.findByNameAndATDId(xAccess.getId(),
									xImpliedGrant);
							xxATDGrantDao.remove(xATDGrant);

						}
					}
					access = serviceDefService.populateXXToRangerAccessTypeDef(xAccess);
					break;
				}
			}
			if (!found) {
				XXAccessTypeDef xAccessType = new XXAccessTypeDef();
				xAccessType = serviceDefService.populateRangerAccessTypeDefToXX(access, xAccessType, createdSvcDef,
						RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xAccessType.setOrder(i);
				xAccessType = xxATDDao.create(xAccessType);

				Collection<String> impliedGrants = access.getImpliedGrants();
				XXAccessTypeDefGrantsDao xxATDGrantDao = daoMgr.getXXAccessTypeDefGrants();
				for (String impliedGrant : impliedGrants) {
					XXAccessTypeDefGrants xImpliedGrant = new XXAccessTypeDefGrants();
					xImpliedGrant.setAtdId(xAccessType.getId());
					xImpliedGrant.setImpliedGrant(impliedGrant);
					xImpliedGrant = xxATDGrantDao.create(xImpliedGrant);
				}
				access = serviceDefService.populateXXToRangerAccessTypeDef(xAccessType);
			}
		}

		for (XXAccessTypeDef xAccess : xxAccessTypes) {
			boolean found = false;
			for (RangerAccessTypeDef access : accessTypes) {
				if (xAccess.getItemId() != null && xAccess.getItemId().equals(access.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				List<XXPolicyItemAccess> polItemAccessList = daoMgr.getXXPolicyItemAccess().findByType(xAccess.getId());
				if(!stringUtil.isEmpty(polItemAccessList)) {
					throw restErrorUtil.createRESTException("Policy/Policies are referring to this access-type: "
									+ xAccess.getName() + ". Please remove such references from policy before updating service-def.",
							MessageEnums.DATA_NOT_UPDATABLE);
				}
				deleteXXAccessTypeDef(xAccess);
			}
		}

		XXPolicyConditionDefDao xxPolCondDao = daoMgr.getXXPolicyConditionDef();
		for (int i = 0; i < policyConditions.size(); i++) {
			RangerPolicyConditionDef condition = policyConditions.get(i);
			boolean found = false;
			for (XXPolicyConditionDef xCondition : xxPolicyConditions) {
				if (condition.getItemId() != null && condition.getItemId().equals(xCondition.getItemId())) {
					found = true;
					xCondition = serviceDefService.populateRangerPolicyConditionDefToXX(condition, xCondition,
							createdSvcDef, RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xCondition.setOrder(i);
					xCondition = xxPolCondDao.update(xCondition);
					condition = serviceDefService.populateXXToRangerPolicyConditionDef(xCondition);
					break;
				}
			}
			if (!found) {
				XXPolicyConditionDef xCondition = new XXPolicyConditionDef();
				xCondition = serviceDefService.populateRangerPolicyConditionDefToXX(condition, xCondition,
						createdSvcDef, RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xCondition.setOrder(i);
				xCondition = xxPolCondDao.create(xCondition);
				condition = serviceDefService.populateXXToRangerPolicyConditionDef(xCondition);
			}
		}
		for(XXPolicyConditionDef xCondition : xxPolicyConditions) {
			boolean found = false;
			for(RangerPolicyConditionDef condition : policyConditions) {
				if(xCondition.getItemId() != null && xCondition.getItemId().equals(condition.getItemId())) {
					found = true;
					break;
				}
			}
			if(!found) {
				List<XXPolicyItemCondition> policyItemCondList = daoMgr.getXXPolicyItemCondition()
						.findByPolicyConditionDefId(xCondition.getId());
				if(!stringUtil.isEmpty(policyItemCondList)) {
					throw restErrorUtil.createRESTException("Policy/Policies are referring to this policy-condition: "
									+ xCondition.getName() + ". Please remove such references from policy before updating service-def.",
							MessageEnums.DATA_NOT_UPDATABLE);
				}
				for(XXPolicyItemCondition policyItemCond : policyItemCondList) {
					daoMgr.getXXPolicyItemCondition().remove(policyItemCond);
				}
				xxPolCondDao.remove(xCondition);
			}
		}

		XXContextEnricherDefDao xxContextEnricherDao = daoMgr.getXXContextEnricherDef();
		for (int i = 0; i < contextEnrichers.size(); i++) {
			RangerContextEnricherDef context = contextEnrichers.get(i);
			boolean found = false;
			for (XXContextEnricherDef xContext : xxContextEnrichers) {
				if (context.getItemId() != null && context.getItemId().equals(xContext.getItemId())) {
					found = true;
					xContext = serviceDefService.populateRangerContextEnricherDefToXX(context, xContext, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xContext.setOrder(i);
					xContext = xxContextEnricherDao.update(xContext);
					context = serviceDefService.populateXXToRangerContextEnricherDef(xContext);
					break;
				}
			}
			if (!found) {
				XXContextEnricherDef xContext = new XXContextEnricherDef();
				xContext = serviceDefService.populateRangerContextEnricherDefToXX(context, xContext, createdSvcDef,
						RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
				xContext.setOrder(i);
				xContext = xxContextEnricherDao.create(xContext);
				context = serviceDefService.populateXXToRangerContextEnricherDef(xContext);
			}
		}
		for (XXContextEnricherDef xContext : xxContextEnrichers) {
			boolean found = false;
			for (RangerContextEnricherDef context : contextEnrichers) {
				if (xContext.getItemId() != null && xContext.getItemId().equals(context.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				daoMgr.getXXContextEnricherDef().remove(xContext);
			}
		}

		XXEnumDefDao xxEnumDefDao = daoMgr.getXXEnumDef();
		for (RangerEnumDef enumDef : enums) {
			boolean found = false;
			for (XXEnumDef xEnumDef : xxEnums) {
				if (enumDef.getItemId() != null && enumDef.getItemId().equals(xEnumDef.getItemId())) {
					found = true;
					xEnumDef = serviceDefService.populateRangerEnumDefToXX(enumDef, xEnumDef, createdSvcDef,
							RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
					xEnumDef = xxEnumDefDao.update(xEnumDef);

					XXEnumElementDefDao xEnumEleDao = daoMgr.getXXEnumElementDef();
					List<XXEnumElementDef> xxEnumEleDefs = xEnumEleDao.findByEnumDefId(xEnumDef.getId());
					List<RangerEnumElementDef> enumEleDefs = enumDef.getElements();

					for (int i = 0; i < enumEleDefs.size(); i++) {
						RangerEnumElementDef eleDef = enumEleDefs.get(i);
						boolean foundEle = false;
						for (XXEnumElementDef xEleDef : xxEnumEleDefs) {
							if (eleDef.getItemId() != null && eleDef.getItemId().equals(xEleDef.getItemId())) {
								foundEle = true;
								xEleDef = serviceDefService.populateRangerEnumElementDefToXX(eleDef, xEleDef, xEnumDef,
										RangerServiceDefService.OPERATION_UPDATE_CONTEXT);
								xEleDef.setOrder(i);
								xEleDef = xEnumEleDao.update(xEleDef);
								break;
							}
						}
						if (!foundEle) {
							XXEnumElementDef xElement = new XXEnumElementDef();
							xElement = serviceDefService.populateRangerEnumElementDefToXX(eleDef, xElement, xEnumDef,
									RangerServiceDefService.OPERATION_CREATE_CONTEXT);
							xElement.setOrder(i);
							xElement = xEnumEleDao.create(xElement);
						}
					}
					for (XXEnumElementDef xxEleDef : xxEnumEleDefs) {
						boolean foundEle = false;
						for (RangerEnumElementDef enumEle : enumEleDefs) {
							if (xxEleDef.getItemId() != null && xxEleDef.getItemId().equals(enumEle.getItemId())) {
								foundEle = true;
								break;
							}
						}
						if (!foundEle) {
							xEnumEleDao.remove(xxEleDef);
						}
					}
					enumDef = serviceDefService.populateXXToRangerEnumDef(xEnumDef);
					break;
				}
			}
			if (!found) {
				XXEnumDef xEnum = new XXEnumDef();
				xEnum = serviceDefService.populateRangerEnumDefToXX(enumDef, xEnum, createdSvcDef,
						RangerServiceDefService.OPERATION_CREATE_CONTEXT);
				xEnum = xxEnumDefDao.create(xEnum);

				List<RangerEnumElementDef> elements = enumDef.getElements();
				XXEnumElementDefDao xxEnumEleDefDao = daoMgr.getXXEnumElementDef();
				for (RangerEnumElementDef element : elements) {
					XXEnumElementDef xElement = new XXEnumElementDef();
					xElement = serviceDefService.populateRangerEnumElementDefToXX(element, xElement, xEnum,
							RangerServiceDefService.OPERATION_CREATE_CONTEXT);
					xElement = xxEnumEleDefDao.create(xElement);
				}
				enumDef = serviceDefService.populateXXToRangerEnumDef(xEnum);
			}
		}
		for (XXEnumDef xEnumDef : xxEnums) {
			boolean found = false;
			for (RangerEnumDef enumDef : enums) {
				if (xEnumDef.getItemId() != null && xEnumDef.getItemId().equals(enumDef.getItemId())) {
					found = true;
					break;
				}
			}
			if (!found) {
				List<XXEnumElementDef> enumEleDefList = daoMgr.getXXEnumElementDef().findByEnumDefId(xEnumDef.getId());
				for (XXEnumElementDef eleDef : enumEleDefList) {
					daoMgr.getXXEnumElementDef().remove(eleDef);
				}
				xxEnumDefDao.remove(xEnumDef);
			}
		}
	}

	@Override
	public void deleteServiceDef(Long serviceDefId) throws Exception {

		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session == null) {
			throw restErrorUtil.createRESTException(
					"UserSession cannot be null, only Admin can update service-def",
					MessageEnums.OPER_NO_PERMISSION);
		}

		if (!session.isKeyAdmin() && !session.isUserAdmin()) {
			throw restErrorUtil.createRESTException(
					"User is not allowed to update service-def, only Admin can update service-def",
					MessageEnums.OPER_NO_PERMISSION);
		}

		deleteServiceDef(serviceDefId, false);
	}

	public void deleteServiceDef(Long serviceDefId, boolean forceDelete) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.deleteServiceDef(" + serviceDefId + ")");
		}

		RangerServiceDef serviceDef = getServiceDef(serviceDefId);
		if(serviceDef == null) {
			throw restErrorUtil.createRESTException("No Service Definiton found for Id: " + serviceDefId,
					MessageEnums.DATA_NOT_FOUND);
		}

		if (!forceDelete) {
			List<XXService> svcDefServiceList = daoMgr.getXXService().findByServiceDefId(serviceDefId);
			if (!stringUtil.isEmpty(svcDefServiceList)) {
				throw restErrorUtil.createRESTException(
						"Services exists under given service definition, can't delete Service-Def: "
								+ serviceDef.getName(), MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
			}
		}

		List<XXAccessTypeDef> accTypeDefs = daoMgr.getXXAccessTypeDef().findByServiceDefId(serviceDefId);
		for(XXAccessTypeDef accessType : accTypeDefs) {
			deleteXXAccessTypeDef(accessType);
		}

		XXContextEnricherDefDao xContextEnricherDao = daoMgr.getXXContextEnricherDef();
		List<XXContextEnricherDef> contextEnrichers = xContextEnricherDao.findByServiceDefId(serviceDefId);
		for(XXContextEnricherDef context : contextEnrichers) {
			xContextEnricherDao.remove(context);
		}

		XXEnumDefDao enumDefDao = daoMgr.getXXEnumDef();
		List<XXEnumDef> enumDefList = enumDefDao.findByServiceDefId(serviceDefId);
		for (XXEnumDef enumDef : enumDefList) {
			List<XXEnumElementDef> enumEleDefList = daoMgr.getXXEnumElementDef().findByEnumDefId(enumDef.getId());
			for (XXEnumElementDef eleDef : enumEleDefList) {
				daoMgr.getXXEnumElementDef().remove(eleDef);
			}
			enumDefDao.remove(enumDef);
		}

		XXPolicyConditionDefDao policyCondDao = daoMgr.getXXPolicyConditionDef();
		List<XXPolicyConditionDef> policyCondList = policyCondDao.findByServiceDefId(serviceDefId);

		for (XXPolicyConditionDef policyCond : policyCondList) {
			List<XXPolicyItemCondition> policyItemCondList = daoMgr.getXXPolicyItemCondition().findByPolicyConditionDefId(policyCond.getId());
			for (XXPolicyItemCondition policyItemCond : policyItemCondList) {
				daoMgr.getXXPolicyItemCondition().remove(policyItemCond);
			}
			policyCondDao.remove(policyCond);
		}

		List<XXResourceDef> resDefList = daoMgr.getXXResourceDef().findByServiceDefId(serviceDefId);
		for(XXResourceDef resDef : resDefList) {
			deleteXXResourceDef(resDef);
		}

		XXServiceConfigDefDao configDefDao = daoMgr.getXXServiceConfigDef();
		List<XXServiceConfigDef> configDefList = configDefDao.findByServiceDefId(serviceDefId);
		for(XXServiceConfigDef configDef : configDefList) {
			configDefDao.remove(configDef);
		}

		XXServiceDao serviceDao = daoMgr.getXXService();
		List<XXService> serviceList = serviceDao.findByServiceDefId(serviceDefId);
		for(XXService service : serviceList) {
			deleteService(service.getId());
		}

		Long version = serviceDef.getVersion();
		if(version == null) {
			version = new Long(1);
			LOG.info("Found Version Value: `null`, so setting value of version to 1, While updating object, version should not be null.");
		} else {
			version = new Long(version.longValue() + 1);
		}
		serviceDef.setVersion(version);

		serviceDefService.delete(serviceDef);
		LOG.info("ServiceDefinition has been deleted successfully. Service-Def Name: " + serviceDef.getName());

		dataHistService.createObjectDataHistory(serviceDef, RangerDataHistService.ACTION_DELETE);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.deleteServiceDef(" + serviceDefId + ")");
		}
	}

	public void deleteXXAccessTypeDef(XXAccessTypeDef xAccess) {
		List<XXAccessTypeDefGrants> atdGrantsList = daoMgr.getXXAccessTypeDefGrants().findByATDId(xAccess.getId());

		for (XXAccessTypeDefGrants atdGrant : atdGrantsList) {
			daoMgr.getXXAccessTypeDefGrants().remove(atdGrant);
		}

		List<XXPolicyItemAccess> policyItemAccessList = daoMgr.getXXPolicyItemAccess().findByType(xAccess.getId());
		for (XXPolicyItemAccess policyItemAccess : policyItemAccessList) {
			daoMgr.getXXPolicyItemAccess().remove(policyItemAccess);
		}
		daoMgr.getXXAccessTypeDef().remove(xAccess);
	}

	public void deleteXXResourceDef(XXResourceDef xRes) {

		List<XXResourceDef> xChildObjs = daoMgr.getXXResourceDef().findByParentResId(xRes.getId());
		for(XXResourceDef childRes : xChildObjs) {
			deleteXXResourceDef(childRes);
		}

		List<XXPolicyResource> xxResources = daoMgr.getXXPolicyResource().findByResDefId(xRes.getId());
		for (XXPolicyResource xPolRes : xxResources) {
			deleteXXPolicyResource(xPolRes);
		}

		daoMgr.getXXResourceDef().remove(xRes);
	}

	public void deleteXXPolicyResource(XXPolicyResource xPolRes) {
		List<XXPolicyResourceMap> polResMapList = daoMgr.getXXPolicyResourceMap().findByPolicyResId(xPolRes.getId());
		XXPolicyResourceMapDao polResMapDao = daoMgr.getXXPolicyResourceMap();
		for (XXPolicyResourceMap xxPolResMap : polResMapList) {
			polResMapDao.remove(xxPolResMap);
		}
		daoMgr.getXXPolicyResource().remove(xPolRes);
	}

	@Override
	public RangerServiceDef getServiceDef(Long id) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.getServiceDef(" + id + ")");
		}

		RangerServiceDef ret = serviceDefService.read(id);
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefDBStore.getServiceDef(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceDef getServiceDefByName(String name) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.getServiceDefByName(" + name + ")");
		}

		RangerServiceDef ret = null;

		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(name);

		if(xServiceDef != null) {
			ret = serviceDefService.getPopulatedViewObject(xServiceDef);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("== ServiceDefDBStore.getServiceDefByName(" + name + "): " + ret);
		}

		return  ret;
	}

	@Override
	public List<RangerServiceDef> getServiceDefs(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceDefs(" + filter + ")");
		}

		RangerServiceDefList svcDefList = serviceDefService.searchRangerServiceDefs(filter);

		predicateUtil.applyFilter(svcDefList.getServiceDefs(), filter);

		List<RangerServiceDef> ret = svcDefList.getServiceDefs();

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceDefs(" + filter + "): " + ret);
		}

		return ret;
	}

	public RangerServiceDefList getPaginatedServiceDefs(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServiceDefs(" + filter + ")");
		}

		RangerServiceDefList svcDefList = serviceDefService.searchRangerServiceDefs(filter);

		predicateUtil.applyFilter(svcDefList.getServiceDefs(), filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServiceDefs(" + filter + ")");
		}

		return svcDefList;
	}

	@Override
	public RangerService createService(RangerService service) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefDBStore.createService(" + service + ")");
		}

		if (service == null) {
			throw restErrorUtil.createRESTException("Service object cannot be null.",
					MessageEnums.ERROR_CREATING_OBJECT);
		}

		boolean createDefaultPolicy = true;
		Map<String, String> configs = service.getConfigs();
		Map<String, String> validConfigs = validateRequiredConfigParams(service, configs);
		if (validConfigs == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> ConfigParams cannot be null, ServiceDefDBStore.createService(" + service + ")");
			}
			throw restErrorUtil.createRESTException("ConfigParams cannot be null.", MessageEnums.ERROR_CREATING_OBJECT);
		}

		// While creating, value of version should be 1.
		service.setVersion(new Long(1));

		if (populateExistingBaseFields) {
			svcServiceWithAssignedId.setPopulateExistingBaseFields(true);
			daoMgr.getXXService().setIdentityInsert(true);

			service = svcServiceWithAssignedId.create(service);

			daoMgr.getXXService().setIdentityInsert(false);
			daoMgr.getXXService().updateSequence();
			svcServiceWithAssignedId.setPopulateExistingBaseFields(false);
			createDefaultPolicy = false;
		} else {
			service = svcService.create(service);
		}
		XXService xCreatedService = daoMgr.getXXService().getById(service.getId());
		VXUser vXUser = null;

		XXServiceConfigMapDao xConfMapDao = daoMgr.getXXServiceConfigMap();
		for (Entry<String, String> configMap : validConfigs.entrySet()) {
			String configKey = configMap.getKey();
			String configValue = configMap.getValue();

			if (StringUtils.equalsIgnoreCase(configKey, "username")) {
				String userName = stringUtil.getValidUserName(configValue);
				XXUser xxUser = daoMgr.getXXUser().findByUserName(userName);
				if (xxUser != null) {
					vXUser = xUserService.populateViewBean(xxUser);
				} else {
					UserSessionBase usb = ContextUtil.getCurrentUserSession();
					if (usb != null && !usb.isUserAdmin()) {
						throw restErrorUtil.createRESTException("User does not exist with given username: ["
								+ userName + "] please use existing user", MessageEnums.OPER_NO_PERMISSION);
					}
					vXUser = xUserMgr.createServiceConfigUser(userName);
				}
			}

			if (StringUtils.equalsIgnoreCase(configKey, CONFIG_KEY_PASSWORD)) {
				String encryptedPwd = PasswordUtils.encryptPassword(configValue);
				String decryptedPwd = PasswordUtils.decryptPassword(encryptedPwd);

				if (StringUtils.equals(decryptedPwd, configValue)) {
					configValue = encryptedPwd;
				}
			}

			XXServiceConfigMap xConfMap = new XXServiceConfigMap();
			xConfMap = (XXServiceConfigMap) rangerAuditFields.populateAuditFields(xConfMap, xCreatedService);
			xConfMap.setServiceId(xCreatedService.getId());
			xConfMap.setConfigkey(configKey);
			xConfMap.setConfigvalue(configValue);
			xConfMap = xConfMapDao.create(xConfMap);
		}
		RangerService createdService = svcService.getPopulatedViewObject(xCreatedService);
		dataHistService.createObjectDataHistory(createdService, RangerDataHistService.ACTION_CREATE);

		List<XXTrxLog> trxLogList = svcService.getTransactionLog(createdService,
				RangerServiceService.OPERATION_CREATE_CONTEXT);
		bizUtil.createTrxLog(trxLogList);

		if (createDefaultPolicy) {
			createDefaultPolicies(xCreatedService, vXUser);
		}

		return createdService;

	}

	@Override
	public RangerService updateService(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.updateService()");
		}

		XXService existing = daoMgr.getXXService().getById(service.getId());

		if(existing == null) {
			throw restErrorUtil.createRESTException(
					"no service exists with ID=" + service.getId(),
					MessageEnums.DATA_NOT_FOUND);
		}

		String existingName = existing.getName();

		boolean renamed = !StringUtils.equalsIgnoreCase(service.getName(), existingName);

		if(renamed) {
			XXService newNameService = daoMgr.getXXService().findByName(service.getName());

			if(newNameService != null) {
				throw restErrorUtil.createRESTException("another service already exists with name '"
						+ service.getName() + "'. ID=" + newNameService.getId(), MessageEnums.DATA_NOT_UPDATABLE);
			}
		}

		Map<String, String> configs = service.getConfigs();
		Map<String, String> validConfigs = validateRequiredConfigParams(service, configs);
		if (validConfigs == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("==> ConfigParams cannot be null, ServiceDefDBStore.createService(" + service + ")");
			}
			throw restErrorUtil.createRESTException("ConfigParams cannot be null.", MessageEnums.ERROR_CREATING_OBJECT);
		}

		List<XXTrxLog> trxLogList = svcService.getTransactionLog(service, existing, RangerServiceService.OPERATION_UPDATE_CONTEXT);

		if(populateExistingBaseFields) {
			svcServiceWithAssignedId.setPopulateExistingBaseFields(true);
			service = svcServiceWithAssignedId.update(service);
			svcServiceWithAssignedId.setPopulateExistingBaseFields(false);
		} else {
			service.setCreateTime(existing.getCreateTime());
			service.setGuid(existing.getGuid());
			service.setVersion(existing.getVersion());
			service.setPolicyUpdateTime(existing.getPolicyUpdateTime());
			service.setPolicyVersion(existing.getPolicyVersion());

			service = svcService.update(service);
		}

		XXService xUpdService = daoMgr.getXXService().getById(service.getId());

		String oldPassword = null;

		List<XXServiceConfigMap> dbConfigMaps = daoMgr.getXXServiceConfigMap().findByServiceId(service.getId());
		for(XXServiceConfigMap dbConfigMap : dbConfigMaps) {
			if(StringUtils.equalsIgnoreCase(dbConfigMap.getConfigkey(), CONFIG_KEY_PASSWORD)) {
				oldPassword = dbConfigMap.getConfigvalue();
			}
			daoMgr.getXXServiceConfigMap().remove(dbConfigMap);
		}

		VXUser vXUser = null;
		XXServiceConfigMapDao xConfMapDao = daoMgr.getXXServiceConfigMap();
		for (Entry<String, String> configMap : validConfigs.entrySet()) {
			String configKey = configMap.getKey();
			String configValue = configMap.getValue();

			if(StringUtils.equalsIgnoreCase(configKey, "username")) {
				String userName = stringUtil.getValidUserName(configValue);
				XXUser xxUser = daoMgr.getXXUser().findByUserName(userName);
				if (xxUser != null) {
					vXUser = xUserService.populateViewBean(xxUser);
				} else {
					UserSessionBase usb = ContextUtil.getCurrentUserSession();
					if (usb != null && !usb.isUserAdmin()) {
						throw restErrorUtil.createRESTException("User does not exist with given username: ["
								+ userName + "] please use existing user", MessageEnums.OPER_NO_PERMISSION);
					}
					vXUser = xUserMgr.createServiceConfigUser(userName);
				}
			}

			if (StringUtils.equalsIgnoreCase(configKey, CONFIG_KEY_PASSWORD)) {
				if (StringUtils.equalsIgnoreCase(configValue, HIDDEN_PASSWORD_STR)) {
					configValue = oldPassword;
				} else {
					String encryptedPwd = PasswordUtils.encryptPassword(configValue);
					String decryptedPwd = PasswordUtils.decryptPassword(encryptedPwd);

					if (StringUtils.equals(decryptedPwd, configValue)) {
						configValue = encryptedPwd;
					}
				}
			}

			XXServiceConfigMap xConfMap = new XXServiceConfigMap();
			xConfMap = (XXServiceConfigMap) rangerAuditFields.populateAuditFields(xConfMap, xUpdService);
			xConfMap.setServiceId(service.getId());
			xConfMap.setConfigkey(configKey);
			xConfMap.setConfigvalue(configValue);
			xConfMap = xConfMapDao.create(xConfMap);
		}

		RangerService updService = svcService.getPopulatedViewObject(xUpdService);
		dataHistService.createObjectDataHistory(updService, RangerDataHistService.ACTION_UPDATE);
		bizUtil.createTrxLog(trxLogList);

		return updService;
	}

	@Override
	public void deleteService(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.deleteService(" + id + ")");
		}

		RangerService service = getService(id);

		if(service == null) {
			throw new Exception("no service exists with ID=" + id);
		}

		List<XXPolicy> policies = daoMgr.getXXPolicy().findByServiceId(service.getId());
		for(XXPolicy policy : policies) {
			LOG.info("Deleting Policy, policyName: " + policy.getName());
			deletePolicy(policy.getId());
		}

		XXServiceConfigMapDao configDao = daoMgr.getXXServiceConfigMap();
		List<XXServiceConfigMap> configs = configDao.findByServiceId(service.getId());
		for (XXServiceConfigMap configMap : configs) {
			configDao.remove(configMap);
		}

		Long version = service.getVersion();
		if(version == null) {
			version = new Long(1);
			LOG.info("Found Version Value: `null`, so setting value of version to 1, While updating object, version should not be null.");
		} else {
			version = new Long(version.longValue() + 1);
		}
		service.setVersion(version);

		svcService.delete(service);

		dataHistService.createObjectDataHistory(service, RangerDataHistService.ACTION_DELETE);

		List<XXTrxLog> trxLogList = svcService.getTransactionLog(service, RangerServiceService.OPERATION_DELETE_CONTEXT);
		bizUtil.createTrxLog(trxLogList);
	}

	@Override
	public List<RangerPolicy> getPoliciesByResourceSignature(String serviceName, String policySignature, Boolean isPolicyEnabled) throws Exception {

		List<XXPolicy> xxPolicies = daoMgr.getXXPolicy().findByResourceSignatureByPolicyStatus(serviceName, policySignature, isPolicyEnabled);
		List<RangerPolicy> policies = new ArrayList<RangerPolicy>(xxPolicies.size());
		for (XXPolicy xxPolicy : xxPolicies) {
			RangerPolicy policy = policyService.getPopulatedViewObject(xxPolicy);
			policies.add(policy);
		}

		return policies;
	}

	@Override
	public RangerService getService(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getService()");
		}

		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session == null) {
			throw restErrorUtil.createRESTException("UserSession cannot be null.",
					MessageEnums.OPER_NOT_ALLOWED_FOR_STATE);
		}

		XXService xService = daoMgr.getXXService().getById(id);

		// TODO: As of now we are allowing SYS_ADMIN to read all the
		// services including KMS

		if (!bizUtil.hasAccess(xService, null)) {
			throw restErrorUtil.createRESTException("Logged in user is not allowed to read service, id: " + id,
					MessageEnums.OPER_NO_PERMISSION);
		}

		return svcService.getPopulatedViewObject(xService);
	}

	@Override
	public RangerService getServiceByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServiceByName()");
		}
		XXService xService = daoMgr.getXXService().findByName(name);

		// TODO: As of now we are allowing SYS_ADMIN to read all the
		// services including KMS

		if (ContextUtil.getCurrentUserSession() != null) {
			if (xService == null) {
				return null;
			}
			if (!bizUtil.hasAccess(xService, null)) {
				throw restErrorUtil.createRESTException("Logged in user is not allowed to read service, name: " + name,
						MessageEnums.OPER_NO_PERMISSION);
			}
		}

		return xService == null ? null : svcService.getPopulatedViewObject(xService);
	}

	@Override
	public List<RangerService> getServices(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServices()");
		}

		RangerServiceList serviceList = svcService.searchRangerServices(filter);

		predicateUtil.applyFilter(serviceList.getServices(), filter);

		List<RangerService> ret = serviceList.getServices();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServices()");
		}

		return ret;
	}

	public RangerServiceList getPaginatedServices(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServices()");
		}

		RangerServiceList serviceList = svcService.searchRangerServices(filter);

		predicateUtil.applyFilter(serviceList.getServices(), filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPaginatedServices()");
		}

		return serviceList;
	}

	@Override
	public RangerPolicy createPolicy(RangerPolicy policy) throws Exception {

		RangerService service = getServiceByName(policy.getService());

		if(service == null) {
			throw new Exception("service does not exist - name=" + policy.getService());
		}

		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(service.getType());

		if(xServiceDef == null) {
			throw new Exception("service-def does not exist - name=" + service.getType());
		}

		XXPolicy existing = daoMgr.getXXPolicy().findByNameAndServiceId(policy.getName(), service.getId());

		if(existing != null) {
			throw new Exception("policy already exists: ServiceName=" + policy.getService() + "; PolicyName=" + policy.getName() + ". ID=" + existing.getId());
		}

		Map<String, RangerPolicyResource> resources = policy.getResources();
		List<RangerPolicyItem> policyItems = policy.getPolicyItems();

		policy.setVersion(new Long(1));
		updatePolicySignature(policy);

		if(populateExistingBaseFields) {
			assignedIdPolicyService.setPopulateExistingBaseFields(true);
			daoMgr.getXXPolicy().setIdentityInsert(true);

			policy = assignedIdPolicyService.create(policy);

			daoMgr.getXXPolicy().setIdentityInsert(false);
			daoMgr.getXXPolicy().updateSequence();
			assignedIdPolicyService.setPopulateExistingBaseFields(false);
		} else {
			policy = policyService.create(policy);
		}

		XXPolicy xCreatedPolicy = daoMgr.getXXPolicy().getById(policy.getId());

		createNewResourcesForPolicy(policy, xCreatedPolicy, resources);
		createNewPolicyItemsForPolicy(policy, xCreatedPolicy, policyItems, xServiceDef);
		handlePolicyUpdate(service);
		RangerPolicy createdPolicy = policyService.getPopulatedViewObject(xCreatedPolicy);
		dataHistService.createObjectDataHistory(createdPolicy, RangerDataHistService.ACTION_CREATE);

		List<XXTrxLog> trxLogList = policyService.getTransactionLog(createdPolicy, RangerPolicyService.OPERATION_CREATE_CONTEXT);
		bizUtil.createTrxLog(trxLogList);

		RangerServicePoliciesCache.getInstance().
				updatePolicyInCache("create", createdPolicy, this);

		return createdPolicy;
	}

	@Override
	public RangerPolicy updatePolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.updatePolicy(" + policy + ")");
		}

		XXPolicy xxExisting = daoMgr.getXXPolicy().getById(policy.getId());
		RangerPolicy existing = policyService.getPopulatedViewObject(xxExisting);

		if(existing == null) {
			throw new Exception("no policy exists with ID=" + policy.getId());
		}

		RangerService service = getServiceByName(policy.getService());

		if(service == null) {
			throw new Exception("service does not exist - name=" + policy.getService());
		}

		XXServiceDef xServiceDef = daoMgr.getXXServiceDef().findByName(service.getType());

		if(xServiceDef == null) {
			throw new Exception("service-def does not exist - name=" + service.getType());
		}

		if(! StringUtils.equalsIgnoreCase(existing.getService(), policy.getService())) {
			throw new Exception("policy id=" + policy.getId() + " already exists in service " + existing.getService() + ". It can not be moved to service " + policy.getService());
		}
		boolean renamed = !StringUtils.equalsIgnoreCase(policy.getName(), existing.getName());

		if(renamed) {
			XXPolicy newNamePolicy = daoMgr.getXXPolicy().findByNameAndServiceId(policy.getName(), service.getId());

			if(newNamePolicy != null) {
				throw new Exception("another policy already exists with name '" + policy.getName() + "'. ID=" + newNamePolicy.getId());
			}
		}
		Map<String, RangerPolicyResource> newResources = policy.getResources();
		List<RangerPolicyItem> newPolicyItems = policy.getPolicyItems();

		policy.setCreateTime(xxExisting.getCreateTime());
		policy.setGuid(xxExisting.getGuid());
		policy.setVersion(xxExisting.getVersion());

		List<XXTrxLog> trxLogList = policyService.getTransactionLog(policy, xxExisting, RangerPolicyService.OPERATION_UPDATE_CONTEXT);

		updatePolicySignature(policy);

		policy = policyService.update(policy);
		XXPolicy newUpdPolicy = daoMgr.getXXPolicy().getById(policy.getId());

		deleteExistingPolicyResources(policy);
		deleteExistingPolicyItems(policy);

		createNewResourcesForPolicy(policy, newUpdPolicy, newResources);
		createNewPolicyItemsForPolicy(policy, newUpdPolicy, newPolicyItems, xServiceDef);

		handlePolicyUpdate(service);
		RangerPolicy updPolicy = policyService.getPopulatedViewObject(newUpdPolicy);
		dataHistService.createObjectDataHistory(updPolicy, RangerDataHistService.ACTION_UPDATE);

		bizUtil.createTrxLog(trxLogList);

		RangerServicePoliciesCache.getInstance().
				updatePolicyInCache("update", updPolicy, this);

		return updPolicy;
	}

	public void deletePolicys(List<RangerPolicy> rangerPolicies) throws Exception {
		if (rangerPolicies == null) {
			return;
		}
		for(RangerPolicy rangerPolicy : rangerPolicies) {
			deletePolicy(rangerPolicy.getId());
		}
	}

	@Override
	public void deletePolicy(Long policyId) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.deletePolicy(" + policyId + ")");
		}

		RangerPolicy policy = getPolicy(policyId);

		if(policy == null) {
			throw new Exception("no policy exists with ID=" + policyId);
		}

		String policyName = policy.getName();
		RangerService service = getServiceByName(policy.getService());

		if(service == null) {
			throw new Exception("service does not exist - name='" + policy.getService());
		}

		Long version = policy.getVersion();
		if(version == null) {
			version = new Long(1);
			LOG.info("Found Version Value: `null`, so setting value of version to 1, While updating object, version should not be null.");
		} else {
			version = new Long(version.longValue() + 1);
		}

		policy.setVersion(version);

		List<XXTrxLog> trxLogList = policyService.getTransactionLog(policy, RangerPolicyService.OPERATION_DELETE_CONTEXT);

		deleteExistingPolicyItems(policy);
		deleteExistingPolicyResources(policy);

		policyService.delete(policy);
		handlePolicyUpdate(service);

		dataHistService.createObjectDataHistory(policy, RangerDataHistService.ACTION_DELETE);

		bizUtil.createTrxLog(trxLogList);

		RangerServicePoliciesCache.getInstance().
				updatePolicyInCache("delete", policy, this);

		LOG.info("Policy Deleted Successfully. PolicyName : " + policyName);
	}

	@Override
	public RangerPolicy getPolicy(Long id) throws Exception {
		return policyService.read(id);
	}

	@Override
	public List<RangerPolicy> getPolicies(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPolicies()");
		}

		RangerPolicyList policyList = policyService.searchRangerPolicies(filter);

		predicateUtil.applyFilter(policyList.getPolicies(), filter);

		List<RangerPolicy> ret = policyList.getPolicies();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPolicies()");
		}

		return ret;
	}

	public RangerPolicyList getPaginatedPolicies(SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedPolicies(+ " + filter + ")");
		}

		RangerPolicyList policyList = policyService.searchRangerPolicies(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("before filter: count=" + policyList.getListSize());
		}
		predicateUtil.applyFilter(policyList.getPolicies(), filter);
		if (LOG.isDebugEnabled()) {
			LOG.debug("after filter: count=" + policyList.getListSize());
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPaginatedPolicies(" + filter + "): count=" + policyList.getListSize());
		}

		return policyList;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies(" + serviceId + ")");
		}

		XXService service = daoMgr.getXXService().getById(serviceId);

		if (service == null) {
			throw new Exception("service does not exist - id='" + serviceId);
		}

		List<RangerPolicy> ret = getServicePolicies(service, filter);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePolicies(" + serviceId + ") : policy-count=" + (ret == null ? 0 : ret.size()));
		}
		return ret;
	}

	public RangerPolicyList getPaginatedServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServicePolicies(" + serviceId + ")");
		}

		XXService service = daoMgr.getXXService().getById(serviceId);

		if (service == null) {
			throw new Exception("service does not exist - id='" + serviceId);
		}

		RangerPolicyList ret = getPaginatedServicePolicies(service.getName(), filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPaginatedServicePolicies(" + serviceId + ")");
		}
		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(String serviceName, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies(" + serviceName + ")");
		}

		List<RangerPolicy> ret = null;

		XXService service = daoMgr.getXXService().findByName(serviceName);
		if (service == null) {
			throw new Exception("service does not exist - name='" + serviceName);
		}

		ret = getServicePolicies(service, filter);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePolicies(" + serviceName + "): count=" + ((ret == null) ? 0 : ret.size()));
		}

		return ret;
	}

	private List<RangerPolicy> getServicePolicies(XXService service, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies()");
		}

		if (service == null) {
			throw new Exception("service does not exist");
		}

		List<RangerPolicy> ret = null;

		ServicePolicies servicePolicies = RangerServicePoliciesCache.getInstance().getServicePolicies(service.getName(), this);
		List<RangerPolicy> policies = servicePolicies != null ? servicePolicies.getPolicies() : null;

		if(policies != null && filter != null) {
			ret = new ArrayList<RangerPolicy>(policies);
			predicateUtil.applyFilter(ret, filter);
		} else {
			ret = policies;
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePolicies(): count=" + ((ret == null) ? 0 : ret.size()));
		}

		return ret;
	}

	private List<RangerPolicy> getServicePoliciesFromDb(XXService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePoliciesFromDb(" + service.getName() + ")");
		}

		RangerPolicyRetriever policyRetriever = new RangerPolicyRetriever(daoMgr);

		List<RangerPolicy> ret = policyRetriever.getServicePolicies(service);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePoliciesFromDb(" + service.getName() + "): count=" + ((ret == null) ? 0 : ret.size()));
		}

		return ret;
	}

	public RangerPolicyList getPaginatedServicePolicies(String serviceName, SearchFilter filter) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getPaginatedServicePolicies(" + serviceName + ")");
		}

		if (filter == null) {
			filter = new SearchFilter();
		}

		filter.setParam(SearchFilter.SERVICE_NAME, serviceName);

		RangerPolicyList ret = getPaginatedPolicies(filter);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getPaginatedServicePolicies(" + serviceName + "): count="
					+ ((ret == null) ? 0 : ret.getListSize()));
		}

		return ret;
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServicePolicies ret = null;

		XXService serviceDbObj = daoMgr.getXXService().findByName(serviceName);

		if (serviceDbObj == null) {
			throw new Exception("service does not exist. name=" + serviceName);
		}

		if (lastKnownVersion == null || serviceDbObj.getPolicyVersion() == null || !lastKnownVersion.equals(serviceDbObj.getPolicyVersion())) {
			ret = RangerServicePoliciesCache.getInstance().getServicePolicies(serviceName, this);
		}

		if (lastKnownVersion != null) {
			// ServicePolicies are not changed
			ret = RangerServicePoliciesCache.getInstance().getServicePolicies(serviceName, this);
		}

		if (LOG.isDebugEnabled()) {
			RangerServicePoliciesCache.getInstance().dump();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		return ret;
	}

	@Override
	public Long getServicePolicyVersion(String serviceName) {

		XXService serviceDbObj = daoMgr.getXXService().findByName(serviceName);

		return serviceDbObj != null ? serviceDbObj.getPolicyVersion() : null;
	}

	@Override
	public ServicePolicies getServicePolicies(String serviceName) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.getServicePolicies(" + serviceName  + ")");
		}

		ServicePolicies ret = null;

		XXService serviceDbObj = daoMgr.getXXService().findByName(serviceName);

		if (serviceDbObj == null) {
			throw new Exception("service does not exist. name=" + serviceName);
		}

		RangerServiceDef serviceDef = getServiceDef(serviceDbObj.getType());

		if (serviceDef == null) {
			throw new Exception("service-def does not exist. id=" + serviceDbObj.getType());
		}
		List<RangerPolicy> policies = null;

		if (serviceDbObj.getIsenabled()) {

			policies = getServicePoliciesFromDb(serviceDbObj);

		} else {
			policies = new ArrayList<RangerPolicy>();
		}

		ret = new ServicePolicies();

		ret.setServiceId(serviceDbObj.getId());
		ret.setServiceName(serviceDbObj.getName());
		ret.setPolicyVersion(serviceDbObj.getPolicyVersion());
		ret.setPolicyUpdateTime(serviceDbObj.getPolicyUpdateTime());
		ret.setPolicies(policies);
		ret.setServiceDef(serviceDef);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDBStore.getServicePolicies(" + serviceName  + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		return ret;
	}

	private void createDefaultPolicies(XXService createdService, VXUser vXUser) throws Exception {
		// we need to create one policy for each resource hierarchy
		RangerServiceDef serviceDef = getServiceDef(createdService.getType());
		RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef);
		int i = 1;
		for (List<RangerResourceDef> aHierarchy : serviceDefHelper.getResourceHierarchies()) {
			createDefaultPolicy(createdService, vXUser, aHierarchy, i);
			i++;
		}
	}

	private void createDefaultPolicy(XXService createdService, VXUser vXUser, List<RangerResourceDef> resourceHierarchy, int num) throws Exception {
		RangerPolicy policy = new RangerPolicy();
		String policyName=createdService.getName()+"-"+num+"-"+DateUtil.dateToString(DateUtil.getUTCDate(),"yyyyMMddHHmmss");

		policy.setIsEnabled(true);
		policy.setVersion(1L);
		policy.setName(policyName);
		policy.setService(createdService.getName());
		policy.setDescription("Default Policy for Service: " + createdService.getName());
		policy.setIsAuditEnabled(true);

		policy.setResources(createDefaultPolicyResource(resourceHierarchy));

		if (vXUser != null) {
			List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicyItem>();
			RangerPolicyItem policyItem = new RangerPolicyItem();

			List<String> users = new ArrayList<String>();
			users.add(vXUser.getName());
			policyItem.setUsers(users);

			List<XXAccessTypeDef> accessTypeDefs = daoMgr.getXXAccessTypeDef().findByServiceDefId(createdService.getType());
			List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicyItemAccess>();
			for(XXAccessTypeDef accessTypeDef : accessTypeDefs) {
				RangerPolicyItemAccess access = new RangerPolicyItemAccess();
				access.setType(accessTypeDef.getName());
				access.setIsAllowed(true);
				accesses.add(access);
			}
			policyItem.setAccesses(accesses);

			policyItem.setDelegateAdmin(true);
			policyItems.add(policyItem);
			policy.setPolicyItems(policyItems);
		}
		policy = createPolicy(policy);
	}

	Map<String, RangerPolicyResource> createDefaultPolicyResource(List<RangerResourceDef> resourceHierarchy) throws Exception {
		Map<String, RangerPolicyResource> resourceMap = new HashMap<String, RangerPolicyResource>();

		for (RangerResourceDef resourceDef : resourceHierarchy) {
			RangerPolicyResource polRes = new RangerPolicyResource();
			polRes.setIsExcludes(false);
			polRes.setIsRecursive(false);

			String value = "*";
			if("path".equalsIgnoreCase(resourceDef.getName())) {
				value = "/*";
			}

			if(resourceDef.getRecursiveSupported()) {
				polRes.setIsRecursive(Boolean.TRUE);
			}

			polRes.setValue(value);
			resourceMap.put(resourceDef.getName(), polRes);
		}
		return resourceMap;
	}

	private Map<String, String> validateRequiredConfigParams(RangerService service, Map<String, String> configs) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDBStore.validateRequiredConfigParams()");
		}
		if(configs == null) {
			return null;
		}

		List<XXServiceConfigDef> svcConfDefList = daoMgr.getXXServiceConfigDef()
				.findByServiceDefName(service.getType());
		for(XXServiceConfigDef svcConfDef : svcConfDefList ) {
			String confField = configs.get(svcConfDef.getName());

			if(svcConfDef.getIsMandatory() && stringUtil.isEmpty(confField)) {
				throw restErrorUtil.createRESTException(
						"Please provide value of mandatory: "+ svcConfDef.getName(),
						MessageEnums.INVALID_INPUT_DATA);
			}
		}
		Map<String, String> validConfigs = new HashMap<String, String>();
		for(Entry<String, String> config : configs.entrySet()) {
			if(!stringUtil.isEmpty(config.getValue())) {
				validConfigs.put(config.getKey(), config.getValue());
			}
		}
		return validConfigs;
	}

	private void handlePolicyUpdate(RangerService service) throws Exception {
		updatePolicyVersion(service);
	}

	private void updatePolicyVersion(RangerService service) throws Exception {
		if(service == null || service.getId() == null) {
			return;
		}
		if (true) return;

		XXServiceDao serviceDao = daoMgr.getXXService();

		XXService serviceDbObj = serviceDao.getById(service.getId());

		if(serviceDbObj == null) {
			LOG.warn("updatePolicyVersion(serviceId=" + service.getId() + "): service not found");

			return;
		}

		Long policyVersion = service.getPolicyVersion();

		if(policyVersion == null) {
			policyVersion = new Long(1);
		} else {
			policyVersion = new Long(policyVersion.longValue() + 1);
		}

		service.setPolicyVersion(policyVersion);
		service.setPolicyUpdateTime(new Date());

		serviceDbObj.setPolicyVersion(service.getPolicyVersion());
		serviceDbObj.setPolicyUpdateTime(service.getPolicyUpdateTime());

		serviceDao.update(serviceDbObj);
	}

	private void createNewPolicyItemsForPolicy(RangerPolicy policy, XXPolicy xPolicy, List<RangerPolicyItem> policyItems, XXServiceDef xServiceDef) throws Exception {

		for (int itemOrder = 0; itemOrder < policyItems.size(); itemOrder++) {
			RangerPolicyItem policyItem = policyItems.get(itemOrder);
			XXPolicyItem xPolicyItem = new XXPolicyItem();
			xPolicyItem = (XXPolicyItem) rangerAuditFields.populateAuditFields(
					xPolicyItem, xPolicy);
			xPolicyItem.setDelegateAdmin(policyItem.getDelegateAdmin());
			xPolicyItem.setPolicyId(policy.getId());
			xPolicyItem.setOrder(itemOrder);
			xPolicyItem = daoMgr.getXXPolicyItem().create(xPolicyItem);

			List<RangerPolicyItemAccess> accesses = policyItem.getAccesses();
			for (int i = 0; i < accesses.size(); i++) {
				RangerPolicyItemAccess access = accesses.get(i);

				XXAccessTypeDef xAccTypeDef = daoMgr.getXXAccessTypeDef()
						.findByNameAndServiceId(access.getType(),
								xPolicy.getService());
				if (xAccTypeDef == null) {
					throw new Exception(access.getType() + ": is not a valid access-type. policy='"+  policy.getName() + "' service='"+ policy.getService() + "'");
				}

				XXPolicyItemAccess xPolItemAcc = new XXPolicyItemAccess();
				xPolItemAcc = (XXPolicyItemAccess) rangerAuditFields.populateAuditFields(xPolItemAcc, xPolicyItem);
				xPolItemAcc.setIsAllowed(access.getIsAllowed());

				xPolItemAcc.setType(xAccTypeDef.getId());
				xPolItemAcc.setPolicyitemid(xPolicyItem.getId());
				xPolItemAcc.setOrder(i);
				xPolItemAcc = daoMgr.getXXPolicyItemAccess()
						.create(xPolItemAcc);
			}
			List<String> users = policyItem.getUsers();
			for(int i = 0; i < users.size(); i++) {
				String user = users.get(i);

				XXUser xUser = daoMgr.getXXUser().findByUserName(user);
				if(xUser == null) {
					throw new Exception(user + ": user does not exist. policy='"+  policy.getName() + "' service='"+ policy.getService() + "'");
				}
				XXPolicyItemUserPerm xUserPerm = new XXPolicyItemUserPerm();
				xUserPerm = (XXPolicyItemUserPerm) rangerAuditFields.populateAuditFields(xUserPerm, xPolicyItem);
				xUserPerm.setUserId(xUser.getId());
				xUserPerm.setPolicyItemId(xPolicyItem.getId());
				xUserPerm.setOrder(i);
				xUserPerm = daoMgr.getXXPolicyItemUserPerm().create(xUserPerm);
			}

			List<String> groups = policyItem.getGroups();
			for(int i = 0; i < groups.size(); i++) {
				String group = groups.get(i);

				XXGroup xGrp = daoMgr.getXXGroup().findByGroupName(group);
				if(xGrp == null) {
					throw new Exception(group + ": group does not exist. policy='"+  policy.getName() + "' service='"+ policy.getService() + "'");
				}
				XXPolicyItemGroupPerm xGrpPerm = new XXPolicyItemGroupPerm();
				xGrpPerm = (XXPolicyItemGroupPerm) rangerAuditFields.populateAuditFields(xGrpPerm, xPolicyItem);
				xGrpPerm.setGroupId(xGrp.getId());
				xGrpPerm.setPolicyItemId(xPolicyItem.getId());
				xGrpPerm.setOrder(i);
				xGrpPerm = daoMgr.getXXPolicyItemGroupPerm().create(xGrpPerm);
			}

			List<RangerPolicyItemCondition> conditions = policyItem.getConditions();
			for(RangerPolicyItemCondition condition : conditions) {
				XXPolicyConditionDef xPolCond = daoMgr
						.getXXPolicyConditionDef().findByServiceDefIdAndName(
								xServiceDef.getId(), condition.getType());

				if(xPolCond == null) {
					throw new Exception(condition.getType() + ": is not a valid condition-type. policy='"+  policy.getName() + "' service='"+ policy.getService() + "'");
				}

				for(int i = 0; i < condition.getValues().size(); i++) {
					String value = condition.getValues().get(i);
					XXPolicyItemCondition xPolItemCond = new XXPolicyItemCondition();
					xPolItemCond = (XXPolicyItemCondition) rangerAuditFields.populateAuditFields(xPolItemCond, xPolicyItem);
					xPolItemCond.setPolicyItemId(xPolicyItem.getId());
					xPolItemCond.setType(xPolCond.getId());
					xPolItemCond.setValue(value);
					xPolItemCond.setOrder(i);
					xPolItemCond = daoMgr.getXXPolicyItemCondition().create(xPolItemCond);
				}
			}
		}
	}

	private void createNewResourcesForPolicy(RangerPolicy policy, XXPolicy xPolicy, Map<String, RangerPolicyResource> resources) throws Exception {

		for (Entry<String, RangerPolicyResource> resource : resources.entrySet()) {
			RangerPolicyResource policyRes = resource.getValue();

			XXResourceDef xResDef = daoMgr.getXXResourceDef()
					.findByNameAndPolicyId(resource.getKey(), policy.getId());
			if (xResDef == null) {
				throw new Exception(resource.getKey() + ": is not a valid resource-type. policy='"+  policy.getName() + "' service='"+ policy.getService() + "'");
			}

			XXPolicyResource xPolRes = new XXPolicyResource();
			xPolRes = (XXPolicyResource) rangerAuditFields.populateAuditFields(xPolRes, xPolicy);

			xPolRes.setIsExcludes(policyRes.getIsExcludes());
			xPolRes.setIsRecursive(policyRes.getIsRecursive());
			xPolRes.setPolicyId(policy.getId());
			xPolRes.setResDefId(xResDef.getId());
			xPolRes = daoMgr.getXXPolicyResource().create(xPolRes);

			List<String> values = policyRes.getValues();
			if(CollectionUtils.isNotEmpty(values)){
				for(int i = 0; i < values.size(); i++) {
					if(values.get(i)!=null){
						XXPolicyResourceMap xPolResMap = new XXPolicyResourceMap();
						xPolResMap = (XXPolicyResourceMap) rangerAuditFields.populateAuditFields(xPolResMap, xPolRes);
						xPolResMap.setResourceId(xPolRes.getId());
						xPolResMap.setValue(values.get(i));
						xPolResMap.setOrder(i);
						xPolResMap = daoMgr.getXXPolicyResourceMap().create(xPolResMap);
					}
				}
			}
		}
	}

	private Boolean deleteExistingPolicyItems(RangerPolicy policy) {
		if(policy == null) {
			return false;
		}

		XXPolicyItemDao policyItemDao = daoMgr.getXXPolicyItem();
		List<XXPolicyItem> policyItems = policyItemDao.findByPolicyId(policy.getId());
		for(XXPolicyItem policyItem : policyItems) {
			Long polItemId = policyItem.getId();

			XXPolicyItemConditionDao polCondDao = daoMgr.getXXPolicyItemCondition();
			List<XXPolicyItemCondition> conditions = polCondDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemCondition condition : conditions) {
				polCondDao.remove(condition);
			}

			XXPolicyItemGroupPermDao grpPermDao = daoMgr.getXXPolicyItemGroupPerm();
			List<XXPolicyItemGroupPerm> groups = grpPermDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemGroupPerm group : groups) {
				grpPermDao.remove(group);
			}

			XXPolicyItemUserPermDao userPermDao = daoMgr.getXXPolicyItemUserPerm();
			List<XXPolicyItemUserPerm> users = userPermDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemUserPerm user : users) {
				userPermDao.remove(user);
			}

			XXPolicyItemAccessDao polItemAccDao = daoMgr.getXXPolicyItemAccess();
			List<XXPolicyItemAccess> accesses = polItemAccDao.findByPolicyItemId(polItemId);
			for(XXPolicyItemAccess access : accesses) {
				polItemAccDao.remove(access);
			}

			policyItemDao.remove(policyItem);
		}
		return true;
	}

	private Boolean deleteExistingPolicyResources(RangerPolicy policy) {
		if(policy == null) {
			return false;
		}

		List<XXPolicyResource> resources = daoMgr.getXXPolicyResource().findByPolicyId(policy.getId());

		XXPolicyResourceDao resDao = daoMgr.getXXPolicyResource();
		for(XXPolicyResource resource : resources) {
			List<XXPolicyResourceMap> resMapList = daoMgr.getXXPolicyResourceMap().findByPolicyResId(resource.getId());

			XXPolicyResourceMapDao resMapDao = daoMgr.getXXPolicyResourceMap();
			for(XXPolicyResourceMap resMap : resMapList) {
				resMapDao.remove(resMap);
			}
			resDao.remove(resource);
		}
		return true;
	}

	@Override
	public Boolean getPopulateExistingBaseFields() {
		return populateExistingBaseFields;
	}

	@Override
	public void setPopulateExistingBaseFields(Boolean populateExistingBaseFields) {
		this.populateExistingBaseFields = populateExistingBaseFields;
	}

	public RangerPolicy getPolicyFromEventTime(String eventTime, Long policyId) {

		XXDataHist xDataHist = daoMgr.getXXDataHist().findObjByEventTimeClassTypeAndId(eventTime,
				AppConstants.CLASS_TYPE_RANGER_POLICY, policyId);

		if (xDataHist == null) {
			String errMsg = "No policy history found for given time: " + eventTime;
			LOG.error(errMsg);
			throw restErrorUtil.createRESTException(errMsg, MessageEnums.DATA_NOT_FOUND);
		}

		String content = xDataHist.getContent();
		RangerPolicy policy = (RangerPolicy) dataHistService.writeJsonToJavaObject(content, RangerPolicy.class);

		return policy;
	}

	public VXString getPolicyVersionList(Long policyId) {
		List<Integer> versionList = daoMgr.getXXDataHist().getVersionListOfObject(policyId,
				AppConstants.CLASS_TYPE_RANGER_POLICY);

		VXString vXString = new VXString();
		vXString.setValue(StringUtils.join(versionList, ","));

		return vXString;
	}

	public RangerPolicy getPolicyForVersionNumber(Long policyId, int versionNo) {
		XXDataHist xDataHist = daoMgr.getXXDataHist().findObjectByVersionNumber(policyId,
				AppConstants.CLASS_TYPE_RANGER_POLICY, versionNo);

		if (xDataHist == null) {
			throw restErrorUtil.createRESTException("No Policy found for given version.", MessageEnums.DATA_NOT_FOUND);
		}

		String content = xDataHist.getContent();
		RangerPolicy policy = (RangerPolicy) dataHistService.writeJsonToJavaObject(content, RangerPolicy.class);

		return policy;
	}

	void updatePolicySignature(RangerPolicy policy) {
		RangerPolicyResourceSignature policySignature = factory.createPolicyResourceSignature(policy);
		String signature = policySignature.getSignature();
		policy.setResourceSignature(signature);
		if (LOG.isDebugEnabled()) {
			String message = String.format("Setting signature on policy id=%d, name=%s to [%s]", policy.getId(), policy.getName(), signature);
			LOG.debug(message);
		}
	}
}
