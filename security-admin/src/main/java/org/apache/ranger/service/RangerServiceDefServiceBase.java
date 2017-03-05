/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.SearchField.DATA_TYPE;
import org.apache.ranger.common.SearchField.SEARCH_TYPE;
import org.apache.ranger.entity.*;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.RangerServiceDefList;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class RangerServiceDefServiceBase<T extends XXServiceDefBase, V extends RangerServiceDef>
		extends RangerBaseModelService<T, V> {
	private static final Log LOG = LogFactory.getLog(RangerServiceDefServiceBase.class);

	@Autowired
	RangerAuditFields<XXDBBase> rangerAuditFields;

	@Autowired
	JSONUtil jsonUtil;
	
	@Autowired
	GUIDUtil guidUtil;
	
	public RangerServiceDefServiceBase() {
		super();
		
		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE, "obj.name", DATA_TYPE.STRING, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.SERVICE_TYPE_ID, "obj.id", DATA_TYPE.INTEGER, SEARCH_TYPE.FULL));
		searchFields.add(new SearchField(SearchFilter.IS_ENABLED, "obj.isEnabled", DATA_TYPE.BOOLEAN, SEARCH_TYPE.FULL));
		
		sortFields.add(new SortField(SearchFilter.CREATE_TIME, "obj.createTime"));
		sortFields.add(new SortField(SearchFilter.UPDATE_TIME, "obj.updateTime"));
		sortFields.add(new SortField(SearchFilter.SERVICE_TYPE_ID, "obj.id"));
		sortFields.add(new SortField(SearchFilter.SERVICE_TYPE, "obj.name"));
	}
	
	@Override
	@SuppressWarnings("unchecked")
	protected RangerServiceDef populateViewBean(XXServiceDefBase xServiceDef) {
		RangerServiceDef serviceDef = super.populateViewBean((T) xServiceDef);
		Long serviceDefId = xServiceDef.getId();

		List<XXServiceConfigDef> xConfigs = daoMgr.getXXServiceConfigDef().findByServiceDefId(serviceDefId);
		if (!stringUtil.isEmpty(xConfigs)) {
			List<RangerServiceConfigDef> configs = new ArrayList<RangerServiceConfigDef>();
			for (XXServiceConfigDef xConfig : xConfigs) {
				RangerServiceConfigDef config = populateXXToRangerServiceConfigDef(xConfig);
				configs.add(config);
			}
			serviceDef.setConfigs(configs);
		}

		List<XXResourceDef> xResources = daoMgr.getXXResourceDef().findByServiceDefId(serviceDefId);
		if (!stringUtil.isEmpty(xResources)) {
			List<RangerResourceDef> resources = new ArrayList<RangerResourceDef>();
			for (XXResourceDef xResource : xResources) {
				RangerResourceDef resource = populateXXToRangerResourceDef(xResource);
				resources.add(resource);
			}
			serviceDef.setResources(resources);
		}

		List<XXAccessTypeDef> xAccessTypes = daoMgr.getXXAccessTypeDef().findByServiceDefId(serviceDefId);
		if (!stringUtil.isEmpty(xAccessTypes)) {
			List<RangerAccessTypeDef> accessTypes = new ArrayList<RangerAccessTypeDef>();
			for (XXAccessTypeDef xAtd : xAccessTypes) {
				RangerAccessTypeDef accessType = populateXXToRangerAccessTypeDef(xAtd);
				accessTypes.add(accessType);
			}
			serviceDef.setAccessTypes(accessTypes);
		}

		List<XXPolicyConditionDef> xPolicyConditions = daoMgr.getXXPolicyConditionDef()
				.findByServiceDefId(serviceDefId);
		if (!stringUtil.isEmpty(xPolicyConditions)) {
			List<RangerPolicyConditionDef> policyConditions = new ArrayList<RangerServiceDef.RangerPolicyConditionDef>();
			for (XXPolicyConditionDef xPolicyCondDef : xPolicyConditions) {
				RangerPolicyConditionDef policyCondition = populateXXToRangerPolicyConditionDef(xPolicyCondDef);
				policyConditions.add(policyCondition);
			}
			serviceDef.setPolicyConditions(policyConditions);
		}

		List<XXContextEnricherDef> xContextEnrichers = daoMgr.getXXContextEnricherDef()
				.findByServiceDefId(serviceDefId);
		if (!stringUtil.isEmpty(xContextEnrichers)) {
			List<RangerContextEnricherDef> contextEnrichers = new ArrayList<RangerServiceDef.RangerContextEnricherDef>();
			for (XXContextEnricherDef xContextEnricherDef : xContextEnrichers) {
				RangerContextEnricherDef contextEnricher = populateXXToRangerContextEnricherDef(xContextEnricherDef);
				contextEnrichers.add(contextEnricher);
			}
			serviceDef.setContextEnrichers(contextEnrichers);
		}

		List<XXEnumDef> xEnumList = daoMgr.getXXEnumDef().findByServiceDefId(serviceDefId);
		if (!stringUtil.isEmpty(xEnumList)) {
			List<RangerEnumDef> enums = new ArrayList<RangerEnumDef>();
			for (XXEnumDef xEnum : xEnumList) {
				RangerEnumDef vEnum = populateXXToRangerEnumDef(xEnum);
				enums.add(vEnum);
			}
			serviceDef.setEnums(enums);
		}

		List<XXServiceConfigMap> xConfigMaps = daoMgr.getXXServiceConfigMap().findByServiceId(serviceDefId);
		if (!stringUtil.isEmpty(xConfigMaps)) {
			Map<String, String> configkeys = new HashMap<>();
			for (XXServiceConfigMap xConfigMap : xConfigMaps) {
				configkeys.put(xConfigMap.getConfigkey(), xConfigMap.getConfigvalue());
			}
			serviceDef.setConfigKeys(configkeys);
		}

		return serviceDef;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected XXServiceDefBase mapViewToEntityBean(RangerServiceDef vObj, XXServiceDefBase xObj, int operationContext) {
		
		String guid = (StringUtils.isEmpty(vObj.getGuid())) ? guidUtil.genGUID() : vObj.getGuid();
		
		xObj.setGuid(guid);
		xObj.setName(vObj.getName());
		xObj.setImplclassname(vObj.getImplClass());
		xObj.setLabel(vObj.getLabel());
		xObj.setDescription(vObj.getDescription());
		xObj.setRbkeylabel(vObj.getRbKeyLabel());
		xObj.setRbkeydescription(vObj.getRbKeyDescription());
		xObj.setIsEnabled(vObj.getIsEnabled());
		return xObj;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RangerServiceDef mapEntityToViewBean(RangerServiceDef vObj, XXServiceDefBase xObj) {
		vObj.setGuid(xObj.getGuid());
		vObj.setVersion(xObj.getVersion());
		vObj.setName(xObj.getName());
		vObj.setImplClass(xObj.getImplclassname());
		vObj.setLabel(xObj.getLabel());
		vObj.setDescription(xObj.getDescription());
		vObj.setRbKeyLabel(xObj.getRbkeylabel());
		vObj.setRbKeyDescription(xObj.getRbkeydescription());
		vObj.setIsEnabled(xObj.getIsEnabled());
		return vObj;
	}
	
	public XXServiceConfigDef populateRangerServiceConfigDefToXX(RangerServiceConfigDef vObj, XXServiceConfigDef xObj,
			XXServiceDef serviceDef, int operationContext) {
		if(serviceDef == null) {
			LOG.error("RangerServiceDefServiceBase.populateRangerServiceConfigDefToXX, serviceDef can not be null");
			throw restErrorUtil.createRESTException("RangerServiceDef cannot be null.", MessageEnums.DATA_NOT_FOUND);
		}
		
		xObj = (XXServiceConfigDef) rangerAuditFields.populateAuditFields(xObj, serviceDef);
		xObj.setDefid(serviceDef.getId());
		xObj.setItemId(vObj.getItemId());
		xObj.setName(vObj.getName());
		xObj.setType(vObj.getType());
		xObj.setSubtype(vObj.getSubType());
		xObj.setIsMandatory(vObj.getMandatory());
		xObj.setDefaultvalue(vObj.getDefaultValue());
		xObj.setValidationRegEx(vObj.getValidationRegEx());
		xObj.setValidationMessage(vObj.getValidationMessage());
		xObj.setUiHint(vObj.getUiHint());
		xObj.setLabel(vObj.getLabel());
		xObj.setDescription(vObj.getDescription());
		xObj.setRbkeylabel(vObj.getRbKeyLabel());
		xObj.setRbkeydescription(vObj.getRbKeyDescription());
		xObj.setRbKeyValidationMessage(vObj.getRbKeyValidationMessage());
		xObj.setOrder(AppConstants.DEFAULT_SORT_ORDER);
		return xObj;
	}

	public RangerServiceConfigDef populateXXToRangerServiceConfigDef(XXServiceConfigDef xObj) {
		RangerServiceConfigDef vObj = new RangerServiceConfigDef();
		vObj.setItemId(xObj.getItemId());
		vObj.setName(xObj.getName());
		vObj.setType(xObj.getType());
		vObj.setSubType(xObj.getSubtype());
		vObj.setMandatory(xObj.getIsMandatory());
		vObj.setDefaultValue(xObj.getDefaultvalue());
		vObj.setValidationRegEx(xObj.getValidationRegEx());
		vObj.setValidationMessage(xObj.getValidationMessage());
		vObj.setUiHint(xObj.getUiHint());
		vObj.setLabel(xObj.getLabel());
		vObj.setDescription(xObj.getDescription());
		vObj.setRbKeyLabel(xObj.getRbkeylabel());
		vObj.setRbKeyDescription(xObj.getRbkeydescription());
		vObj.setRbKeyValidationMessage(xObj.getRbKeyValidationMessage());
		return vObj;
	}
	
	public XXResourceDef populateRangerResourceDefToXX(RangerResourceDef vObj, XXResourceDef xObj,
			XXServiceDef serviceDef, int operationContext) {
		if(serviceDef == null) {
			LOG.error("RangerServiceDefServiceBase.populateRangerResourceDefToXX, serviceDef can not be null");
			throw restErrorUtil.createRESTException("RangerServiceDef cannot be null.", MessageEnums.DATA_NOT_FOUND);
		}
		
		xObj = (XXResourceDef) rangerAuditFields.populateAuditFields(xObj, serviceDef);
		xObj.setDefid(serviceDef.getId());
		xObj.setItemId(vObj.getItemId());
		xObj.setName(vObj.getName());
		xObj.setType(vObj.getType());
		xObj.setLevel(vObj.getLevel());
		xObj.setMandatory(vObj.getMandatory());
		xObj.setLookupsupported(vObj.getLookupSupported());
		xObj.setRecursivesupported(vObj.getRecursiveSupported());
		xObj.setExcludessupported(vObj.getExcludesSupported());
		xObj.setMatcher(vObj.getMatcher());
		xObj.setMatcheroptions(mapToJsonString(vObj.getMatcherOptions()));
		xObj.setValidationRegEx(vObj.getValidationRegEx());
		xObj.setValidationMessage(vObj.getValidationMessage());
		xObj.setUiHint(vObj.getUiHint());
		xObj.setLabel(vObj.getLabel());
		xObj.setDescription(vObj.getDescription());
		xObj.setRbkeylabel(vObj.getRbKeyLabel());
		xObj.setRbkeydescription(vObj.getRbKeyDescription());
		xObj.setRbKeyValidationMessage(vObj.getRbKeyValidationMessage());
		xObj.setOrder(AppConstants.DEFAULT_SORT_ORDER);
		return xObj;
	}
	
	public RangerResourceDef populateXXToRangerResourceDef(XXResourceDef xObj) {
		RangerResourceDef vObj = new RangerResourceDef();
		vObj.setItemId(xObj.getItemId());
		vObj.setName(xObj.getName());
		vObj.setType(xObj.getType());
		vObj.setLevel(xObj.getLevel());		
		vObj.setMandatory(xObj.getMandatory());
		vObj.setLookupSupported(xObj.getLookupsupported());
		vObj.setRecursiveSupported(xObj.getRecursivesupported());
		vObj.setExcludesSupported(xObj.getExcludessupported());
		vObj.setMatcher(xObj.getMatcher());
		vObj.setMatcherOptions(jsonStringToMap(xObj.getMatcheroptions()));
		vObj.setValidationRegEx(xObj.getValidationRegEx());
		vObj.setValidationMessage(xObj.getValidationMessage());
		vObj.setUiHint(xObj.getUiHint());
		vObj.setLabel(xObj.getLabel());
		vObj.setDescription(xObj.getDescription());
		vObj.setRbKeyLabel(xObj.getRbkeylabel());
		vObj.setRbKeyDescription(xObj.getRbkeydescription());
		vObj.setRbKeyValidationMessage(xObj.getRbKeyValidationMessage());
		
		XXResourceDef parent = daoMgr.getXXResourceDef().getById(xObj.getParent());
		String parentName = (parent != null) ? parent.getName() : null;
		vObj.setParent(parentName);
		
		return vObj;
	}
	
	public XXAccessTypeDef populateRangerAccessTypeDefToXX(RangerAccessTypeDef vObj, XXAccessTypeDef xObj,
			XXServiceDef serviceDef, int operationContext) {
		if(serviceDef == null) {
			LOG.error("RangerServiceDefServiceBase.populateRangerAccessTypeDefToXX, serviceDef can not be null");
			throw restErrorUtil.createRESTException("RangerServiceDef cannot be null.", MessageEnums.DATA_NOT_FOUND);
		}
		
		xObj = (XXAccessTypeDef) rangerAuditFields.populateAuditFields(xObj, serviceDef);
		xObj.setDefid(serviceDef.getId());
		xObj.setItemId(vObj.getItemId());
		xObj.setName(vObj.getName());
		xObj.setLabel(vObj.getLabel());
		xObj.setRbkeylabel(vObj.getRbKeyLabel());
		xObj.setOrder(AppConstants.DEFAULT_SORT_ORDER);
		return xObj;
	}
	
	public RangerAccessTypeDef populateXXToRangerAccessTypeDef(XXAccessTypeDef xObj) {
		RangerAccessTypeDef vObj = new RangerAccessTypeDef();
		vObj.setItemId(xObj.getItemId());
		vObj.setName(xObj.getName());
		vObj.setLabel(xObj.getLabel());
		vObj.setRbKeyLabel(xObj.getRbkeylabel());
		
		List<String> impliedGrants = daoMgr.getXXAccessTypeDefGrants().findImpliedGrantsByATDId(xObj.getId());
		vObj.setImpliedGrants(impliedGrants);
		return vObj;
	}
	
	public XXPolicyConditionDef populateRangerPolicyConditionDefToXX(RangerPolicyConditionDef vObj,
			XXPolicyConditionDef xObj, XXServiceDef serviceDef, int operationContext) {
		if(serviceDef == null) {
			LOG.error("RangerServiceDefServiceBase.populateRangerPolicyConditionDefToXX, serviceDef can not be null");
			throw restErrorUtil.createRESTException("RangerServiceDef cannot be null.", MessageEnums.DATA_NOT_FOUND);
		}
		
		xObj = (XXPolicyConditionDef) rangerAuditFields.populateAuditFields(xObj, serviceDef);
		xObj.setDefid(serviceDef.getId());
		xObj.setItemId(vObj.getItemId());
		xObj.setName(vObj.getName());
		xObj.setEvaluator(vObj.getEvaluator());
		xObj.setEvaluatoroptions(mapToJsonString(vObj.getEvaluatorOptions()));
		xObj.setValidationRegEx(vObj.getValidationRegEx());
		xObj.setValidationMessage(vObj.getValidationMessage());
		xObj.setUiHint(vObj.getUiHint());
		xObj.setLabel(vObj.getLabel());
		xObj.setDescription(vObj.getDescription());
		xObj.setRbkeylabel(vObj.getRbKeyLabel());
		xObj.setRbkeydescription(vObj.getRbKeyDescription());
		xObj.setRbKeyValidationMessage(vObj.getRbKeyValidationMessage());
		xObj.setOrder(AppConstants.DEFAULT_SORT_ORDER);
		return xObj;
	}
	
	public RangerPolicyConditionDef populateXXToRangerPolicyConditionDef(XXPolicyConditionDef xObj) {
		RangerPolicyConditionDef vObj = new RangerPolicyConditionDef();
		vObj.setItemId(xObj.getItemId());
		vObj.setName(xObj.getName());
		vObj.setEvaluator(xObj.getEvaluator());
		vObj.setEvaluatorOptions(jsonStringToMap(xObj.getEvaluatoroptions()));
		vObj.setValidationRegEx(xObj.getValidationRegEx());
		vObj.setValidationMessage(xObj.getValidationMessage());
		vObj.setUiHint(xObj.getUiHint());
		vObj.setLabel(xObj.getLabel());
		vObj.setDescription(xObj.getDescription());
		vObj.setRbKeyLabel(xObj.getRbkeylabel());
		vObj.setRbKeyDescription(xObj.getRbkeydescription());
		vObj.setRbKeyValidationMessage(xObj.getRbKeyValidationMessage());
		return vObj;
	}
	
	public XXContextEnricherDef populateRangerContextEnricherDefToXX(RangerContextEnricherDef vObj,
			XXContextEnricherDef xObj, XXServiceDef serviceDef, int operationContext) {
		if(serviceDef == null) {
			LOG.error("RangerServiceDefServiceBase.populateRangerContextEnricherDefToXX, serviceDef can not be null");
			throw restErrorUtil.createRESTException("RangerServiceDef cannot be null.", MessageEnums.DATA_NOT_FOUND);
		}

		xObj = (XXContextEnricherDef) rangerAuditFields.populateAuditFields(xObj, serviceDef);
		xObj.setDefid(serviceDef.getId());
		xObj.setItemId(vObj.getItemId());
		xObj.setName(vObj.getName());
		xObj.setEnricher(vObj.getEnricher());
		xObj.setEnricherOptions(mapToJsonString(vObj.getEnricherOptions()));
		xObj.setOrder(AppConstants.DEFAULT_SORT_ORDER);
		return xObj;
	}
	
	public RangerContextEnricherDef populateXXToRangerContextEnricherDef(XXContextEnricherDef xObj) {
		RangerContextEnricherDef vObj = new RangerContextEnricherDef();
		vObj.setItemId(xObj.getItemId());
		vObj.setName(xObj.getName());
		vObj.setEnricher(xObj.getEnricher());
		vObj.setEnricherOptions(jsonStringToMap(xObj.getEnricherOptions()));
		return vObj;
	}
	
	public XXEnumDef populateRangerEnumDefToXX(RangerEnumDef vObj, XXEnumDef xObj, XXServiceDef serviceDef,
			int operationContext) {
		if(serviceDef == null) {
			LOG.error("RangerServiceDefServiceBase.populateRangerEnumDefToXX, serviceDef can not be null");
			throw restErrorUtil.createRESTException("RangerServiceDef cannot be null.", MessageEnums.DATA_NOT_FOUND);
		}

		xObj = (XXEnumDef) rangerAuditFields.populateAuditFields(xObj, serviceDef);
		xObj.setDefid(serviceDef.getId());
		xObj.setItemId(vObj.getItemId());
		xObj.setName(vObj.getName());
		xObj.setDefaultindex(vObj.getDefaultIndex());
		return xObj;
	}
	
	public RangerEnumDef populateXXToRangerEnumDef(XXEnumDef xObj) {
		RangerEnumDef vObj = new RangerEnumDef();
		vObj.setItemId(xObj.getItemId());
		vObj.setName(xObj.getName());
		vObj.setDefaultIndex(xObj.getDefaultindex());
		
		List<XXEnumElementDef> xElements = daoMgr.getXXEnumElementDef().findByEnumDefId(xObj.getId());
		List<RangerEnumElementDef> elements = new ArrayList<RangerEnumElementDef>();
		
		for(XXEnumElementDef xEle : xElements) {
			RangerEnumElementDef element = populateXXToRangerEnumElementDef(xEle);
			elements.add(element);
		}
		vObj.setElements(elements);
		
		return vObj;
	}
	
	public XXEnumElementDef populateRangerEnumElementDefToXX(RangerEnumElementDef vObj, XXEnumElementDef xObj,
			XXEnumDef enumDef, int operationContext) {
		if(enumDef == null) {
			LOG.error("RangerServiceDefServiceBase.populateRangerEnumElementDefToXX, enumDef can not be null");
			throw restErrorUtil.createRESTException("enumDef cannot be null.", MessageEnums.DATA_NOT_FOUND);
		}

		xObj = (XXEnumElementDef) rangerAuditFields.populateAuditFields(xObj, enumDef);
		xObj.setEnumdefid(enumDef.getId());
		xObj.setItemId(vObj.getItemId());
		xObj.setName(vObj.getName());
		xObj.setLabel(vObj.getLabel());
		xObj.setRbkeylabel(vObj.getRbKeyLabel());
		xObj.setOrder(AppConstants.DEFAULT_SORT_ORDER);
		return xObj;
	}
	
	public RangerEnumElementDef populateXXToRangerEnumElementDef(XXEnumElementDef xObj) {
		RangerEnumElementDef vObj = new RangerEnumElementDef();
		vObj.setItemId(xObj.getItemId());
		vObj.setName(xObj.getName());
		vObj.setLabel(xObj.getLabel());
		vObj.setRbKeyLabel(xObj.getRbkeylabel());
		return vObj;
	}

	@SuppressWarnings("unchecked")
	public RangerServiceDefList searchRangerServiceDefs(SearchFilter searchFilter) {
		RangerServiceDefList retList = new RangerServiceDefList();
		int startIndex = searchFilter.getStartIndex();
		int pageSize = searchFilter.getMaxRows();
		searchFilter.setStartIndex(0);
		searchFilter.setMaxRows(Integer.MAX_VALUE);
		List<XXServiceDef> xSvcDefList = (List<XXServiceDef>) searchResources(searchFilter, searchFields, sortFields,
				retList);
		List<XXServiceDef> permittedServiceDefs = new ArrayList<XXServiceDef>();
		for (XXServiceDef xSvcDef : xSvcDefList) {
			if (bizUtil.hasAccess(xSvcDef, null)) {
				permittedServiceDefs.add(xSvcDef);
			}
		}
		if (permittedServiceDefs.size() > 0) {
			populatePageList(permittedServiceDefs, startIndex, pageSize, retList);
		}
		return retList;

	}

	private void populatePageList(List<XXServiceDef> xxObjList, int startIndex, int pageSize,
			RangerServiceDefList retList) {
		List<RangerServiceDef> onePageList = new ArrayList<RangerServiceDef>();

		for (int i = startIndex; i < pageSize + startIndex && i < xxObjList.size(); i++) {
			onePageList.add(populateViewBean(xxObjList.get(i)));
		}
		retList.setServiceDefs(onePageList);
		retList.setStartIndex(startIndex);
		retList.setPageSize(pageSize);
		retList.setResultSize(onePageList.size());
		retList.setTotalCount(xxObjList.size());
	}
	
	private String mapToJsonString(Map<String, String> map) {
		String ret = null;

		if(map != null) {
			try {
				ret = jsonUtil.readMapToString(map);
			} catch(Exception excp) {
				LOG.warn("mapToJsonString() failed to convert map: " + map, excp);
			}
		}

		return ret;
	}

	private Map<String, String> jsonStringToMap(String jsonStr) {
		Map<String, String> ret = null;

		if(!StringUtils.isEmpty(jsonStr)) {
			try {
				ret = jsonUtil.jsonToMap(jsonStr);
			} catch(Exception excp) {
				// fallback to earlier format: "name1=value1;name2=value2"
				for(String optionString : jsonStr.split(";")) {
					if(StringUtils.isEmpty(optionString)) {
						continue;
					}

					String[] nvArr = optionString.split("=");

					String name  = (nvArr != null && nvArr.length > 0) ? nvArr[0].trim() : null;
					String value = (nvArr != null && nvArr.length > 1) ? nvArr[1].trim() : null;

					if(StringUtils.isEmpty(name)) {
						continue;
					}

					if(ret == null) {
						ret = new HashMap<String, String>();
					}

					ret.put(name, value);
				}
			}
		}

		return ret;
	}
}
