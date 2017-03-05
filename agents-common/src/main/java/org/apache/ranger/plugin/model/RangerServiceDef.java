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

package org.apache.ranger.plugin.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.map.annotate.JsonSerialize;


@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class RangerServiceDef extends RangerBaseModelObject implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private String                         name             = null;
	private String                         implClass        = null;
	private String                         label            = null;
	private String                         description      = null;
	private String                         rbKeyLabel       = null;
	private String                         rbKeyDescription = null;
	private List<RangerServiceConfigDef>   configs          = null;
	private List<RangerResourceDef>        resources        = null;
	private List<RangerAccessTypeDef>      accessTypes      = null;
	private List<RangerPolicyConditionDef> policyConditions = null;
	private List<RangerContextEnricherDef> contextEnrichers = null;
	private List<RangerEnumDef>            enums            = null;
	private Map<String, String>            configKeys       = null;

	public RangerServiceDef() {
		this(null, null, null, null, null, null, null, null, null, null, null);
	}

	/**
	 * @param name
	 * @param implClass
	 * @param label
	 * @param description
	 * @param configs
	 * @param resources
	 * @param accessTypes
	 * @param policyConditions
	 * @param contextEnrichers
	 * @param enums
	 */
	public RangerServiceDef(String name, String implClass, String label,
													String description, List<RangerServiceConfigDef> configs, List<RangerResourceDef> resources,
													List<RangerAccessTypeDef> accessTypes, List<RangerPolicyConditionDef> policyConditions,
													List<RangerContextEnricherDef> contextEnrichers, List<RangerEnumDef> enums,
													Map<String, String> configKeys) {
		super();

		setName(name);
		setImplClass(implClass);
		setLabel(label);
		setDescription(description);
		setConfigs(configs);
		setResources(resources);
		setAccessTypes(accessTypes);
		setPolicyConditions(policyConditions);
		setContextEnrichers(contextEnrichers);
		setEnums(enums);
		setConfigKeys(configKeys);
	}

	/**
	 * @param other
	 */
	public void updateFrom(RangerServiceDef other) {
		super.updateFrom(other);

		setName(other.getName());
		setImplClass(other.getImplClass());
		setLabel(other.getLabel());
		setDescription(other.getDescription());
		setConfigs(other.getConfigs());
		setResources(other.getResources());
		setAccessTypes(other.getAccessTypes());
		setPolicyConditions(other.getPolicyConditions());
		setEnums(other.getEnums());
		setConfigKeys(other.getConfigKeys());
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the implClass
	 */
	public String getImplClass() {
		return implClass;
	}

	/**
	 * @param implClass the implClass to set
	 */
	public void setImplClass(String implClass) {
		this.implClass = implClass;
	}

	/**
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * @param label the label to set
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the rbKeyLabel
	 */
	public String getRbKeyLabel() {
		return rbKeyLabel;
	}

	/**
	 * @param rbKeyLabel the rbKeyLabel to set
	 */
	public void setRbKeyLabel(String rbKeyLabel) {
		this.rbKeyLabel = rbKeyLabel;
	}

	/**
	 * @return the rbKeyDescription
	 */
	public String getRbKeyDescription() {
		return rbKeyDescription;
	}

	/**
	 * @param rbKeyDescription the rbKeyDescription to set
	 */
	public void setRbKeyDescription(String rbKeyDescription) {
		this.rbKeyDescription = rbKeyDescription;
	}

	/**
	 * @return the configs
	 */
	public List<RangerServiceConfigDef> getConfigs() {
		return configs;
	}

	/**
	 * @param configs the configs to set
	 */
	public void setConfigs(List<RangerServiceConfigDef> configs) {
		if(this.configs == null) {
			this.configs = new ArrayList<RangerServiceConfigDef>();
		} else 

		if(this.configs == configs) {
			return;
		}

		this.configs.clear();

		if(configs != null) {
			for(RangerServiceConfigDef config : configs) {
				this.configs.add(config);
			}
		}
	}

	/**
	 * @return the resources
	 */
	public List<RangerResourceDef> getResources() {
		return resources;
	}

	/**
	 * @param resources the resources to set
	 */
	public void setResources(List<RangerResourceDef> resources) {
		if(this.resources == null) {
			this.resources = new ArrayList<RangerResourceDef>();
		}

		if(this.resources == resources) {
			return;
		}

		this.resources.clear();

		if(resources != null) {
			for(RangerResourceDef resource : resources) {
				this.resources.add(resource);
			}
		}
	}

	/**
	 * @return the accessTypes
	 */
	public List<RangerAccessTypeDef> getAccessTypes() {
		return accessTypes;
	}

	/**
	 * @param accessTypes the accessTypes to set
	 */
	public void setAccessTypes(List<RangerAccessTypeDef> accessTypes) {
		if(this.accessTypes == null) {
			this.accessTypes = new ArrayList<RangerAccessTypeDef>();
		}

		if(this.accessTypes == accessTypes) {
			return;
		}

		this.accessTypes.clear();

		if(accessTypes != null) {
			for(RangerAccessTypeDef accessType : accessTypes) {
				this.accessTypes.add(accessType);
			}
		}
	}

	/**
	 * @return the policyConditions
	 */
	public List<RangerPolicyConditionDef> getPolicyConditions() {
		return policyConditions;
	}

	/**
	 * @param policyConditions the policyConditions to set
	 */
	public void setPolicyConditions(List<RangerPolicyConditionDef> policyConditions) {
		if(this.policyConditions == null) {
			this.policyConditions = new ArrayList<RangerPolicyConditionDef>();
		}

		if(this.policyConditions == policyConditions) {
			return;
		}

		this.policyConditions.clear();

		if(policyConditions != null) {
			for(RangerPolicyConditionDef policyCondition : policyConditions) {
				this.policyConditions.add(policyCondition);
			}
		}
	}

	/**
	 * @return the contextEnrichers
	 */
	public List<RangerContextEnricherDef> getContextEnrichers() {
		return contextEnrichers;
	}

	/**
	 * @param contextEnrichers the contextEnrichers to set
	 */
	public void setContextEnrichers(List<RangerContextEnricherDef> contextEnrichers) {
		if(this.contextEnrichers == null) {
			this.contextEnrichers = new ArrayList<RangerContextEnricherDef>();
		}

		if(this.contextEnrichers == contextEnrichers) {
			return;
		}

		this.contextEnrichers.clear();

		if(contextEnrichers != null) {
			for(RangerContextEnricherDef contextEnricher : contextEnrichers) {
				this.contextEnrichers.add(contextEnricher);
			}
		}
	}

	/**
	 * @return the enums
	 */
	public List<RangerEnumDef> getEnums() {
		return enums;
	}

	/**
	 * @param enums the enums to set
	 */
	public void setEnums(List<RangerEnumDef> enums) {
		if(this.enums == null) {
			this.enums = new ArrayList<RangerEnumDef>();
		}

		if(this.enums == enums) {
			return;
		}

		this.enums.clear();

		if(enums != null) {
			for(RangerEnumDef enum1 : enums) {
				this.enums.add(enum1);
			}
		}
	}

	public void setConfigKeys(Map<String, String> keys) {
		if(this.configKeys == null) {
			this.configKeys = new HashMap<>();
		}

		this.configKeys.clear();
		if(keys != null) {
			this.configKeys.putAll(keys);
		}
	}

	public Map<String, String> getConfigKeys() {
		return this.configKeys;
	}

	@Override
	public String toString( ) {
		StringBuilder sb = new StringBuilder();

		toString(sb);

		return sb.toString();
	}

	public StringBuilder toString(StringBuilder sb) {
		sb.append("RangerServiceDef={");

		super.toString(sb);

		sb.append("name={").append(name).append("} ");
		sb.append("implClass={").append(implClass).append("} ");
		sb.append("label={").append(label).append("} ");
		sb.append("description={").append(description).append("} ");
		sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
		sb.append("rbKeyDescription={").append(rbKeyDescription).append("} ");

		sb.append("configs={");
		if(configs != null) {
			for(RangerServiceConfigDef config : configs) {
				if(config != null) {
					config.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("configKeys={");
		if(configKeys != null) {
			for (String key : configKeys.keySet()) {
				sb.append(key).append("=").append(configKeys.get(key)).append(",");
			}
		}
		sb.append("} ");

		sb.append("resources={");
		if(resources != null) {
			for(RangerResourceDef resource : resources) {
				if(resource != null) {
					resource.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("accessTypes={");
		if(accessTypes != null) {
			for(RangerAccessTypeDef accessType : accessTypes) {
				if(accessType != null) {
					accessType.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("policyConditions={");
		if(policyConditions != null) {
			for(RangerPolicyConditionDef policyCondition : policyConditions) {
				if(policyCondition != null) {
					policyCondition.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("contextEnrichers={");
		if(contextEnrichers != null) {
			for(RangerContextEnricherDef contextEnricher : contextEnrichers) {
				if(contextEnricher != null) {
					contextEnricher.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("enums={");
		if(enums != null) {
			for(RangerEnumDef e : enums) {
				if(e != null) {
					e.toString(sb);
				}
			}
		}
		sb.append("} ");

		sb.append("}");

		return sb;
	}


	public static class RangerEnumDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private Long                       itemId       = null;
		private String                     name         = null;
		private List<RangerEnumElementDef> elements     = null;
		private Integer                    defaultIndex = null;


		public RangerEnumDef() {
			this(null, null, null, null);
		}

		public RangerEnumDef(Long itemId, String name, List<RangerEnumElementDef> elements, Integer defaultIndex) {
			setItemId(itemId);
			setName(name);
			setElements(elements);
			setDefaultIndex(defaultIndex);
		}

		/**
		 * @return the itemId
		 */
		public Long getItemId() {
			return itemId;
		}

		/**
		 * @param itemId the itemId to set
		 */
		public void setItemId(Long itemId) {
			this.itemId = itemId;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the elements
		 */
		public List<RangerEnumElementDef> getElements() {
			return elements;
		}

		/**
		 * @param elements the elements to set
		 */
		public void setElements(List<RangerEnumElementDef> elements) {
			if(this.elements == null) {
				this.elements = new ArrayList<RangerEnumElementDef>();
			}

			if(this.elements == elements) {
				return;
			}

			this.elements.clear();

			if(elements != null) {
				for(RangerEnumElementDef element : elements) {
					this.elements.add(element);
				}
			}
		}

		/**
		 * @return the defaultIndex
		 */
		public Integer getDefaultIndex() {
			return defaultIndex;
		}

		/**
		 * @param defaultIndex the defaultIndex to set
		 */
		public void setDefaultIndex(Integer defaultIndex) {
			this.defaultIndex = (defaultIndex != null && this.elements.size() > defaultIndex) ? defaultIndex : 0;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerEnumDef={");
			sb.append("itemId={").append(itemId).append("} ");
			sb.append("name={").append(name).append("} ");
			sb.append("elements={");
			if(elements != null) {
				for(RangerEnumElementDef element : elements) {
					if(element != null) {
						element.toString(sb);
					}
				}
			}
			sb.append("} ");
			sb.append("defaultIndex={").append(defaultIndex).append("} ");
			sb.append("}");

			return sb;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((itemId == null) ? 0 : itemId.hashCode());
			result = prime * result
					+ ((defaultIndex == null) ? 0 : defaultIndex.hashCode());
			result = prime * result
					+ ((elements == null) ? 0 : elements.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RangerEnumDef other = (RangerEnumDef) obj;
			if (itemId == null) {
				if (other.itemId != null)
					return false;
			} else if (other.itemId == null || !itemId.equals(other.itemId))
				return false;

			if (defaultIndex == null) {
				if (other.defaultIndex != null)
					return false;
			} else if (!defaultIndex.equals(other.defaultIndex))
				return false;
			if (elements == null) {
				if (other.elements != null)
					return false;
			} else if (!elements.equals(other.elements))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}
	}


	public static class RangerEnumElementDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		
		private Long   itemId     = null;
		private String name       = null;
		private String label      = null;
		private String rbKeyLabel = null;


		public RangerEnumElementDef() {
			this(null, null, null, null);
		}

		public RangerEnumElementDef(Long itemId, String name, String label, String rbKeyLabel) {
			setItemId(itemId);
			setName(name);
			setLabel(label);
			setRbKeyLabel(rbKeyLabel);
		}

		/**
		 * @return the itemId
		 */
		public Long getItemId() {
			return itemId;
		}

		/**
		 * @param itemId the itemId to set
		 */
		public void setItemId(Long itemId) {
			this.itemId = itemId;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerEnumElementDef={");
			sb.append("itemId={").append(itemId).append("} ");
			sb.append("name={").append(name).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
			sb.append("}");

			return sb;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((itemId == null) ? 0 : itemId.hashCode());
			result = prime * result + ((label == null) ? 0 : label.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result
					+ ((rbKeyLabel == null) ? 0 : rbKeyLabel.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RangerEnumElementDef other = (RangerEnumElementDef) obj;
			if (itemId == null) {
				if (other.itemId != null) {
					return false;
				}
			} else if (other.itemId == null || !itemId.equals(other.itemId)) {
				return false;
			}

			if (label == null) {
				if (other.label != null)
					return false;
			} else if (!label.equals(other.label))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (rbKeyLabel == null) {
				if (other.rbKeyLabel != null)
					return false;
			} else if (!rbKeyLabel.equals(other.rbKeyLabel))
				return false;
			return true;
		}
	}


	public static class RangerServiceConfigDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private Long    itemId            = null;
		private String  name              = null;
		private String  type              = null;
		private String  subType           = null;
		private Boolean mandatory         = null;
		private String  defaultValue      = null;
		private String  validationRegEx   = null;
		private String  validationMessage = null;
		private String  uiHint            = null;
		private String  label             = null;
		private String  description       = null;
		private String  rbKeyLabel        = null;
		private String  rbKeyDescription  = null;
		private String  rbKeyValidationMessage = null;


		public RangerServiceConfigDef() {
			this(null, null, null, null, null, null, null, null, null, null, null, null, null, null);
		}

		public RangerServiceConfigDef(Long itemId, String name, String type, String subType, Boolean mandatory, String defaultValue, String validationRegEx, String validationMessage, String uiHint, String label, String description, String rbKeyLabel, String rbKeyDescription, String rbKeyValidationMessage) {
			setItemId(itemId);
			setName(name);
			setType(type);
			setSubType(subType);
			setMandatory(mandatory);
			setDefaultValue(defaultValue);
			setValidationRegEx(validationRegEx);
			setValidationMessage(validationMessage);
			setUiHint(uiHint);
			setLabel(label);
			setDescription(description);
			setRbKeyLabel(rbKeyLabel);
			setRbKeyDescription(rbKeyDescription);
			setRbKeyValidationMessage(rbKeyValidationMessage);
		}

		/**
		 * @return the itemId
		 */
		public Long getItemId() {
			return itemId;
		}

		/**
		 * @param itemId the itemId to set
		 */
		public void setItemId(Long itemId) {
			this.itemId = itemId;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the type
		 */
		public String getType() {
			return type;
		}

		/**
		 * @param type the type to set
		 */
		public void setType(String type) {
			this.type = type;
		}

		/**
		 * @return the type
		 */
		public String getSubType() {
			return subType;
		}

		/**
		 * @param type the type to set
		 */
		public void setSubType(String subType) {
			this.subType = subType;
		}

		/**
		 * @return the mandatory
		 */
		public Boolean getMandatory() {
			return mandatory;
		}

		/**
		 * @param mandatory the mandatory to set
		 */
		public void setMandatory(Boolean mandatory) {
			this.mandatory = mandatory == null ? Boolean.FALSE : mandatory;
		}

		/**
		 * @return the defaultValue
		 */
		public String getDefaultValue() {
			return defaultValue;
		}

		/**
		 * @param defaultValue the defaultValue to set
		 */
		public void setDefaultValue(String defaultValue) {
			this.defaultValue = defaultValue;
		}

		/**
		 * @return the validationRegEx
		 */
		public String getValidationRegEx() {
			return validationRegEx;
		}

		/**
		 * @param validationRegEx the validationRegEx to set
		 */
		public void setValidationRegEx(String validationRegEx) {
			this.validationRegEx = validationRegEx;
		}

		/**
		 * @return the validationMessage
		 */
		public String getValidationMessage() {
			return validationMessage;
		}

		/**
		 * @param validationMessage the validationMessage to set
		 */
		public void setValidationMessage(String validationMessage) {
			this.validationMessage = validationMessage;
		}

		/**
		 * @return the uiHint
		 */
		public String getUiHint() {
			return uiHint;
		}

		/**
		 * @param uiHint the uiHint to set
		 */
		public void setUiHint(String uiHint) {
			this.uiHint = uiHint;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the description
		 */
		public String getDescription() {
			return description;
		}

		/**
		 * @param description the description to set
		 */
		public void setDescription(String description) {
			this.description = description;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		/**
		 * @return the rbKeyDescription
		 */
		public String getRbKeyDescription() {
			return rbKeyDescription;
		}

		/**
		 * @param rbKeyDescription the rbKeyDescription to set
		 */
		public void setRbKeyDescription(String rbKeyDescription) {
			this.rbKeyDescription = rbKeyDescription;
		}

		/**
		 * @return the rbKeyValidationMessage
		 */
		public String getRbKeyValidationMessage() {
			return rbKeyValidationMessage;
		}

		/**
		 * @param rbKeyValidationMessage the rbKeyValidationMessage to set
		 */
		public void setRbKeyValidationMessage(String rbKeyValidationMessage) {
			this.rbKeyValidationMessage = rbKeyValidationMessage;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerServiceConfigDef={");
			sb.append("itemId={").append(name).append("} ");
			sb.append("name={").append(name).append("} ");
			sb.append("type={").append(type).append("} ");
			sb.append("subType={").append(subType).append("} ");
			sb.append("mandatory={").append(mandatory).append("} ");
			sb.append("defaultValue={").append(defaultValue).append("} ");
			sb.append("validationRegEx={").append(validationRegEx).append("} ");
			sb.append("validationMessage={").append(validationMessage).append("} ");
			sb.append("uiHint={").append(uiHint).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("description={").append(description).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
			sb.append("rbKeyDescription={").append(rbKeyDescription).append("} ");
			sb.append("rbKeyValidationMessage={").append(rbKeyValidationMessage).append("} ");
			sb.append("}");

			return sb;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((defaultValue == null) ? 0 : defaultValue.hashCode());
			result = prime * result
					+ ((description == null) ? 0 : description.hashCode());
			result = prime * result + ((label == null) ? 0 : label.hashCode());
			result = prime * result
					+ ((mandatory == null) ? 0 : mandatory.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime
					* result
					+ ((rbKeyDescription == null) ? 0 : rbKeyDescription
							.hashCode());
			result = prime * result
					+ ((rbKeyLabel == null) ? 0 : rbKeyLabel.hashCode());
			result = prime
					* result
					+ ((rbKeyValidationMessage == null) ? 0
							: rbKeyValidationMessage.hashCode());
			result = prime * result
					+ ((subType == null) ? 0 : subType.hashCode());
			result = prime * result + ((type == null) ? 0 : type.hashCode());
			result = prime * result
					+ ((uiHint == null) ? 0 : uiHint.hashCode());
			result = prime
					* result
					+ ((validationMessage == null) ? 0 : validationMessage
							.hashCode());
			result = prime
					* result
					+ ((validationRegEx == null) ? 0 : validationRegEx
							.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RangerServiceConfigDef other = (RangerServiceConfigDef) obj;
			if (defaultValue == null) {
				if (other.defaultValue != null)
					return false;
			} else if (!defaultValue.equals(other.defaultValue))
				return false;
			if (description == null) {
				if (other.description != null)
					return false;
			} else if (!description.equals(other.description))
				return false;
			if (label == null) {
				if (other.label != null)
					return false;
			} else if (!label.equals(other.label))
				return false;
			if (mandatory == null) {
				if (other.mandatory != null)
					return false;
			} else if (!mandatory.equals(other.mandatory))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (rbKeyDescription == null) {
				if (other.rbKeyDescription != null)
					return false;
			} else if (!rbKeyDescription.equals(other.rbKeyDescription))
				return false;
			if (rbKeyLabel == null) {
				if (other.rbKeyLabel != null)
					return false;
			} else if (!rbKeyLabel.equals(other.rbKeyLabel))
				return false;
			if (rbKeyValidationMessage == null) {
				if (other.rbKeyValidationMessage != null)
					return false;
			} else if (!rbKeyValidationMessage
					.equals(other.rbKeyValidationMessage))
				return false;
			if (subType == null) {
				if (other.subType != null)
					return false;
			} else if (!subType.equals(other.subType))
				return false;
			if (type == null) {
				if (other.type != null)
					return false;
			} else if (!type.equals(other.type))
				return false;
			if (uiHint == null) {
				if (other.uiHint != null)
					return false;
			} else if (!uiHint.equals(other.uiHint))
				return false;
			if (validationMessage == null) {
				if (other.validationMessage != null)
					return false;
			} else if (!validationMessage.equals(other.validationMessage))
				return false;
			if (validationRegEx == null) {
				if (other.validationRegEx != null)
					return false;
			} else if (!validationRegEx.equals(other.validationRegEx))
				return false;
			return true;
		}
	}


	public static class RangerResourceDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private Long                itemId                 = null;
		private String              name                   = null;
		private String              type                   = null;
		private Integer             level                  = null;
		private String              parent                 = null;
		private Boolean             mandatory              = null;
		private Boolean             lookupSupported        = null;
		private Boolean             recursiveSupported     = null;
		private Boolean             excludesSupported      = null;
		private String              matcher                = null;
		private Map<String, String> matcherOptions         = null;
		private String              validationRegEx        = null;
		private String              validationMessage      = null;
		private String              uiHint                 = null;
		private String              label                  = null;
		private String              description            = null;
		private String              rbKeyLabel             = null;
		private String              rbKeyDescription       = null;
		private String              rbKeyValidationMessage = null;


		public RangerResourceDef() {
			this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
		}

		public RangerResourceDef(Long itemId, String name, String type, Integer level, String parent, Boolean mandatory, Boolean lookupSupported, Boolean recursiveSupported, Boolean excludesSupported, String matcher, Map<String, String> matcherOptions, String validationRegEx, String validationMessage, String uiHint, String label, String description, String rbKeyLabel, String rbKeyDescription, String rbKeyValidationMessage) {
			setItemId(itemId);
			setName(name);
			setType(type);
			setLevel(level);
			setParent(parent);
			setMandatory(mandatory);
			setLookupSupported(lookupSupported);
			setRecursiveSupported(recursiveSupported);
			setExcludesSupported(excludesSupported);
			setMatcher(matcher);
			setMatcherOptions(matcherOptions);
			setValidationRegEx(validationRegEx);
			setValidationMessage(validationMessage);
			setUiHint(uiHint);
			setLabel(label);
			setDescription(description);
			setRbKeyLabel(rbKeyLabel);
			setRbKeyDescription(rbKeyDescription);
			setRbKeyValidationMessage(rbKeyValidationMessage);
		}

		/**
		 * @return the itemId
		 */
		public Long getItemId() {
			return itemId;
		}

		/**
		 * @param itemId the itemId to set
		 */
		public void setItemId(Long itemId) {
			this.itemId = itemId;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the type
		 */
		public String getType() {
			return type;
		}

		/**
		 * @param type the type to set
		 */
		public void setType(String type) {
			this.type = type;
		}

		/**
		 * @return the level
		 */
		public Integer getLevel() {
			return level;
		}

		/**
		 * @param level the level to set
		 */
		public void setLevel(Integer level) {
			this.level = level == null ? 1 : level;
		}

		/**
		 * @return the parent
		 */
		public String getParent() {
			return parent;
		}

		/**
		 * @param parent the parent to set
		 */
		public void setParent(String parent) {
			this.parent = parent;
		}

		/**
		 * @return the mandatory
		 */
		public Boolean getMandatory() {
			return mandatory;
		}

		/**
		 * @param mandatory the mandatory to set
		 */
		public void setMandatory(Boolean mandatory) {
			this.mandatory = mandatory == null ? Boolean.FALSE : mandatory;
		}

		/**
		 * @return the lookupSupported
		 */
		public Boolean getLookupSupported() {
			return lookupSupported;
		}

		/**
		 * @param lookupSupported the lookupSupported to set
		 */
		public void setLookupSupported(Boolean lookupSupported) {
			this.lookupSupported = lookupSupported == null ? Boolean.FALSE : lookupSupported;
		}

		/**
		 * @return the recursiveSupported
		 */
		public Boolean getRecursiveSupported() {
			return recursiveSupported;
		}

		/**
		 * @param recursiveSupported the recursiveSupported to set
		 */
		public void setRecursiveSupported(Boolean recursiveSupported) {
			this.recursiveSupported = recursiveSupported == null ? Boolean.FALSE : recursiveSupported;
		}

		/**
		 * @return the excludesSupported
		 */
		public Boolean getExcludesSupported() {
			return excludesSupported;
		}

		/**
		 * @param excludesSupported the excludesSupported to set
		 */
		public void setExcludesSupported(Boolean excludesSupported) {
			this.excludesSupported = excludesSupported == null ? Boolean.FALSE : excludesSupported;
		}

		/**
		 * @return the matcher
		 */
		public String getMatcher() {
			return matcher;
		}

		/**
		 * @param matcher the matcher to set
		 */
		public void setMatcher(String matcher) {
			this.matcher = matcher;
		}

		/**
		 * @return the matcher
		 */
		public Map<String, String> getMatcherOptions() {
			return matcherOptions;
		}

		/**
		 * @param matcher the matcher to set
		 */
		public void setMatcherOptions(Map<String, String> matcherOptions) {
			this.matcherOptions = matcherOptions == null ? new HashMap<String, String>() : matcherOptions;
		}

		/**
		 * @return the validationRegEx
		 */
		public String getValidationRegEx() {
			return validationRegEx;
		}

		/**
		 * @param validationRegEx the validationRegEx to set
		 */
		public void setValidationRegEx(String validationRegEx) {
			this.validationRegEx = validationRegEx;
		}

		/**
		 * @return the validationMessage
		 */
		public String getValidationMessage() {
			return validationMessage;
		}

		/**
		 * @param validationMessage the validationMessage to set
		 */
		public void setValidationMessage(String validationMessage) {
			this.validationMessage = validationMessage;
		}

		/**
		 * @return the uiHint
		 */
		public String getUiHint() {
			return uiHint;
		}

		/**
		 * @param uiHint the uiHint to set
		 */
		public void setUiHint(String uiHint) {
			this.uiHint = uiHint;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the description
		 */
		public String getDescription() {
			return description;
		}

		/**
		 * @param description the description to set
		 */
		public void setDescription(String description) {
			this.description = description;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		/**
		 * @return the rbKeyDescription
		 */
		public String getRbKeyDescription() {
			return rbKeyDescription;
		}

		/**
		 * @param rbKeyDescription the rbKeyDescription to set
		 */
		public void setRbKeyDescription(String rbKeyDescription) {
			this.rbKeyDescription = rbKeyDescription;
		}

		/**
		 * @return the rbKeyValidationMessage
		 */
		public String getRbKeyValidationMessage() {
			return rbKeyValidationMessage;
		}

		/**
		 * @param rbKeyValidationMessage the rbKeyValidationMessage to set
		 */
		public void setRbKeyValidationMessage(String rbKeyValidationMessage) {
			this.rbKeyValidationMessage = rbKeyValidationMessage;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerResourceDef={");
			sb.append("itemId={").append(itemId).append("} ");
			sb.append("name={").append(name).append("} ");
			sb.append("type={").append(type).append("} ");
			sb.append("level={").append(level).append("} ");
			sb.append("parent={").append(parent).append("} ");
			sb.append("mandatory={").append(mandatory).append("} ");
			sb.append("lookupSupported={").append(lookupSupported).append("} ");
			sb.append("recursiveSupported={").append(recursiveSupported).append("} ");
			sb.append("excludesSupported={").append(excludesSupported).append("} ");
			sb.append("matcher={").append(matcher).append("} ");
			sb.append("matcherOptions={").append(matcherOptions).append("} ");
			sb.append("validationRegEx={").append(validationRegEx).append("} ");
			sb.append("validationMessage={").append(validationMessage).append("} ");
			sb.append("uiHint={").append(uiHint).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("description={").append(description).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
			sb.append("rbKeyDescription={").append(rbKeyDescription).append("} ");
			sb.append("rbKeyValidationMessage={").append(rbKeyValidationMessage).append("} ");
			sb.append("}");

			return sb;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((description == null) ? 0 : description.hashCode());
			result = prime
					* result
					+ ((excludesSupported == null) ? 0 : excludesSupported
							.hashCode());
			result = prime * result + ((label == null) ? 0 : label.hashCode());
			result = prime * result + ((level == null) ? 0 : level.hashCode());
			result = prime
					* result
					+ ((lookupSupported == null) ? 0 : lookupSupported
							.hashCode());
			result = prime * result
					+ ((mandatory == null) ? 0 : mandatory.hashCode());
			result = prime * result
					+ ((matcher == null) ? 0 : matcher.hashCode());
			result = prime
					* result
					+ ((matcherOptions == null) ? 0 : matcherOptions.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result
					+ ((parent == null) ? 0 : parent.hashCode());
			result = prime
					* result
					+ ((rbKeyDescription == null) ? 0 : rbKeyDescription
							.hashCode());
			result = prime * result
					+ ((rbKeyLabel == null) ? 0 : rbKeyLabel.hashCode());
			result = prime
					* result
					+ ((rbKeyValidationMessage == null) ? 0
							: rbKeyValidationMessage.hashCode());
			result = prime
					* result
					+ ((recursiveSupported == null) ? 0 : recursiveSupported
							.hashCode());
			result = prime * result + ((type == null) ? 0 : type.hashCode());
			result = prime * result
					+ ((uiHint == null) ? 0 : uiHint.hashCode());
			result = prime
					* result
					+ ((validationMessage == null) ? 0 : validationMessage
							.hashCode());
			result = prime
					* result
					+ ((validationRegEx == null) ? 0 : validationRegEx
							.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RangerResourceDef other = (RangerResourceDef) obj;
			if (description == null) {
				if (other.description != null)
					return false;
			} else if (!description.equals(other.description))
				return false;
			if (excludesSupported == null) {
				if (other.excludesSupported != null)
					return false;
			} else if (!excludesSupported.equals(other.excludesSupported))
				return false;
			if (label == null) {
				if (other.label != null)
					return false;
			} else if (!label.equals(other.label))
				return false;
			if (level == null) {
				if (other.level != null)
					return false;
			} else if (!level.equals(other.level))
				return false;
			if (lookupSupported == null) {
				if (other.lookupSupported != null)
					return false;
			} else if (!lookupSupported.equals(other.lookupSupported))
				return false;
			if (mandatory == null) {
				if (other.mandatory != null)
					return false;
			} else if (!mandatory.equals(other.mandatory))
				return false;
			if (matcher == null) {
				if (other.matcher != null)
					return false;
			} else if (!matcher.equals(other.matcher))
				return false;
			if (matcherOptions == null) {
				if (other.matcherOptions != null)
					return false;
			} else if (!matcherOptions.equals(other.matcherOptions))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (parent == null) {
				if (other.parent != null)
					return false;
			} else if (!parent.equals(other.parent))
				return false;
			if (rbKeyDescription == null) {
				if (other.rbKeyDescription != null)
					return false;
			} else if (!rbKeyDescription.equals(other.rbKeyDescription))
				return false;
			if (rbKeyLabel == null) {
				if (other.rbKeyLabel != null)
					return false;
			} else if (!rbKeyLabel.equals(other.rbKeyLabel))
				return false;
			if (rbKeyValidationMessage == null) {
				if (other.rbKeyValidationMessage != null)
					return false;
			} else if (!rbKeyValidationMessage
					.equals(other.rbKeyValidationMessage))
				return false;
			if (recursiveSupported == null) {
				if (other.recursiveSupported != null)
					return false;
			} else if (!recursiveSupported.equals(other.recursiveSupported))
				return false;
			if (type == null) {
				if (other.type != null)
					return false;
			} else if (!type.equals(other.type))
				return false;
			if (uiHint == null) {
				if (other.uiHint != null)
					return false;
			} else if (!uiHint.equals(other.uiHint))
				return false;
			if (validationMessage == null) {
				if (other.validationMessage != null)
					return false;
			} else if (!validationMessage.equals(other.validationMessage))
				return false;
			if (validationRegEx == null) {
				if (other.validationRegEx != null)
					return false;
			} else if (!validationRegEx.equals(other.validationRegEx))
				return false;
			return true;
		}
		
	}


	public static class RangerAccessTypeDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private Long               itemId        = null;
		private String             name          = null;
		private String             label         = null;
		private String             rbKeyLabel    = null;
		private Collection<String> impliedGrants = null;


		public RangerAccessTypeDef() {
			this(null, null, null, null, null);
		}

		public RangerAccessTypeDef(Long itemId, String name, String label, String rbKeyLabel, Collection<String> impliedGrants) {
			setItemId(itemId);
			setName(name);
			setLabel(label);
			setRbKeyLabel(rbKeyLabel);
			setImpliedGrants(impliedGrants);
		}

		/**
		 * @return the itemId
		 */
		public Long getItemId() {
			return itemId;
		}

		/**
		 * @param itemId the itemId to set
		 */
		public void setItemId(Long itemId) {
			this.itemId = itemId;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		/**
		 * @return the impliedGrants
		 */
		public Collection<String> getImpliedGrants() {
			return impliedGrants;
		}

		/**
		 * @param impliedGrants the impliedGrants to set
		 */
		public void setImpliedGrants(Collection<String> impliedGrants) {
			if(this.impliedGrants == null) {
				this.impliedGrants = new ArrayList<String>();
			}

			if(this.impliedGrants == impliedGrants) {
				return;
			}

			this.impliedGrants.clear();

			if(impliedGrants != null) {
				for(String impliedGrant : impliedGrants) {
					this.impliedGrants.add(impliedGrant);
				}
			}
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerAccessTypeDef={");
			sb.append("itemId={").append(itemId).append("} ");
			sb.append("name={").append(name).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");

			sb.append("impliedGrants={");
			if(impliedGrants != null) {
				for(String impliedGrant : impliedGrants) {
					if(impliedGrant != null) {
						sb.append(impliedGrant).append(" ");
					}
				}
			}
			sb.append("} ");

			sb.append("}");

			return sb;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((itemId == null) ? 0 : itemId.hashCode());
			result = prime * result
					+ ((impliedGrants == null) ? 0 : impliedGrants.hashCode());
			result = prime * result + ((label == null) ? 0 : label.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result
					+ ((rbKeyLabel == null) ? 0 : rbKeyLabel.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RangerAccessTypeDef other = (RangerAccessTypeDef) obj;
			if (itemId == null) {
				if (other.itemId != null)
					return false;
			} else if (other.itemId == null || !itemId.equals(other.itemId))
				return false;

			if (impliedGrants == null) {
				if (other.impliedGrants != null)
					return false;
			} else if (!impliedGrants.equals(other.impliedGrants))
				return false;
			if (label == null) {
				if (other.label != null)
					return false;
			} else if (!label.equals(other.label))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (rbKeyLabel == null) {
				if (other.rbKeyLabel != null)
					return false;
			} else if (!rbKeyLabel.equals(other.rbKeyLabel))
				return false;
			return true;
		}
	}


	public static class RangerPolicyConditionDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private Long                itemId                 = null;
		private String              name                   = null;
		private String              evaluator              = null;
		private Map<String, String> evaluatorOptions       = null;
		private String              validationRegEx        = null;
		private String              validationMessage      = null;
		private String              uiHint                 = null;
		private String              label                  = null;
		private String              description            = null;
		private String              rbKeyLabel             = null;
		private String              rbKeyDescription       = null;
		private String              rbKeyValidationMessage = null;


		public RangerPolicyConditionDef() {
			this(null, null, null, null, null, null, null, null, null, null, null, null);
		}

		public RangerPolicyConditionDef(Long itemId, String name, String evaluator, Map<String, String> evaluatorOptions) {
			this(itemId, name, evaluator, evaluatorOptions, null, null, null, null, null, null, null, null);
		}

		public RangerPolicyConditionDef(Long itemId, String name, String evaluator, Map<String, String> evaluatorOptions, String validationRegEx, String vaidationMessage, String uiHint, String label, String description, String rbKeyLabel, String rbKeyDescription, String rbKeyValidationMessage) {
			setItemId(itemId);
			setName(name);
			setEvaluator(evaluator);
			setEvaluatorOptions(evaluatorOptions);
			setValidationRegEx(validationRegEx);
			setValidationMessage(validationMessage);
			setUiHint(uiHint);
			setLabel(label);
			setDescription(description);
			setRbKeyLabel(rbKeyLabel);
			setRbKeyDescription(rbKeyDescription);
			setRbKeyValidationMessage(rbKeyValidationMessage);
		}

		/**
		 * @return the itemId
		 */
		public Long getItemId() {
			return itemId;
		}

		/**
		 * @param itemId the itemId to set
		 */
		public void setItemId(Long itemId) {
			this.itemId = itemId;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the evaluator
		 */
		public String getEvaluator() {
			return evaluator;
		}

		/**
		 * @param evaluator the evaluator to set
		 */
		public void setEvaluator(String evaluator) {
			this.evaluator = evaluator;
		}

		/**
		 * @return the evaluator
		 */
		public Map<String, String> getEvaluatorOptions() {
			return evaluatorOptions;
		}

		/**
		 * @param evaluator the evaluator to set
		 */
		public void setEvaluatorOptions(Map<String, String> evaluatorOptions) {
			this.evaluatorOptions = evaluatorOptions == null ? new HashMap<String, String>() : evaluatorOptions;
		}

		/**
		 * @return the validationRegEx
		 */
		public String getValidationRegEx() {
			return validationRegEx;
		}

		/**
		 * @param validationRegEx the validationRegEx to set
		 */
		public void setValidationRegEx(String validationRegEx) {
			this.validationRegEx = validationRegEx;
		}

		/**
		 * @return the validationMessage
		 */
		public String getValidationMessage() {
			return validationMessage;
		}

		/**
		 * @param validationMessage the validationMessage to set
		 */
		public void setValidationMessage(String validationMessage) {
			this.validationMessage = validationMessage;
		}

		/**
		 * @return the uiHint
		 */
		public String getUiHint() {
			return uiHint;
		}

		/**
		 * @param uiHint the uiHint to set
		 */
		public void setUiHint(String uiHint) {
			this.uiHint = uiHint;
		}

		/**
		 * @return the label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * @param label the label to set
		 */
		public void setLabel(String label) {
			this.label = label;
		}

		/**
		 * @return the description
		 */
		public String getDescription() {
			return description;
		}

		/**
		 * @param description the description to set
		 */
		public void setDescription(String description) {
			this.description = description;
		}

		/**
		 * @return the rbKeyLabel
		 */
		public String getRbKeyLabel() {
			return rbKeyLabel;
		}

		/**
		 * @param rbKeyLabel the rbKeyLabel to set
		 */
		public void setRbKeyLabel(String rbKeyLabel) {
			this.rbKeyLabel = rbKeyLabel;
		}

		/**
		 * @return the rbKeyDescription
		 */
		public String getRbKeyDescription() {
			return rbKeyDescription;
		}

		/**
		 * @param rbKeyDescription the rbKeyDescription to set
		 */
		public void setRbKeyDescription(String rbKeyDescription) {
			this.rbKeyDescription = rbKeyDescription;
		}

		/**
		 * @return the rbKeyValidationMessage
		 */
		public String getRbKeyValidationMessage() {
			return rbKeyValidationMessage;
		}

		/**
		 * @param rbKeyValidationMessage the rbKeyValidationMessage to set
		 */
		public void setRbKeyValidationMessage(String rbKeyValidationMessage) {
			this.rbKeyValidationMessage = rbKeyValidationMessage;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerPolicyConditionDef={");
			sb.append("itemId={").append(itemId).append("} ");
			sb.append("name={").append(name).append("} ");
			sb.append("evaluator={").append(evaluator).append("} ");
			sb.append("evaluatorOptions={").append(evaluatorOptions).append("} ");
			sb.append("validationRegEx={").append(validationRegEx).append("} ");
			sb.append("validationMessage={").append(validationMessage).append("} ");
			sb.append("uiHint={").append(uiHint).append("} ");
			sb.append("label={").append(label).append("} ");
			sb.append("description={").append(description).append("} ");
			sb.append("rbKeyLabel={").append(rbKeyLabel).append("} ");
			sb.append("rbKeyDescription={").append(rbKeyDescription).append("} ");
			sb.append("rbKeyValidationMessage={").append(rbKeyValidationMessage).append("} ");
			sb.append("}");

			return sb;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((itemId == null) ? 0 : itemId.hashCode());
			result = prime * result
					+ ((description == null) ? 0 : description.hashCode());
			result = prime * result
					+ ((evaluator == null) ? 0 : evaluator.hashCode());
			result = prime
					* result
					+ ((evaluatorOptions == null) ? 0 : evaluatorOptions
							.hashCode());
			result = prime * result + ((label == null) ? 0 : label.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime
					* result
					+ ((rbKeyDescription == null) ? 0 : rbKeyDescription
							.hashCode());
			result = prime * result
					+ ((rbKeyLabel == null) ? 0 : rbKeyLabel.hashCode());
			result = prime
					* result
					+ ((rbKeyValidationMessage == null) ? 0
							: rbKeyValidationMessage.hashCode());
			result = prime * result
					+ ((uiHint == null) ? 0 : uiHint.hashCode());
			result = prime
					* result
					+ ((validationMessage == null) ? 0 : validationMessage
							.hashCode());
			result = prime
					* result
					+ ((validationRegEx == null) ? 0 : validationRegEx
							.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RangerPolicyConditionDef other = (RangerPolicyConditionDef) obj;
			if (itemId == null) {
				if (other.itemId != null)
					return false;
			} else if (other.itemId != null || !itemId.equals(other.itemId)) {
				return false;
			}

			if (description == null) {
				if (other.description != null)
					return false;
			} else if (!description.equals(other.description))
				return false;
			if (evaluator == null) {
				if (other.evaluator != null)
					return false;
			} else if (!evaluator.equals(other.evaluator))
				return false;
			if (evaluatorOptions == null) {
				if (other.evaluatorOptions != null)
					return false;
			} else if (!evaluatorOptions.equals(other.evaluatorOptions))
				return false;
			if (label == null) {
				if (other.label != null)
					return false;
			} else if (!label.equals(other.label))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (rbKeyDescription == null) {
				if (other.rbKeyDescription != null)
					return false;
			} else if (!rbKeyDescription.equals(other.rbKeyDescription))
				return false;
			if (rbKeyLabel == null) {
				if (other.rbKeyLabel != null)
					return false;
			} else if (!rbKeyLabel.equals(other.rbKeyLabel))
				return false;
			if (rbKeyValidationMessage == null) {
				if (other.rbKeyValidationMessage != null)
					return false;
			} else if (!rbKeyValidationMessage
					.equals(other.rbKeyValidationMessage))
				return false;
			if (uiHint == null) {
				if (other.uiHint != null)
					return false;
			} else if (!uiHint.equals(other.uiHint))
				return false;
			if (validationMessage == null) {
				if (other.validationMessage != null)
					return false;
			} else if (!validationMessage.equals(other.validationMessage))
				return false;
			if (validationRegEx == null) {
				if (other.validationRegEx != null)
					return false;
			} else if (!validationRegEx.equals(other.validationRegEx))
				return false;
			return true;
		}
	}

	public static class RangerContextEnricherDef implements java.io.Serializable {
		private static final long serialVersionUID = 1L;

		private Long                itemId              = null;
		private String              name            = null;
		private String              enricher        = null;
		private Map<String, String> enricherOptions = null;


		public RangerContextEnricherDef() {
			this(null, null, null, null);
		}

		public RangerContextEnricherDef(Long itemId, String name, String enricher, Map<String, String> enricherOptions) {
			setItemId(itemId);
			setName(name);
			setEnricher(enricher);
			setEnricherOptions(enricherOptions);
		}

		/**
		 * @return the itemId
		 */
		public Long getItemId() {
			return itemId;
		}

		/**
		 * @param itemId the itemId to set
		 */
		public void setItemId(Long itemId) {
			this.itemId = itemId;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @param name the name to set
		 */
		public void setName(String name) {
			this.name = name;
		}

		/**
		 * @return the enricher
		 */
		public String getEnricher() {
			return enricher;
		}

		/**
		 * @param enricher the enricher to set
		 */
		public void setEnricher(String enricher) {
			this.enricher = enricher;
		}

		/**
		 * @return the evaluator
		 */
		public Map<String, String> getEnricherOptions() {
			return enricherOptions;
		}

		/**
		 * @param evaluator the evaluator to set
		 */
		public void setEnricherOptions(Map<String, String> enricherOptions) {
			this.enricherOptions = enricherOptions == null ? new HashMap<String, String>() : enricherOptions;
		}

		@Override
		public String toString( ) {
			StringBuilder sb = new StringBuilder();

			toString(sb);

			return sb.toString();
		}

		public StringBuilder toString(StringBuilder sb) {
			sb.append("RangerContextEnricherDef={");
			sb.append("itemId={").append(itemId).append("} ");
			sb.append("name={").append(name).append("} ");
			sb.append("enricher={").append(enricher).append("} ");
			sb.append("enricherOptions={").append(enricherOptions).append("} ");
			sb.append("}");

			return sb;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((itemId == null) ? 0 : itemId.hashCode());
			result = prime * result
					+ ((enricher == null) ? 0 : enricher.hashCode());
			result = prime
					* result
					+ ((enricherOptions == null) ? 0 : enricherOptions
							.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			RangerContextEnricherDef other = (RangerContextEnricherDef) obj;
			if (itemId == null) {
				if (other.itemId != null)
					return false;
			} else if (other.itemId == null || !itemId.equals(other.itemId))
				return false;

			if (enricher == null) {
				if (other.enricher != null)
					return false;
			} else if (!enricher.equals(other.enricher))
				return false;
			if (enricherOptions == null) {
				if (other.enricherOptions != null)
					return false;
			} else if (!enricherOptions.equals(other.enricherOptions))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}
	}
}
