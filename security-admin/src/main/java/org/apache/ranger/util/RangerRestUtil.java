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

 package org.apache.ranger.util;

import com.google.common.collect.HashMultimap;
import org.apache.log4j.Logger;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConfigUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.view.VXMessage;
import org.apache.ranger.view.VXPortalUser;
import org.apache.ranger.view.VXResponse;
import org.apache.shiro.config.Ini;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Component
public class RangerRestUtil {
	static final Logger logger = Logger.getLogger(RangerRestUtil.class);

	@Autowired
	StringUtil stringUtil;

	@Autowired
	RESTErrorUtil restErrorUtil;

	@Autowired
	RangerConfigUtil configUtil;

	void splitUserRoleList(Collection<String> collection) {
		Collection<String> newCollection = new ArrayList<String>();
		for (String role : collection) {
			String roles[] = role.split(",");
			for (int i = 0; i < roles.length; i++) {
				String str = roles[i];
				newCollection.add(str);
			}
		}
		collection.clear();
		collection.addAll(newCollection);
	}

	/**
	 * This method cleans up the data provided by the user for update
	 * 
	 * @param userProfile
	 * @return
	 */
	public void validateVUserProfileForUpdate(XXPortalUser gjUser,
			VXPortalUser userProfile) {

		List<VXMessage> messageList = new ArrayList<VXMessage>();

		// Email Update is allowed.
		// if (userProfile.getEmailAddress() != null
		// && !userProfile.getEmailAddress().equalsIgnoreCase(
		// gjUser.getEmailAddress())) {
		// throw restErrorUtil.createRESTException(
		// "Email address can't be updated",
		// MessageEnums.DATA_NOT_UPDATABLE, null, "emailAddress",
		// userProfile.getEmailAddress());
		// }

		// Login Id can't be changed
		if (userProfile.getLoginId() != null
				&& !gjUser.getLoginId().equalsIgnoreCase(
						userProfile.getLoginId())) {
			throw restErrorUtil.createRESTException(
					"Username can't be updated",
					MessageEnums.DATA_NOT_UPDATABLE, null, "loginId",
					userProfile.getLoginId());
		}
		// }
		userProfile.setFirstName(restErrorUtil.validateStringForUpdate(
				userProfile.getFirstName(), gjUser.getFirstName(),
				StringUtil.VALIDATION_NAME, "Invalid first name",
				MessageEnums.INVALID_INPUT_DATA, null, "firstName"));

		userProfile.setFirstName(restErrorUtil.validateStringForUpdate(
				userProfile.getFirstName(), gjUser.getFirstName(),
				StringUtil.VALIDATION_NAME, "Invalid first name",
				MessageEnums.INVALID_INPUT_DATA, null, "firstName"));
		// firstName
		if (!stringUtil.isValidName(userProfile.getFirstName())) {
			logger.info("Invalid first name." + userProfile);
			messageList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null,
					"firstName"));
		}
		// create the public screen name
		userProfile.setPublicScreenName(userProfile.getFirstName() + " "
				+ userProfile.getLastName());

		userProfile.setNotes(restErrorUtil.validateStringForUpdate(
				userProfile.getNotes(), gjUser.getNotes(),
				StringUtil.VALIDATION_NAME, "Invalid notes",
				MessageEnums.INVALID_INPUT_DATA, null, "notes"));

		// validate user roles
		if (userProfile.getUserRoleList() != null) {
			// First let's normalize it
			splitUserRoleList(userProfile.getUserRoleList());
			for (String userRole : userProfile.getUserRoleList()) {
				restErrorUtil.validateStringList(userRole,
						configUtil.getRoles(), "Invalid role", null,
						"userRoleList");
			}

		}
		if (messageList.size() > 0) {
			VXResponse gjResponse = new VXResponse();
			gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
			gjResponse.setMsgDesc("Validation failure");
			gjResponse.setMessageList(messageList);
			logger.info("Validation Error in updateUser() userProfile="
					+ userProfile + ", error=" + gjResponse);
			throw restErrorUtil.createRESTException(gjResponse);
		}

	}

	public String toSentryProviderIni(String serviceName, ServicePolicies servicePolicies) {
		StringBuffer sbIni = new StringBuffer();
		try {
			Ini ini = new Ini();
			Ini.Section groupsSection = ini.addSection("groups");
			Ini.Section rolesSection = ini.addSection("roles");
			Ini.Section usersSection = ini.addSection("users");

			// Table<groupname, rolename, Set<Provider>>
			HashMultimap<String, String> groupRoles = HashMultimap.create();
			HashMultimap<String, String> roleProvider = HashMultimap.create();
			HashMultimap<String, String> userGroups = HashMultimap.create();

			List<RangerPolicy> listPolicy = servicePolicies.getPolicies();
			for (RangerPolicy policy : listPolicy) {
				if (policy.getIsEnabled()) {
					Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap = policy.getResources();
					RangerPolicy.RangerPolicyResource dbResource = policyResourceMap.get("database");
					RangerPolicy.RangerPolicyResource tableResource = policyResourceMap.get("table");
					RangerPolicy.RangerPolicyResource columnResource = policyResourceMap.get("column");
					RangerPolicy.RangerPolicyResource udfResource = policyResourceMap.get("udf");

					List<RangerPolicy.RangerPolicyItem> policyItemList = policy.getPolicyItems();

					if ((null != dbResource && dbResource.getIsExcludes())
							|| (null != tableResource && tableResource.getIsExcludes())
							|| (null != columnResource && columnResource.getIsExcludes())
							|| (null != udfResource && udfResource.getIsExcludes())) {
						System.out.print("ERROR : Sentry provider not support Excludes!");
					}

					if (null != udfResource) {
						// UDF
						System.out.print("Not implemented.");
					} else {
						int policyItemIndex = 0;
						for (String database : dbResource.getValues())
							for (String table : tableResource.getValues())
								for (String column : columnResource.getValues()) {
									for (RangerPolicy.RangerPolicyItem policyItem : policyItemList) {
										ArrayList<String> providerList = generateProvider(serviceName, database, table, column, policyItem.getAccesses());
										String roleName = "policy" + policy.getId() + "-" + policyItemIndex++;
										List<String> groupList  = policyItem.getGroups();
										Map<String, List<String>> memberList = policyItem.getGroupMember();

										if (policyItem.getUsers() != null && !policyItem.getUsers().isEmpty()) {
											String groupName = "group" + "-" + roleName;

											if (groupList != null) {
												List<String> newGroupList = new ArrayList<>(groupList);
												newGroupList.add(groupName);
												groupList = newGroupList;
											} else {
												groupList = new ArrayList<>();
												groupList.add(groupName);
											}
											if (memberList != null) {
												Map<String,List<String>> newMemberList = new HashMap<>(memberList);
												newMemberList.put(groupName,policyItem.getUsers());
												memberList = newMemberList;
											} else {
												memberList = new HashMap<>();
												memberList.put(groupName,policyItem.getUsers());
											}
										}

										for (int groupIndex = 0; groupIndex < groupList.size(); groupIndex ++) {
											groupRoles.put(groupList.get(groupIndex), roleName);
											roleProvider.putAll(roleName, providerList);
										}

										Iterator iter = memberList.entrySet().iterator();
										while(iter.hasNext()) {
											Map.Entry<String, List<String>> entry = (Map.Entry<String, List<String>>)iter.next();
											for(String user : entry.getValue()) {
												userGroups.put(user, entry.getKey());
											}
										}
									}
								}
					}
				}
			}

			Iterator iter = groupRoles.entries().iterator();
			while(iter.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry<String, String>)iter.next();
				if (groupsSection.containsKey(entry.getKey())) {
					groupsSection.put(entry.getKey(), groupsSection.get(entry.getKey()) + ", " + entry.getValue());
				} else {
					groupsSection.put(entry.getKey(), entry.getValue());
				}
			}
			iter = roleProvider.entries().iterator();
			while(iter.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry<String, String>)iter.next();
				if (rolesSection.containsKey(entry.getKey())) {
					rolesSection.put(entry.getKey(), rolesSection.get(entry.getKey()) + ", " + entry.getValue());
				} else {
					rolesSection.put(entry.getKey(), entry.getValue());
				}
			}
			iter = userGroups.entries().iterator();
			while(iter.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry<String, String>)iter.next();
				if (usersSection.containsKey(entry.getKey())) {
					usersSection.put(entry.getKey(), usersSection.get(entry.getKey()) + ", " + entry.getValue());
				} else {
					usersSection.put(entry.getKey(), entry.getValue());
				}
			}

			iter = groupsSection.entrySet().iterator();
			sbIni.append("[groups]\n");
			while(iter.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry<String, String>)iter.next();
				sbIni.append(entry.getKey() + " = " + entry.getValue() + "\n");
			}
			iter = rolesSection.entrySet().iterator();
			sbIni.append("\n[roles]\n");
			while(iter.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry<String, String>)iter.next();
				sbIni.append(entry.getKey() + " = " + entry.getValue() + "\n");
			}
			iter = usersSection.entrySet().iterator();
			sbIni.append("\n[users]\n");
			while(iter.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry<String, String>)iter.next();
				sbIni.append(entry.getKey() + " = " + entry.getValue() + "\n");
			}
		} catch(Exception excp) {
			logger.error("toSentryProviderIni(" + serviceName + ") failed", excp);
		}

		return sbIni.toString();
	}

	ArrayList<String> generateProvider(String serviceName, String db, String table, String column,
																		 List<RangerPolicy.RangerPolicyItemAccess> policyItemAccessList) {
		ArrayList<String> providerList = new ArrayList<String>();
		try {
			for (RangerPolicy.RangerPolicyItemAccess policyItemAccess : policyItemAccessList) {
				if (true == policyItemAccess.getIsAllowed()) {
					String action = policyItemAccess.getType().equalsIgnoreCase("all")?"*":policyItemAccess.getType();
					// server=server1->db=*->table=*->Column=*->action=create
					StringBuffer sbRole = new StringBuffer();
					boolean hasCol = false, hasTbl = false;

					if (false == action.equalsIgnoreCase("*")) {
						if (action.equalsIgnoreCase("update")) {
							// adjustment 'update' to 'insert'
							action = "insert";
						}
						sbRole.insert(0, "->action=" + action);
					}
					if (false == column.equalsIgnoreCase("*")) {
						sbRole.insert(0, "->column=" + column);
						hasCol = true;
					}
					if (false == table.equalsIgnoreCase("*") || hasCol) {
						sbRole.insert(0, "->table=" + table);
						hasTbl = true;
					}
					if (false == db.equalsIgnoreCase("*") || hasTbl) {
						sbRole.insert(0, "->db=" + db);
					}
					sbRole.insert(0, "server=" + serviceName);

					if (action.equalsIgnoreCase("*")) {
						providerList.clear();
						providerList.add(sbRole.toString());
						break;
					} else {
						providerList.add(sbRole.toString());
					}
				}
			}

			providerList.add("server=" + serviceName + "->uri=*");
		} catch(Exception excp) {
			logger.error("toSentryProviderIni(" + serviceName + ") failed", excp);
		}
		return providerList;
	}

}