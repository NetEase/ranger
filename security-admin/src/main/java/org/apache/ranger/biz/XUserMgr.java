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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.common.ContextUtil;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.RangerCommonEnums;
import org.apache.ranger.entity.XXGroupPermission;
import org.apache.ranger.entity.XXModuleDef;
import org.apache.ranger.entity.XXUserPermission;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.service.RangerPolicyService;
import org.apache.ranger.service.XGroupPermissionService;
import org.apache.ranger.service.XModuleDefService;
import org.apache.ranger.service.XPortalUserService;
import org.apache.ranger.service.XResourceService;
import org.apache.ranger.service.XUserPermissionService;
import org.apache.ranger.view.VXGroupPermission;
import org.apache.ranger.view.VXModuleDef;
import org.apache.ranger.view.VXUserPermission;
import org.apache.log4j.Logger;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.PropertiesUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.common.SearchCriteria;
import org.apache.ranger.common.UserSessionBase;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXAuditMapDao;
import org.apache.ranger.db.XXAuthSessionDao;
import org.apache.ranger.db.XXGroupDao;
import org.apache.ranger.db.XXGroupGroupDao;
import org.apache.ranger.db.XXGroupPermissionDao;
import org.apache.ranger.db.XXGroupUserDao;
import org.apache.ranger.db.XXPermMapDao;
import org.apache.ranger.db.XXPolicyDao;
import org.apache.ranger.db.XXPortalUserDao;
import org.apache.ranger.db.XXPortalUserRoleDao;
import org.apache.ranger.db.XXResourceDao;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXUserDao;
import org.apache.ranger.db.XXUserPermissionDao;
import org.apache.ranger.entity.XXAuditMap;
import org.apache.ranger.entity.XXAuthSession;
import org.apache.ranger.entity.XXGroup;
import org.apache.ranger.entity.XXGroupGroup;
import org.apache.ranger.entity.XXGroupUser;
import org.apache.ranger.entity.XXPermMap;
import org.apache.ranger.entity.XXPolicy;
import org.apache.ranger.entity.XXPortalUser;
import org.apache.ranger.entity.XXResource;
import org.apache.ranger.entity.XXTrxLog;
import org.apache.ranger.entity.XXUser;
import org.apache.ranger.service.XGroupService;
import org.apache.ranger.service.XUserService;
import org.apache.ranger.view.VXAuditMap;
import org.apache.ranger.view.VXAuditMapList;
import org.apache.ranger.view.VXGroup;
import org.apache.ranger.view.VXGroupGroup;
import org.apache.ranger.view.VXGroupList;
import org.apache.ranger.view.VXGroupUser;
import org.apache.ranger.view.VXGroupUserList;
import org.apache.ranger.view.VXLong;
import org.apache.ranger.view.VXPermMap;
import org.apache.ranger.view.VXPermMapList;
import org.apache.ranger.view.VXPortalUser;
import org.apache.ranger.view.VXUser;
import org.apache.ranger.view.VXUserGroupInfo;
import org.apache.ranger.view.VXUserList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletResponse;

import org.apache.ranger.view.VXResponse;
import org.apache.ranger.entity.XXPortalUserRole;
import org.apache.ranger.view.VXString;
import org.apache.ranger.view.VXStringList;
@Component
public class XUserMgr extends XUserMgrBase {

	@Autowired
	XUserService xUserService;

	@Autowired
	XGroupService xGroupService;

	@Autowired
	RangerBizUtil msBizUtil;

	@Autowired
	UserMgr userMgr;

	@Autowired
	RangerDaoManager daoManager;

	@Autowired
	RangerBizUtil xaBizUtil;

	@Autowired
	XModuleDefService xModuleDefService;

	@Autowired
	XUserPermissionService xUserPermissionService;

	@Autowired
	XGroupPermissionService xGroupPermissionService;

	@Autowired
	XPortalUserService xPortalUserService;
	
	@Autowired
	XResourceService xResourceService;

	@Autowired
	SessionMgr sessionMgr;
	
	@Autowired
	GUIDUtil guidUtil;

	@Autowired
	RangerPolicyService policyService;

	@Autowired
	ServiceDBStore svcStore;
	
	@Autowired
	RangerDaoManager daoMgr;

	static final Logger logger = Logger.getLogger(XUserMgr.class);



	public VXUser getXUserByUserName(String userName) {
		VXUser vXUser=null;
		vXUser=xUserService.getXUserByUserName(userName);
		if(vXUser!=null && !hasAccessToModule(RangerConstants.MODULE_USER_GROUPS)){
			vXUser=getMaskedVXUser(vXUser);
		}
		return vXUser;
	}

	public VXUser createXUser(VXUser vXUser) {
		checkAdminAccess();
		String userName = vXUser.getName();
		if (userName == null || "null".equalsIgnoreCase(userName)
				|| userName.trim().isEmpty()) {
			throw restErrorUtil.createRESTException(
					"Please provide a valid username.",
					MessageEnums.INVALID_INPUT_DATA);
		}

		if (vXUser.getDescription() == null) {
			setUserDesc(vXUser);
		}

		String actualPassword = vXUser.getPassword();

		VXPortalUser vXPortalUser = new VXPortalUser();
		vXPortalUser.setLoginId(userName);
		vXPortalUser.setFirstName(vXUser.getFirstName());
		if("null".equalsIgnoreCase(vXPortalUser.getFirstName())){
			vXPortalUser.setFirstName("");
		}
		vXPortalUser.setLastName(vXUser.getLastName());
		if("null".equalsIgnoreCase(vXPortalUser.getLastName())){
			vXPortalUser.setLastName("");
		}
		vXPortalUser.setEmailAddress(vXUser.getEmailAddress());
		if (vXPortalUser.getFirstName() != null
				&& vXPortalUser.getLastName() != null
				&& !vXPortalUser.getFirstName().trim().isEmpty()
				&& !vXPortalUser.getLastName().trim().isEmpty()) {
			vXPortalUser.setPublicScreenName(vXPortalUser.getFirstName() + " "
					+ vXPortalUser.getLastName());
		} else {
			vXPortalUser.setPublicScreenName(vXUser.getName());
		}
		vXPortalUser.setPassword(actualPassword);
		vXPortalUser.setUserRoleList(vXUser.getUserRoleList());
		vXPortalUser = userMgr.createDefaultAccountUser(vXPortalUser);

		VXUser createdXUser = xUserService.createResource(vXUser);

		createdXUser.setPassword(actualPassword);
		List<XXTrxLog> trxLogList = xUserService.getTransactionLog(
				createdXUser, "create");

		String hiddenPassword = PropertiesUtil.getProperty("ranger.password.hidden", "*****");
		createdXUser.setPassword(hiddenPassword);

		Collection<Long> groupIdList = vXUser.getGroupIdList();
		List<VXGroupUser> vXGroupUsers = new ArrayList<VXGroupUser>();
		if (groupIdList != null) {
			for (Long groupId : groupIdList) {
				VXGroupUser vXGroupUser = createXGroupUser(
						createdXUser.getId(), groupId);
				// trxLogList.addAll(xGroupUserService.getTransactionLog(
				// vXGroupUser, "create"));
				vXGroupUsers.add(vXGroupUser);
			}
		}
		for (VXGroupUser vXGroupUser : vXGroupUsers) {
			trxLogList.addAll(xGroupUserService.getTransactionLog(vXGroupUser,
					"create"));
		}
		//
		xaBizUtil.createTrxLog(trxLogList);

		assignPermissionToUser(vXPortalUser, true);

		return createdXUser;
	}

	public void assignPermissionToUser(VXPortalUser vXPortalUser, boolean isCreate) {
		HashMap<String, Long> moduleNameId = getAllModuleNameAndIdMap();

		for (String role : vXPortalUser.getUserRoleList()) {

			if (role.equals(RangerConstants.ROLE_USER)) {

				createOrUpdateUserPermisson(vXPortalUser, moduleNameId.get(RangerConstants.MODULE_RESOURCE_BASED_POLICIES), isCreate);
				createOrUpdateUserPermisson(vXPortalUser, moduleNameId.get(RangerConstants.MODULE_REPORTS), isCreate);
			} else if (role.equals(RangerConstants.ROLE_SYS_ADMIN)) {

				createOrUpdateUserPermisson(vXPortalUser, moduleNameId.get(RangerConstants.MODULE_REPORTS), isCreate);
				createOrUpdateUserPermisson(vXPortalUser, moduleNameId.get(RangerConstants.MODULE_RESOURCE_BASED_POLICIES), isCreate);
				createOrUpdateUserPermisson(vXPortalUser, moduleNameId.get(RangerConstants.MODULE_AUDIT), isCreate);
				createOrUpdateUserPermisson(vXPortalUser, moduleNameId.get(RangerConstants.MODULE_USER_GROUPS), isCreate);
			} else if (role.equals(RangerConstants.ROLE_KEY_ADMIN)) {

				createOrUpdateUserPermisson(vXPortalUser, moduleNameId.get(RangerConstants.MODULE_KEY_MANAGER), isCreate);
				createOrUpdateUserPermisson(vXPortalUser, moduleNameId.get(RangerConstants.MODULE_REPORTS), isCreate);
				createOrUpdateUserPermisson(vXPortalUser, moduleNameId.get(RangerConstants.MODULE_RESOURCE_BASED_POLICIES), isCreate);
			}

		}
	}

	// Insert or Updating Mapping permissions depending upon roles
	private void createOrUpdateUserPermisson(VXPortalUser portalUser, Long moduleId, boolean isCreate) {
		VXUserPermission vXUserPermission;
		XXUserPermission xUserPermission = daoManager.getXXUserPermission().findByModuleIdAndPortalUserId(portalUser.getId(), moduleId);
		if (xUserPermission == null) {
			vXUserPermission = new VXUserPermission();

			// When Creating XXUserPermission UI sends xUserId, to keep it consistent here xUserId should be used
			XXUser xUser = daoManager.getXXUser().findByPortalUserId(portalUser.getId());
			if (xUser == null) {
				logger.warn("Could not found corresponding xUser for username: [" + portalUser.getLoginId() + "], So not assigning permission to this user");
				return;
			} else {
				vXUserPermission.setUserId(xUser.getId());
			}

			vXUserPermission.setIsAllowed(RangerCommonEnums.IS_ALLOWED);
			vXUserPermission.setModuleId(moduleId);
			try {
				vXUserPermission = this.createXUserPermission(vXUserPermission);
				logger.info("Permission assigned to user: [" + vXUserPermission.getUserName() + "] For Module: [" + vXUserPermission.getModuleName() + "]");
			} catch (Exception e) {
				logger.error("Error while assigning permission to user: [" + portalUser.getLoginId() + "] for module: [" + moduleId + "]", e);
			}
		} else if (isCreate) {
			vXUserPermission = xUserPermissionService.populateViewBean(xUserPermission);
			vXUserPermission.setIsAllowed(RangerCommonEnums.IS_ALLOWED);
			vXUserPermission = this.updateXUserPermission(vXUserPermission);
			logger.info("Permission Updated for user: [" + vXUserPermission.getUserName() + "] For Module: [" + vXUserPermission.getModuleName() + "]");
		}
	}

	public HashMap<String, Long> getAllModuleNameAndIdMap() {

		List<XXModuleDef> xXModuleDefs = daoManager.getXXModuleDef().getAll();

		if (!CollectionUtils.isEmpty(xXModuleDefs)) {
			HashMap<String, Long> moduleNameAndIdMap = new HashMap<String, Long>();
			for (XXModuleDef xXModuleDef : xXModuleDefs) {
				moduleNameAndIdMap.put(xXModuleDef.getModule(), xXModuleDef.getId());
			}
			return moduleNameAndIdMap;
		}

		return null;
	}

	private VXGroupUser createXGroupUser(Long userId, Long groupId) {
		VXGroupUser vXGroupUser = new VXGroupUser();
		vXGroupUser.setParentGroupId(groupId);
		vXGroupUser.setUserId(userId);
		VXGroup vXGroup = xGroupService.readResource(groupId);
		vXGroupUser.setName(vXGroup.getName());
		vXGroupUser = xGroupUserService.createResource(vXGroupUser);

		return vXGroupUser;
	}

	public VXUser updateXUser(VXUser vXUser) {
		if (vXUser == null || vXUser.getName() == null
				|| "null".equalsIgnoreCase(vXUser.getName())
				|| vXUser.getName().trim().isEmpty()) {
			throw restErrorUtil.createRESTException("Please provide a valid "
					+ "username.", MessageEnums.INVALID_INPUT_DATA);
		}
		checkAccess(vXUser.getName());
		VXPortalUser oldUserProfile = userMgr.getUserProfileByLoginId(vXUser
				.getName());
		VXPortalUser vXPortalUser = new VXPortalUser();
		if (oldUserProfile != null && oldUserProfile.getId() != null) {
			vXPortalUser.setId(oldUserProfile.getId());
		}
		// TODO : There is a possibility that old user may not exist.

		vXPortalUser.setFirstName(vXUser.getFirstName());
		if("null".equalsIgnoreCase(vXPortalUser.getFirstName())){
			vXPortalUser.setFirstName("");
		}
		vXPortalUser.setLastName(vXUser.getLastName());
		if("null".equalsIgnoreCase(vXPortalUser.getLastName())){
			vXPortalUser.setLastName("");
		}
		vXPortalUser.setEmailAddress(vXUser.getEmailAddress());
		vXPortalUser.setLoginId(vXUser.getName());
		vXPortalUser.setStatus(vXUser.getStatus());
		vXPortalUser.setUserRoleList(vXUser.getUserRoleList());
		if (vXPortalUser.getFirstName() != null
				&& vXPortalUser.getLastName() != null
				&& !vXPortalUser.getFirstName().trim().isEmpty()
				&& !vXPortalUser.getLastName().trim().isEmpty()) {
			vXPortalUser.setPublicScreenName(vXPortalUser.getFirstName() + " "
					+ vXPortalUser.getLastName());
		} else {
			vXPortalUser.setPublicScreenName(vXUser.getName());
		}
		vXPortalUser.setUserSource(vXUser.getUserSource());
		String hiddenPasswordString = PropertiesUtil.getProperty("ranger.password.hidden", "*****");
		String password = vXUser.getPassword();
		if (oldUserProfile != null && password != null
				&& password.equals(hiddenPasswordString)) {
			vXPortalUser.setPassword(oldUserProfile.getPassword());
		}
		vXPortalUser.setPassword(password);

		Collection<Long> groupIdList = vXUser.getGroupIdList();
		XXPortalUser xXPortalUser = new XXPortalUser();
		xXPortalUser = userMgr.updateUserWithPass(vXPortalUser);
		//update permissions start
		Collection<String> roleListUpdatedProfile =new ArrayList<String>();
		if (oldUserProfile != null && oldUserProfile.getId() != null) {
			if(vXUser!=null && vXUser.getUserRoleList()!=null){
				Collection<String> roleListOldProfile = oldUserProfile.getUserRoleList();
				Collection<String> roleListNewProfile = vXUser.getUserRoleList();
				if(roleListNewProfile!=null && roleListOldProfile!=null){
					for (String role : roleListNewProfile) {
						if(role!=null && !roleListOldProfile.contains(role)){
							roleListUpdatedProfile.add(role);
						}
					}
					
				}
			}
		}
		if(roleListUpdatedProfile!=null && roleListUpdatedProfile.size()>0){
			vXPortalUser.setUserRoleList(roleListUpdatedProfile);
			List<XXUserPermission> xuserPermissionList = daoManager
					.getXXUserPermission()
					.findByUserPermissionId(vXPortalUser.getId());
			if (xuserPermissionList!=null && xuserPermissionList.size()>0){
				for (XXUserPermission xXUserPermission : xuserPermissionList) {
					if (xXUserPermission != null) {
						try {
							xUserPermissionService.deleteResource(xXUserPermission.getId());
						} catch (Exception e) {
							logger.error(e.getMessage());
						}
					}
				}
			}
			assignPermissionToUser(vXPortalUser,true);
		}
		//update permissions end
		Collection<String> roleList = new ArrayList<String>();
		if (xXPortalUser != null) {
			roleList = userMgr.getRolesForUser(xXPortalUser);
		}
		if (roleList == null || roleList.size() == 0) {
			roleList = new ArrayList<String>();
			roleList.add(RangerConstants.ROLE_USER);
		}

		// TODO I've to get the transaction log from here.
		// There is nothing to log anything in XXUser so far.
		vXUser = xUserService.updateResource(vXUser);
		vXUser.setUserRoleList(roleList);
		vXUser.setPassword(password);
		List<XXTrxLog> trxLogList = xUserService.getTransactionLog(vXUser,
				oldUserProfile, "update");
		vXUser.setPassword(hiddenPasswordString);

		Long userId = vXUser.getId();
		List<Long> groupUsersToRemove = new ArrayList<Long>();

		if (groupIdList != null) {
			SearchCriteria searchCriteria = new SearchCriteria();
			searchCriteria.addParam("xUserId", userId);
			VXGroupUserList vXGroupUserList = xGroupUserService
					.searchXGroupUsers(searchCriteria);
			List<VXGroupUser> vXGroupUsers = vXGroupUserList.getList();

			if (vXGroupUsers != null) {

				// Create
				for (Long groupId : groupIdList) {
					boolean found = false;
					for (VXGroupUser vXGroupUser : vXGroupUsers) {
						if (groupId.equals(vXGroupUser.getParentGroupId())) {
							found = true;
							break;
						}
					}
					if (!found) {
						VXGroupUser vXGroupUser = createXGroupUser(userId,
								groupId);
						trxLogList.addAll(xGroupUserService.getTransactionLog(
								vXGroupUser, "create"));
					}
				}

				// Delete
				for (VXGroupUser vXGroupUser : vXGroupUsers) {
					boolean found = false;
					for (Long groupId : groupIdList) {
						if (groupId.equals(vXGroupUser.getParentGroupId())) {
							trxLogList.addAll(xGroupUserService
									.getTransactionLog(vXGroupUser, "update"));
							found = true;
							break;
						}
					}
					if (!found) {
						// TODO I've to get the transaction log from here.
						trxLogList.addAll(xGroupUserService.getTransactionLog(
								vXGroupUser, "delete"));
						groupUsersToRemove.add(vXGroupUser.getId());
						// xGroupUserService.deleteResource(vXGroupUser.getId());
					}
				}

			} else {
				for (Long groupId : groupIdList) {
					VXGroupUser vXGroupUser = createXGroupUser(userId, groupId);
					trxLogList.addAll(xGroupUserService.getTransactionLog(
							vXGroupUser, "create"));
				}
			}
			vXUser.setGroupIdList(groupIdList);
		} else {
			logger.debug("Group id list can't be null for user. Group user "
					+ "mapping not updated for user : " + userId);
		}

		xaBizUtil.createTrxLog(trxLogList);

		for (Long groupUserId : groupUsersToRemove) {
			xGroupUserService.deleteResource(groupUserId);
		}
		
		XXServiceDao serviceDao = daoMgr.getXXService();
		serviceDao.updatePolicyVersion();

		return vXUser;
	}

	public VXUserGroupInfo createXUserGroupFromMap(
			VXUserGroupInfo vXUserGroupInfo) {
		checkAdminAccess();
		VXUserGroupInfo vxUGInfo = new VXUserGroupInfo();

		VXUser vXUser = vXUserGroupInfo.getXuserInfo();

		vXUser = xUserService.createXUserWithOutLogin(vXUser);

		vxUGInfo.setXuserInfo(vXUser);

		List<VXGroup> vxg = new ArrayList<VXGroup>();

		for (VXGroup vXGroup : vXUserGroupInfo.getXgroupInfo()) {
			VXGroup VvXGroup = xGroupService.createXGroupWithOutLogin(vXGroup);
			vxg.add(VvXGroup);
			VXGroupUser vXGroupUser = new VXGroupUser();
			vXGroupUser.setUserId(vXUser.getId());
			vXGroupUser.setName(VvXGroup.getName());
			vXGroupUser = xGroupUserService
					.createXGroupUserWithOutLogin(vXGroupUser);
		}
		VXPortalUser vXPortalUser = userMgr.getUserProfileByLoginId(vXUser
				.getName());
		if(vXPortalUser!=null){
			assignPermissionToUser(vXPortalUser, true);
		}
		vxUGInfo.setXgroupInfo(vxg);

		return vxUGInfo;
	}

	public VXUser createXUserWithOutLogin(VXUser vXUser) {
		checkAdminAccess();
		return xUserService.createXUserWithOutLogin(vXUser);
	}

	public VXGroup createXGroup(VXGroup vXGroup) {
		checkAdminAccess();
		if (vXGroup.getDescription() == null) {
			vXGroup.setDescription(vXGroup.getName());
		}

		vXGroup = xGroupService.createResource(vXGroup);
		List<XXTrxLog> trxLogList = xGroupService.getTransactionLog(vXGroup,
				"create");
		xaBizUtil.createTrxLog(trxLogList);
		return vXGroup;
	}

	public VXGroup createXGroupWithoutLogin(VXGroup vXGroup) {
		checkAdminAccess();
		return xGroupService.createXGroupWithOutLogin(vXGroup);
	}

	public VXGroupUser createXGroupUser(VXGroupUser vXGroupUser) {
		checkAdminAccess();
		vXGroupUser = xGroupUserService
				.createXGroupUserWithOutLogin(vXGroupUser);
		
		XXServiceDao serviceDao = daoMgr.getXXService();
		serviceDao.updatePolicyVersion();
		
		return vXGroupUser;
	}
	
	//修改多个组里的多个用户
	public List<VXGroupUser> createXGroupUsers(List<VXGroupUser> vXGroupUsers) {
		checkAdminAccess();
		
		//将现有的userid从group_user中删除
		for (VXGroupUser vXGroupUser : vXGroupUsers) {
			xGroupUserService.deleteByUserId(vXGroupUser.getUserId());
		}
			
		//groupname是必传参数，groupid可不传
		for (VXGroupUser vXGroupUser : vXGroupUsers) {
			XXGroup xGroup = daoManager.getXXGroup().findByGroupName(vXGroupUser.getName());
			vXGroupUser.setParentGroupId(xGroup.getId());
			vXGroupUser = xGroupUserService.createResource(vXGroupUser);
		}
		
		XXServiceDao serviceDao = daoMgr.getXXService();
		serviceDao.updatePolicyVersion();
		
		return vXGroupUsers;
	}
	
	//将用户从所属的所有组中删除
	public void deleteXGroupUsers(List<Long> users) {
		checkAdminAccess();
		
		//将现有的userid从group_user中删除
		for (Long userid : users) {
			xGroupUserService.deleteByUserId(userid);
		}

		XXServiceDao serviceDao = daoMgr.getXXService();
		serviceDao.updatePolicyVersion();
	}

	public VXUser getXUser(Long id) {
		VXUser vXUser=null;
		vXUser=xUserService.readResourceWithOutLogin(id);
		if(vXUser!=null && !hasAccessToModule(RangerConstants.MODULE_USER_GROUPS)){
			vXUser=getMaskedVXUser(vXUser);
		}
		return vXUser;
	}

	public VXGroupUser getXGroupUser(Long id) {
		return xGroupUserService.readResourceWithOutLogin(id);

	}

	public VXGroup getXGroup(Long id) {
		VXGroup vXGroup=null;
		vXGroup=xGroupService.readResourceWithOutLogin(id);
		if(vXGroup!=null && !hasAccessToModule(RangerConstants.MODULE_USER_GROUPS)){
			vXGroup=getMaskedVXGroup(vXGroup);
		}
		return vXGroup;
	}

	/**
	 * // public void createXGroupAndXUser(String groupName, String userName) {
	 * 
	 * // Long groupId; // Long userId; // XXGroup xxGroup = //
	 * appDaoManager.getXXGroup().findByGroupName(groupName); // VXGroup
	 * vxGroup; // if (xxGroup == null) { // vxGroup = new VXGroup(); //
	 * vxGroup.setName(groupName); // vxGroup.setDescription(groupName); //
	 * vxGroup.setGroupType(AppConstants.XA_GROUP_USER); //
	 * vxGroup.setPriAcctId(1l); // vxGroup.setPriGrpId(1l); // vxGroup =
	 * xGroupService.createResource(vxGroup); // groupId = vxGroup.getId(); // }
	 * else { // groupId = xxGroup.getId(); // } // XXUser xxUser =
	 * appDaoManager.getXXUser().findByUserName(userName); // VXUser vxUser; //
	 * if (xxUser == null) { // vxUser = new VXUser(); //
	 * vxUser.setName(userName); // vxUser.setDescription(userName); //
	 * vxUser.setPriGrpId(1l); // vxUser.setPriAcctId(1l); // vxUser =
	 * xUserService.createResource(vxUser); // userId = vxUser.getId(); // }
	 * else { // userId = xxUser.getId(); // } // VXGroupUser vxGroupUser = new
	 * VXGroupUser(); // vxGroupUser.setParentGroupId(groupId); //
	 * vxGroupUser.setUserId(userId); // vxGroupUser.setName(groupName); //
	 * vxGroupUser.setPriAcctId(1l); // vxGroupUser.setPriGrpId(1l); //
	 * vxGroupUser = xGroupUserService.createResource(vxGroupUser);
	 * 
	 * // }
	 */

	public void deleteXGroupAndXUser(String groupName, String userName) {
		checkAdminAccess();
		VXGroup vxGroup = xGroupService.getGroupByGroupName(groupName);
		VXUser vxUser = xUserService.getXUserByUserName(userName);
		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.addParam("xGroupId", vxGroup.getId());
		searchCriteria.addParam("xUserId", vxUser.getId());
		VXGroupUserList vxGroupUserList = xGroupUserService
				.searchXGroupUsers(searchCriteria);
		for (VXGroupUser vxGroupUser : vxGroupUserList.getList()) {
			daoManager.getXXGroupUser().remove(vxGroupUser.getId());
		}
	}

	public VXGroupList getXUserGroups(Long xUserId) {
		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.addParam("xUserId", xUserId);
		VXGroupUserList vXGroupUserList = xGroupUserService
				.searchXGroupUsers(searchCriteria);
		VXGroupList vXGroupList = new VXGroupList();
		List<VXGroup> vXGroups = new ArrayList<VXGroup>();
		if (vXGroupUserList != null) {
			List<VXGroupUser> vXGroupUsers = vXGroupUserList.getList();
			Set<Long> groupIdList = new HashSet<Long>();
			for (VXGroupUser vXGroupUser : vXGroupUsers) {
				groupIdList.add(vXGroupUser.getParentGroupId());
			}
			for (Long groupId : groupIdList) {
				VXGroup vXGroup = xGroupService.readResource(groupId);
				vXGroups.add(vXGroup);
			}
			vXGroupList.setVXGroups(vXGroups);
		} else {
			logger.debug("No groups found for user id : " + xUserId);
		}
		return vXGroupList;
	}

	public Set<String> getGroupsForUser(String userName) {
		Set<String> ret = new HashSet<String>();

		try {
			VXUser user = getXUserByUserName(userName);

			if (user != null) {
				VXGroupList groups = getXUserGroups(user.getId());

				if (groups != null
						&& !CollectionUtils.isEmpty(groups.getList())) {
					for (VXGroup group : groups.getList()) {
						ret.add(group.getName());
					}
				} else {
					if (logger.isDebugEnabled()) {
						logger.debug("getGroupsForUser('" + userName
								+ "'): no groups found for user");
					}
				}
			} else {
				if (logger.isDebugEnabled()) {
					logger.debug("getGroupsForUser('" + userName
							+ "'): user not found");
				}
			}
		} catch (Exception excp) {
			logger.error("getGroupsForUser('" + userName + "') failed", excp);
		}

		return ret;
	}

	public VXUserList getXGroupUsers(Long xGroupId) {
		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.addParam("xGroupId", xGroupId);
		VXGroupUserList vXGroupUserList = xGroupUserService
				.searchXGroupUsers(searchCriteria);
		VXUserList vXUserList = new VXUserList();

		List<VXUser> vXUsers = new ArrayList<VXUser>();
		if (vXGroupUserList != null) {
			List<VXGroupUser> vXGroupUsers = vXGroupUserList.getList();
			Set<Long> userIdList = new HashSet<Long>();
			for (VXGroupUser vXGroupUser : vXGroupUsers) {
				userIdList.add(vXGroupUser.getUserId());
			}
			for (Long userId : userIdList) {
				VXUser vXUser = xUserService.readResource(userId);
				vXUsers.add(vXUser);

			}
			vXUserList.setVXUsers(vXUsers);
		} else {
			logger.debug("No users found for group id : " + xGroupId);
		}
		return vXUserList;
	}

	// FIXME Hack : Unnecessary, to be removed after discussion.
	private void setUserDesc(VXUser vXUser) {
		vXUser.setDescription(vXUser.getName());
	}

	@Override
	public VXGroup updateXGroup(VXGroup vXGroup) {
		checkAdminAccess();
		XXGroup xGroup = daoManager.getXXGroup().getById(vXGroup.getId());
		List<XXTrxLog> trxLogList = xGroupService.getTransactionLog(vXGroup,
				xGroup, "update");
		xaBizUtil.createTrxLog(trxLogList);
		vXGroup = (VXGroup) xGroupService.updateResource(vXGroup);
		return vXGroup;
	}
	public VXGroupUser updateXGroupUser(VXGroupUser vXGroupUser) {
		checkAdminAccess();
		
		XXServiceDao serviceDao = daoMgr.getXXService();
		serviceDao.updatePolicyVersion();
		
		return super.updateXGroupUser(vXGroupUser);
	}

	public void deleteXGroupUser(Long id, boolean force) {
		checkAdminAccess();
		super.deleteXGroupUser(id, force);
		
		XXServiceDao serviceDao = daoMgr.getXXService();
		serviceDao.updatePolicyVersion();
	}

	public VXGroupGroup createXGroupGroup(VXGroupGroup vXGroupGroup){
		checkAdminAccess();
		return super.createXGroupGroup(vXGroupGroup);
	}

	public VXGroupGroup updateXGroupGroup(VXGroupGroup vXGroupGroup) {
		checkAdminAccess();
		return super.updateXGroupGroup(vXGroupGroup);
	}

	public void deleteXGroupGroup(Long id, boolean force) {
		checkAdminAccess();
		super.deleteXGroupGroup(id, force);
	}

	public void deleteXPermMap(Long id, boolean force) {
		if (force) {
			XXPermMap xPermMap = daoManager.getXXPermMap().getById(id);
			if (xPermMap != null) {
				if (xResourceService.readResource(xPermMap.getResourceId()) == null) {
					throw restErrorUtil.createRESTException("Invalid Input Data - No resource found with Id: " + xPermMap.getResourceId(), MessageEnums.INVALID_INPUT_DATA);
				}
			}

			xPermMapService.deleteResource(id);
		} else {
			throw restErrorUtil.createRESTException("serverMsg.modelMgrBaseDeleteModel", MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		}
	}

	public VXLong getXPermMapSearchCount(SearchCriteria searchCriteria) {
		VXPermMapList permMapList = xPermMapService.searchXPermMaps(searchCriteria);
		VXLong vXLong = new VXLong();
		vXLong.setValue(permMapList.getListSize());
		return vXLong;
	}

	public void deleteXAuditMap(Long id, boolean force) {
		if (force) {
			XXAuditMap xAuditMap = daoManager.getXXAuditMap().getById(id);
			if (xAuditMap != null) {
				if (xResourceService.readResource(xAuditMap.getResourceId()) == null) {
					throw restErrorUtil.createRESTException("Invalid Input Data - No resource found with Id: " + xAuditMap.getResourceId(), MessageEnums.INVALID_INPUT_DATA);
				}
			}

			xAuditMapService.deleteResource(id);
		} else {
			throw restErrorUtil.createRESTException("serverMsg.modelMgrBaseDeleteModel", MessageEnums.OPER_NOT_ALLOWED_FOR_ENTITY);
		}
	}

	public VXLong getXAuditMapSearchCount(SearchCriteria searchCriteria) {
		VXAuditMapList auditMapList = xAuditMapService.searchXAuditMaps(searchCriteria);
		VXLong vXLong = new VXLong();
		vXLong.setValue(auditMapList.getListSize());
		return vXLong;
	}

	public void modifyUserVisibility(HashMap<Long, Integer> visibilityMap) {
		checkAdminAccess();
		Set<Map.Entry<Long, Integer>> entries = visibilityMap.entrySet();
		for (Map.Entry<Long, Integer> entry : entries) {
			XXUser xUser = daoManager.getXXUser().getById(entry.getKey());
			VXUser vObj = xUserService.populateViewBean(xUser);
			vObj.setIsVisible(entry.getValue());
			vObj = xUserService.updateResource(vObj);
		}
	}

	public void modifyGroupsVisibility(HashMap<Long, Integer> groupVisibilityMap) {
		checkAdminAccess();
		Set<Map.Entry<Long, Integer>> entries = groupVisibilityMap.entrySet();
		for (Map.Entry<Long, Integer> entry : entries) {
			XXGroup xGroup = daoManager.getXXGroup().getById(entry.getKey());
			VXGroup vObj = xGroupService.populateViewBean(xGroup);
			vObj.setIsVisible(entry.getValue());
			vObj = xGroupService.updateResource(vObj);
		}
	}

	// Module permissions
	public VXModuleDef createXModuleDefPermission(VXModuleDef vXModuleDef) {

		XXModuleDef xModDef = daoManager.getXXModuleDef().findByModuleName(vXModuleDef.getModule());

		if (xModDef != null) {
			throw restErrorUtil.createRESTException("Module Def with same name already exists.", MessageEnums.ERROR_DUPLICATE_OBJECT);
		}

		return xModuleDefService.createResource(vXModuleDef);
	}

	public VXModuleDef getXModuleDefPermission(Long id) {
		return xModuleDefService.readResource(id);
	}

	public VXModuleDef updateXModuleDefPermission(VXModuleDef vXModuleDef) {

		List<VXGroupPermission> groupPermListNew = vXModuleDef.getGroupPermList();
		List<VXUserPermission> userPermListNew = vXModuleDef.getUserPermList();

		List<VXGroupPermission> groupPermListOld = new ArrayList<VXGroupPermission>();
		List<VXUserPermission> userPermListOld = new ArrayList<VXUserPermission>();

		XXModuleDef xModuleDef = daoManager.getXXModuleDef().getById(vXModuleDef.getId());
		VXModuleDef vModuleDefPopulateOld = xModuleDefService.populateViewBean(xModuleDef);

		List<XXGroupPermission> xgroupPermissionList = daoManager.getXXGroupPermission().findByModuleId(vXModuleDef.getId(), true);

		for (XXGroupPermission xGrpPerm : xgroupPermissionList) {
			VXGroupPermission vXGrpPerm = xGroupPermissionService.populateViewBean(xGrpPerm);
			groupPermListOld.add(vXGrpPerm);
		}
		vModuleDefPopulateOld.setGroupPermList(groupPermListOld);

		List<XXUserPermission> xuserPermissionList = daoManager.getXXUserPermission().findByModuleId(vXModuleDef.getId(), true);

		for (XXUserPermission xUserPerm : xuserPermissionList) {
			VXUserPermission vUserPerm = xUserPermissionService.populateViewBean(xUserPerm);
			userPermListOld.add(vUserPerm);
		}
		vModuleDefPopulateOld.setUserPermList(userPermListOld);

		if (groupPermListOld != null && groupPermListNew != null) {
			for (VXGroupPermission newVXGroupPerm : groupPermListNew) {

				boolean isExist = false;

				for (VXGroupPermission oldVXGroupPerm : groupPermListOld) {
					if (newVXGroupPerm.getModuleId().equals(oldVXGroupPerm.getModuleId()) && newVXGroupPerm.getGroupId().equals(oldVXGroupPerm.getGroupId())) {
						if (!newVXGroupPerm.getIsAllowed().equals(oldVXGroupPerm.getIsAllowed())) {
							oldVXGroupPerm.setIsAllowed(newVXGroupPerm.getIsAllowed());
							oldVXGroupPerm = this.updateXGroupPermission(oldVXGroupPerm);
						}
						isExist = true;
					}
				}
				if (!isExist) {
					newVXGroupPerm = this.createXGroupPermission(newVXGroupPerm);
				}
			}
		}

		if (userPermListOld != null && userPermListNew != null) {
			for (VXUserPermission newVXUserPerm : userPermListNew) {

				boolean isExist = false;
				for (VXUserPermission oldVXUserPerm : userPermListOld) {
					if (newVXUserPerm.getModuleId().equals(oldVXUserPerm.getModuleId()) && newVXUserPerm.getUserId().equals(oldVXUserPerm.getUserId())) {
						if (!newVXUserPerm.getIsAllowed().equals(oldVXUserPerm.getIsAllowed())) {
							oldVXUserPerm.setIsAllowed(newVXUserPerm.getIsAllowed());
							oldVXUserPerm = this.updateXUserPermission(oldVXUserPerm);
						}
						isExist = true;
					}
				}
				if (!isExist) {
					newVXUserPerm = this.createXUserPermission(newVXUserPerm);
				}
			}
		}
		vXModuleDef = xModuleDefService.updateResource(vXModuleDef);

		return vXModuleDef;
	}

	public void deleteXModuleDefPermission(Long id, boolean force) {
		xModuleDefService.deleteResource(id);
	}

	// User permission
	public VXUserPermission createXUserPermission(VXUserPermission vXUserPermission) {

		vXUserPermission = xUserPermissionService.createResource(vXUserPermission);

		Set<UserSessionBase> userSessions = sessionMgr.getActiveUserSessionsForPortalUserId(vXUserPermission.getUserId());
		if (!CollectionUtils.isEmpty(userSessions)) {
			for (UserSessionBase userSession : userSessions) {
				logger.info("Assigning permission to user who's found logged in into system, so updating permission in session of that user: [" + vXUserPermission.getUserName()
						+ "]");
				sessionMgr.resetUserModulePermission(userSession);
			}
		}

		return vXUserPermission;
	}

	public VXUserPermission getXUserPermission(Long id) {
		return xUserPermissionService.readResource(id);
	}

	public VXUserPermission updateXUserPermission(VXUserPermission vXUserPermission) {

		vXUserPermission = xUserPermissionService.updateResource(vXUserPermission);

		Set<UserSessionBase> userSessions = sessionMgr.getActiveUserSessionsForPortalUserId(vXUserPermission.getUserId());
		if (!CollectionUtils.isEmpty(userSessions)) {
			for (UserSessionBase userSession : userSessions) {
				logger.info("Updating permission of user who's found logged in into system, so updating permission in session of user: [" + vXUserPermission.getUserName() + "]");
				sessionMgr.resetUserModulePermission(userSession);
			}
		}

		return vXUserPermission;
	}

	public void deleteXUserPermission(Long id, boolean force) {

		XXUserPermission xUserPermission = daoManager.getXXUserPermission().getById(id);
		if (xUserPermission == null) {
			throw restErrorUtil.createRESTException("No UserPermission found to delete, ID: " + id, MessageEnums.DATA_NOT_FOUND);
		}

		xUserPermissionService.deleteResource(id);

		Set<UserSessionBase> userSessions = sessionMgr.getActiveUserSessionsForPortalUserId(xUserPermission.getUserId());
		if (!CollectionUtils.isEmpty(userSessions)) {
			for (UserSessionBase userSession : userSessions) {
				logger.info("deleting permission of user who's found logged in into system, so updating permission in session of that user");
				sessionMgr.resetUserModulePermission(userSession);
			}
		}
	}

	// Group permission
	public VXGroupPermission createXGroupPermission(VXGroupPermission vXGroupPermission) {

		vXGroupPermission = xGroupPermissionService.createResource(vXGroupPermission);

		List<XXGroupUser> grpUsers = daoManager.getXXGroupUser().findByGroupId(vXGroupPermission.getGroupId());
		for (XXGroupUser xGrpUser : grpUsers) {
			Set<UserSessionBase> userSessions = sessionMgr.getActiveUserSessionsForXUserId(xGrpUser.getUserId());
			if (!CollectionUtils.isEmpty(userSessions)) {
				for (UserSessionBase userSession : userSessions) {
					logger.info("Assigning permission to group, one of the user belongs to that group found logged in into system, so updating permission in session of that user");
					sessionMgr.resetUserModulePermission(userSession);
				}
			}
		}

		return vXGroupPermission;
	}

	public VXGroupPermission getXGroupPermission(Long id) {
		return xGroupPermissionService.readResource(id);
	}

	public VXGroupPermission updateXGroupPermission(VXGroupPermission vXGroupPermission) {

		vXGroupPermission = xGroupPermissionService.updateResource(vXGroupPermission);

		List<XXGroupUser> grpUsers = daoManager.getXXGroupUser().findByGroupId(vXGroupPermission.getGroupId());
		for (XXGroupUser xGrpUser : grpUsers) {
			Set<UserSessionBase> userSessions = sessionMgr.getActiveUserSessionsForXUserId(xGrpUser.getUserId());
			if (!CollectionUtils.isEmpty(userSessions)) {
				for (UserSessionBase userSession : userSessions) {
					logger.info("Assigning permission to group whose one of the user found logged in into system, so updating permission in session of that user");
					sessionMgr.resetUserModulePermission(userSession);
				}
			}
		}

		return vXGroupPermission;
	}

	public void deleteXGroupPermission(Long id, boolean force) {

		XXGroupPermission xGrpPerm = daoManager.getXXGroupPermission().getById(id);

		if (xGrpPerm == null) {
			throw restErrorUtil.createRESTException("No GroupPermission object with ID: [" + id + "found.", MessageEnums.DATA_NOT_FOUND);
		}

		xGroupPermissionService.deleteResource(id);

		List<XXGroupUser> grpUsers = daoManager.getXXGroupUser().findByGroupId(xGrpPerm.getGroupId());
		for (XXGroupUser xGrpUser : grpUsers) {
			Set<UserSessionBase> userSessions = sessionMgr.getActiveUserSessionsForXUserId(xGrpUser.getUserId());
			if (!CollectionUtils.isEmpty(userSessions)) {
				for (UserSessionBase userSession : userSessions) {
					logger.info("deleting permission of the group whose one of the user found logged in into system, so updating permission in session of that user");
					sessionMgr.resetUserModulePermission(userSession);
				}
			}
		}
	}

	public void modifyUserActiveStatus(HashMap<Long, Integer> statusMap) {
		checkAdminAccess();
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		String currentUser=null;
		if(session!=null){
			currentUser=session.getLoginId();
			if(currentUser==null || currentUser.trim().isEmpty()){
				currentUser=null;
			}
		}
		if(currentUser==null){
			return;
		}
		Set<Map.Entry<Long, Integer>> entries = statusMap.entrySet();
		for (Map.Entry<Long, Integer> entry : entries) {
			if(entry!=null && entry.getKey()!=null && entry.getValue()!=null){
				XXUser xUser = daoManager.getXXUser().getById(entry.getKey());
				if(xUser!=null){
					VXPortalUser vXPortalUser = userMgr.getUserProfileByLoginId(xUser.getName());
					if(vXPortalUser!=null){
						if(vXPortalUser.getLoginId()!=null && !vXPortalUser.getLoginId().equalsIgnoreCase(currentUser)){
							vXPortalUser.setStatus(entry.getValue());
							userMgr.updateUser(vXPortalUser);
						}
					}
				}
			}
		}
	}

	public void checkAdminAccess() {
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session != null) {
			if (!session.isUserAdmin()) {
				throw restErrorUtil.create403RESTException("Operation" + " denied. LoggedInUser=" + (session != null ? session.getXXPortalUser().getId() : "Not Logged In")
						+ " ,isn't permitted to perform the action.");
			}
		} else {
			VXResponse vXResponse = new VXResponse();
			vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED);
			vXResponse.setMsgDesc("Bad Credentials");
			throw restErrorUtil.generateRESTException(vXResponse);
		}
	}

	public void checkAccess(String loginID) {
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session != null) {
			if (!session.isUserAdmin() && !session.isKeyAdmin() && !session.getLoginId().equalsIgnoreCase(loginID)) {
				throw restErrorUtil.create403RESTException("Operation" + " denied. LoggedInUser=" + (session != null ? session.getXXPortalUser().getId() : "Not Logged In")
						+ " ,isn't permitted to perform the action.");
			}
		} else {
			VXResponse vXResponse = new VXResponse();
			vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED);
			vXResponse.setMsgDesc("Bad Credentials");
			throw restErrorUtil.generateRESTException(vXResponse);
		}
	}

	public VXPermMapList searchXPermMaps(SearchCriteria searchCriteria) {
		VXPermMapList vXPermMapList = super.searchXPermMaps(searchCriteria);
		return applyDelegatedAdminAccess(vXPermMapList, searchCriteria);
	}

	private VXPermMapList applyDelegatedAdminAccess(VXPermMapList vXPermMapList, SearchCriteria searchCriteria) {

		VXPermMapList returnList;
		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
		// If user is system admin
		if (currentUserSession != null && currentUserSession.isUserAdmin()) {
			returnList = super.searchXPermMaps(searchCriteria);
		} else {
			returnList = new VXPermMapList();
			int startIndex = searchCriteria.getStartIndex();
			int pageSize = searchCriteria.getMaxRows();
			searchCriteria.setStartIndex(0);
			searchCriteria.setMaxRows(Integer.MAX_VALUE);
			List<VXPermMap> resultList = xPermMapService.searchXPermMaps(searchCriteria).getVXPermMaps();

			List<VXPermMap> adminPermResourceList = new ArrayList<VXPermMap>();
			for (VXPermMap xXPermMap : resultList) {
				XXResource xRes = daoManager.getXXResource().getById(xXPermMap.getResourceId());
				VXResponse vXResponse = msBizUtil.hasPermission(xResourceService.populateViewBean(xRes),
						AppConstants.XA_PERM_TYPE_ADMIN);
				if (vXResponse.getStatusCode() == VXResponse.STATUS_SUCCESS) {
					adminPermResourceList.add(xXPermMap);
				}
			}

			if (adminPermResourceList.size() > 0) {
				populatePageList(adminPermResourceList, startIndex, pageSize, returnList);
			}
		}
		return returnList;
	}

	private void populatePageList(List<VXPermMap> permMapList, int startIndex, int pageSize, VXPermMapList vxPermMapList) {
		List<VXPermMap> onePageList = new ArrayList<VXPermMap>();
		for (int i = startIndex; i < pageSize + startIndex && i < permMapList.size(); i++) {
			VXPermMap vXPermMap = permMapList.get(i);
			onePageList.add(vXPermMap);
		}
		vxPermMapList.setVXPermMaps(onePageList);
		vxPermMapList.setStartIndex(startIndex);
		vxPermMapList.setPageSize(pageSize);
		vxPermMapList.setResultSize(onePageList.size());
		vxPermMapList.setTotalCount(permMapList.size());
	}

	public VXAuditMapList searchXAuditMaps(SearchCriteria searchCriteria) {
		VXAuditMapList vXAuditMapList = xAuditMapService.searchXAuditMaps(searchCriteria);
		return applyDelegatedAdminAccess(vXAuditMapList, searchCriteria);
	}

	private VXAuditMapList applyDelegatedAdminAccess(VXAuditMapList vXAuditMapList, SearchCriteria searchCriteria) {

		VXAuditMapList returnList;
		UserSessionBase currentUserSession = ContextUtil.getCurrentUserSession();
		// If user is system admin
		if (currentUserSession != null && currentUserSession.isUserAdmin()) {
			returnList = super.searchXAuditMaps(searchCriteria);
		} else {
			returnList = new VXAuditMapList();
			int startIndex = searchCriteria.getStartIndex();
			int pageSize = searchCriteria.getMaxRows();
			searchCriteria.setStartIndex(0);
			searchCriteria.setMaxRows(Integer.MAX_VALUE);
			List<VXAuditMap> resultList = xAuditMapService.searchXAuditMaps(searchCriteria).getVXAuditMaps();

			List<VXAuditMap> adminAuditResourceList = new ArrayList<VXAuditMap>();
			for (VXAuditMap xXAuditMap : resultList) {
				XXResource xRes = daoManager.getXXResource().getById(xXAuditMap.getResourceId());
				VXResponse vXResponse = msBizUtil.hasPermission(xResourceService.populateViewBean(xRes),
						AppConstants.XA_PERM_TYPE_ADMIN);
				if (vXResponse.getStatusCode() == VXResponse.STATUS_SUCCESS) {
					adminAuditResourceList.add(xXAuditMap);
				}
			}

			if (adminAuditResourceList.size() > 0) {
				populatePageList(adminAuditResourceList, startIndex, pageSize, returnList);
			}
		}

		return returnList;
	}

	private void populatePageList(List<VXAuditMap> auditMapList, int startIndex, int pageSize,
			VXAuditMapList vxAuditMapList) {
		List<VXAuditMap> onePageList = new ArrayList<VXAuditMap>();
		for (int i = startIndex; i < pageSize + startIndex && i < auditMapList.size(); i++) {
			VXAuditMap vXAuditMap = auditMapList.get(i);
			onePageList.add(vXAuditMap);
		}
		vxAuditMapList.setVXAuditMaps(onePageList);
		vxAuditMapList.setStartIndex(startIndex);
		vxAuditMapList.setPageSize(pageSize);
		vxAuditMapList.setResultSize(onePageList.size());
		vxAuditMapList.setTotalCount(auditMapList.size());
	}

	public void checkAccessRoles(List<String> stringRolesList) {
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session != null && stringRolesList!=null) {
			if (!session.isUserAdmin() && !session.isKeyAdmin()) {
				throw restErrorUtil.create403RESTException("Permission"
						+ " denied. LoggedInUser="
						+ (session != null ? session.getXXPortalUser().getId()
								: "Not Logged In")
						+ " ,isn't permitted to perform the action.");
			}else{
				if (session.isUserAdmin() && stringRolesList.contains(RangerConstants.ROLE_KEY_ADMIN)) {
					throw restErrorUtil.create403RESTException("Permission"
							+ " denied. LoggedInUser="
							+ (session != null ? session.getXXPortalUser().getId()
									: "")
							+ " isn't permitted to perform the action.");
				}
				if (session.isKeyAdmin() && stringRolesList.contains(RangerConstants.ROLE_SYS_ADMIN)) {
					throw restErrorUtil.create403RESTException("Permission"
							+ " denied. LoggedInUser="
							+ (session != null ? session.getXXPortalUser().getId()
									: "")
							+ " isn't permitted to perform the action.");
				}
			}
		}else{
			VXResponse vXResponse = new VXResponse();
			vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED);
			vXResponse.setMsgDesc("Bad Credentials");
			throw restErrorUtil.generateRESTException(vXResponse);
		}
	}

	public VXStringList setUserRolesByExternalID(Long userId, List<VXString> vStringRolesList) {
		List<String> roleListNewProfile = new ArrayList<String>();
		if(vStringRolesList!=null){
			for (VXString vXString : vStringRolesList) {
				roleListNewProfile.add(vXString.getValue());
			}
		}
		checkAccessRoles(roleListNewProfile);
		VXUser vXUser=getXUser(userId);
		List<XXPortalUserRole> portalUserRoleList =null;
		if(vXUser!=null && roleListNewProfile.size()>0){
			VXPortalUser oldUserProfile = userMgr.getUserProfileByLoginId(vXUser.getName());
			if(oldUserProfile!=null){
				updateUserRolesPermissions(oldUserProfile,roleListNewProfile);
				portalUserRoleList = daoManager.getXXPortalUserRole().findByUserId(oldUserProfile.getId());
				return getStringListFromUserRoleList(portalUserRoleList);
			}else{
				throw restErrorUtil.createRESTException("User ID doesn't exist.", MessageEnums.INVALID_INPUT_DATA);
			}
		}else{
			throw restErrorUtil.createRESTException("User ID doesn't exist.", MessageEnums.INVALID_INPUT_DATA);
		}
	}

	public VXStringList setUserRolesByName(String userName, List<VXString> vStringRolesList) {
		List<String> roleListNewProfile = new ArrayList<String>();
		if(vStringRolesList!=null){
			for (VXString vXString : vStringRolesList) {
				roleListNewProfile.add(vXString.getValue());
			}
		}
		checkAccessRoles(roleListNewProfile);
		if(userName!=null && roleListNewProfile.size()>0){
			VXPortalUser oldUserProfile = userMgr.getUserProfileByLoginId(userName);
			if(oldUserProfile!=null){
				updateUserRolesPermissions(oldUserProfile,roleListNewProfile);
				List<XXPortalUserRole> portalUserRoleList = daoManager.getXXPortalUserRole().findByUserId(oldUserProfile.getId());
				return getStringListFromUserRoleList(portalUserRoleList);
			}else{
				throw restErrorUtil.createRESTException("Login ID doesn't exist.", MessageEnums.INVALID_INPUT_DATA);
			}
		}else{
			throw restErrorUtil.createRESTException("Login ID doesn't exist.", MessageEnums.INVALID_INPUT_DATA);
		}

	}

	public VXStringList getUserRolesByExternalID(Long userId) {
		VXUser vXUser=getXUser(userId);
		if(vXUser==null){
			throw restErrorUtil.createRESTException("Please provide a valid ID", MessageEnums.INVALID_INPUT_DATA);
		}
		checkAccess(vXUser.getName());
		List<XXPortalUserRole> portalUserRoleList =null;
		VXPortalUser oldUserProfile = userMgr.getUserProfileByLoginId(vXUser.getName());
		if(oldUserProfile!=null){
			portalUserRoleList = daoManager.getXXPortalUserRole().findByUserId(oldUserProfile.getId());
			return getStringListFromUserRoleList(portalUserRoleList);
		}else{
				throw restErrorUtil.createRESTException("User ID doesn't exist.", MessageEnums.INVALID_INPUT_DATA);
		}
	}

	public VXStringList getUserRolesByName(String userName) {
		VXPortalUser vXPortalUser=null;
		if(userName!=null && !userName.trim().isEmpty()){
			checkAccess(userName);
			vXPortalUser = userMgr.getUserProfileByLoginId(userName);
			if(vXPortalUser!=null && vXPortalUser.getUserRoleList()!=null){
				List<XXPortalUserRole> portalUserRoleList = daoManager.getXXPortalUserRole().findByUserId(vXPortalUser.getId());
				return getStringListFromUserRoleList(portalUserRoleList);
			}else{
				throw restErrorUtil.createRESTException("Please provide a valid userName", MessageEnums.INVALID_INPUT_DATA);
			}
		}else{
			throw restErrorUtil.createRESTException("Please provide a valid userName", MessageEnums.INVALID_INPUT_DATA);
		}
	}

	public void updateUserRolesPermissions(VXPortalUser oldUserProfile,List<String> roleListNewProfile){
		//update permissions start
		Collection<String> roleListUpdatedProfile =new ArrayList<String>();
		if (oldUserProfile != null && oldUserProfile.getId() != null) {
				Collection<String> roleListOldProfile = oldUserProfile.getUserRoleList();
				if(roleListNewProfile!=null && roleListOldProfile!=null){
					for (String role : roleListNewProfile) {
						if(role!=null && !roleListOldProfile.contains(role)){
							roleListUpdatedProfile.add(role);
						}
					}
				}
		}
		if(roleListUpdatedProfile!=null && roleListUpdatedProfile.size()>0){
			oldUserProfile.setUserRoleList(roleListUpdatedProfile);
			List<XXUserPermission> xuserPermissionList = daoManager
					.getXXUserPermission()
					.findByUserPermissionId(oldUserProfile.getId());
			if (xuserPermissionList!=null && xuserPermissionList.size()>0){
				for (XXUserPermission xXUserPermission : xuserPermissionList) {
					if (xXUserPermission != null) {
						xUserPermissionService.deleteResource(xXUserPermission.getId());
					}
				}
			}
			assignPermissionToUser(oldUserProfile,true);
			if(roleListUpdatedProfile!=null && roleListUpdatedProfile.size()>0){
				userMgr.updateRoles(oldUserProfile.getId(), oldUserProfile.getUserRoleList());
			}
		}
		//update permissions end
		}

	public VXStringList getStringListFromUserRoleList(
			List<XXPortalUserRole> listXXPortalUserRole) {
		if(listXXPortalUserRole==null){
			return null;
		}
		List<VXString> xStrList = new ArrayList<VXString>();
		VXString vXStr=null;
		for (XXPortalUserRole userRole : listXXPortalUserRole) {
			if(userRole!=null){
				vXStr = new VXString();
				vXStr.setValue(userRole.getUserRole());
				xStrList.add(vXStr);
			}
		}
		VXStringList vXStringList = new VXStringList(xStrList);
		return vXStringList;
	}

	public boolean hasAccess(String loginID) {
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session != null) {
			if(session.isUserAdmin() || session.getLoginId().equalsIgnoreCase(loginID)){
				return true;
			}
		}
		return false;
	}

	public VXUser getMaskedVXUser(VXUser vXUser) {
		if(vXUser!=null){
			if(vXUser.getGroupIdList()!=null && vXUser.getGroupIdList().size()>0){
				vXUser.setGroupIdList(new ArrayList<Long>());
			}
			if(vXUser.getGroupNameList()!=null && vXUser.getGroupNameList().size()>0){
				vXUser.setGroupNameList(getMaskedCollection(vXUser.getGroupNameList()));
			}
			if(vXUser.getUserRoleList()!=null && vXUser.getUserRoleList().size()>0){
				vXUser.setUserRoleList(getMaskedCollection(vXUser.getUserRoleList()));
			}
			vXUser.setUpdatedBy(AppConstants.Masked_String);
		}
		return vXUser;
	}

	public VXGroup getMaskedVXGroup(VXGroup vXGroup) {
        if(vXGroup!=null){
            vXGroup.setUpdatedBy(AppConstants.Masked_String);
        }
        return vXGroup;
	}

	@Override
	public VXUserList searchXUsers(SearchCriteria searchCriteria) {
        VXUserList vXUserList = new VXUserList();
        vXUserList=xUserService.searchXUsers(searchCriteria);
        if(vXUserList!=null && !hasAccessToModule(RangerConstants.MODULE_USER_GROUPS)){
	        List<VXUser> vXUsers = new ArrayList<VXUser>();
	        if(vXUserList!=null && vXUserList.getListSize()>0){
	            for(VXUser vXUser:vXUserList.getList()){
                    vXUser=getMaskedVXUser(vXUser);
                    vXUsers.add(vXUser);
	            }
	            vXUserList.setVXUsers(vXUsers);
	        }
        }
        return vXUserList;
	}

	@Override
	public VXGroupList searchXGroups(SearchCriteria searchCriteria) {
        VXGroupList vXGroupList=null;
        vXGroupList=xGroupService.searchXGroups(searchCriteria);
        if(vXGroupList!=null && !hasAccessToModule(RangerConstants.MODULE_USER_GROUPS)){
            if(vXGroupList!=null && vXGroupList.getListSize()>0){
                List<VXGroup> listMasked=new ArrayList<VXGroup>();
                for(VXGroup vXGroup:vXGroupList.getList()){
                    vXGroup=getMaskedVXGroup(vXGroup);
                    listMasked.add(vXGroup);
                }
                vXGroupList.setVXGroups(listMasked);
            }
        }
        return vXGroupList;
	}

	public Collection<String> getMaskedCollection(Collection<String> listunMasked){
        List<String> listMasked=new ArrayList<String>();
        if(listunMasked!=null && listunMasked.size()>0){
            for(String content:listunMasked){
                listMasked.add(AppConstants.Masked_String);
            }
        }
        return listMasked;
	}

	public boolean hasAccessToModule(String moduleName){
		UserSessionBase userSession = ContextUtil.getCurrentUserSession();
		if (userSession != null && userSession.getLoginId()!=null){
			VXUser vxUser = xUserService.getXUserByUserName(userSession.getLoginId());
			if(vxUser!=null){
				List<String> permissionList = daoManager.getXXModuleDef().findAccessibleModulesByUserId(userSession.getUserId(), vxUser.getId());
				if(permissionList!=null && permissionList.contains(moduleName)){
					return true;
				}
			}
		}
		return false;
	}

	public void deleteXGroup(Long id, boolean force) {
		checkAdminAccess();
		XXGroupDao xXGroupDao = daoManager.getXXGroup();
		XXGroup xXGroup = xXGroupDao.getById(id);
		VXGroup vXGroup = xGroupService.populateViewBean(xXGroup);
		if (vXGroup == null || StringUtil.isEmpty(vXGroup.getName())) {
			throw restErrorUtil.createRESTException("Group ID doesn't exist.", MessageEnums.INVALID_INPUT_DATA);
		}
		if(logger.isDebugEnabled()){
			logger.info("Force delete status="+force+" for group="+vXGroup.getName());
		}

		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.addParam("xGroupId", id);
		VXGroupUserList vxGroupUserList = searchXGroupUsers(searchCriteria);

		searchCriteria = new SearchCriteria();
		searchCriteria.addParam("groupId", id);
		VXPermMapList vXPermMapList = searchXPermMaps(searchCriteria);

		searchCriteria = new SearchCriteria();
		searchCriteria.addParam("groupId", id);
		VXAuditMapList vXAuditMapList = searchXAuditMaps(searchCriteria);

		XXGroupPermissionDao xXGroupPermissionDao=daoManager.getXXGroupPermission();
		List<XXGroupPermission> xXGroupPermissions=xXGroupPermissionDao.findByGroupId(id);

		XXGroupGroupDao xXGroupGroupDao = daoManager.getXXGroupGroup();
		List<XXGroupGroup> xXGroupGroups = xXGroupGroupDao.findByGroupId(id);

		XXPolicyDao xXPolicyDao = daoManager.getXXPolicy();
		List<XXPolicy> xXPolicyList = xXPolicyDao.findByGroupId(id);
		logger.warn("Deleting GROUP : "+vXGroup.getName());
		if (force) {
			//delete XXGroupUser records of matching group
			XXGroupUserDao xGroupUserDao = daoManager.getXXGroupUser();
			XXUserDao xXUserDao = daoManager.getXXUser();
			XXUser xXUser =null;
			for (VXGroupUser groupUser : vxGroupUserList.getList()) {
				if(groupUser!=null){
					xXUser=xXUserDao.getById(groupUser.getUserId());
					if(xXUser!=null){
						logger.warn("Removing user '" + xXUser.getName() + "' from group '" + groupUser.getName() + "'");
					}
					xGroupUserDao.remove(groupUser.getId());
				}
			}
			//delete XXPermMap records of matching group
			XXPermMapDao xXPermMapDao = daoManager.getXXPermMap();
			XXResourceDao xXResourceDao = daoManager.getXXResource();
			XXResource xXResource =null;
			for (VXPermMap vXPermMap : vXPermMapList.getList()) {
				if(vXPermMap!=null){
					xXResource=xXResourceDao.getById(vXPermMap.getResourceId());
					if(xXResource!=null){
						logger.warn("Deleting '" + AppConstants.getLabelFor_XAPermType(vXPermMap.getPermType()) + "' permission from policy ID='" + vXPermMap.getResourceId() + "' for group '" + vXPermMap.getGroupName() + "'");
					}
					xXPermMapDao.remove(vXPermMap.getId());
				}
			}
			//delete XXAuditMap records of matching group
			XXAuditMapDao xXAuditMapDao = daoManager.getXXAuditMap();
			for (VXAuditMap vXAuditMap : vXAuditMapList.getList()) {
				if(vXAuditMap!=null){
					xXResource=xXResourceDao.getById(vXAuditMap.getResourceId());
					xXAuditMapDao.remove(vXAuditMap.getId());
				}
			}
			//delete XXGroupGroupDao records of group-group mapping
			for (XXGroupGroup xXGroupGroup : xXGroupGroups) {
				if(xXGroupGroup!=null){
					XXGroup xXGroupParent=xXGroupDao.getById(xXGroupGroup.getParentGroupId());
					XXGroup xXGroupChild=xXGroupDao.getById(xXGroupGroup.getGroupId());
					if(xXGroupParent!=null && xXGroupChild!=null){
						logger.warn("Removing group '" + xXGroupChild.getName() + "' from group '" + xXGroupParent.getName() + "'");
					}
					xXGroupGroupDao.remove(xXGroupGroup.getId());
				}
			}
			//delete XXPolicyItemGroupPerm records of group
			for (XXPolicy xXPolicy : xXPolicyList) {
				RangerPolicy rangerPolicy = policyService.getPopulatedViewObject(xXPolicy);
				List<RangerPolicyItem> policyItems = rangerPolicy.getPolicyItems();
				removeUserGroupReferences(policyItems,null,vXGroup.getName());
				rangerPolicy.setPolicyItems(policyItems);
				try {
					svcStore.updatePolicy(rangerPolicy);
				} catch (Throwable excp) {
					logger.error("updatePolicy(" + rangerPolicy + ") failed", excp);
					restErrorUtil.createRESTException(excp.getMessage());
				}
			}
			if(CollectionUtils.isNotEmpty(xXGroupPermissions)){
				for (XXGroupPermission xXGroupPermission : xXGroupPermissions) {
					if(xXGroupPermission!=null){
						XXModuleDef xXModuleDef=daoManager.getXXModuleDef().findByModuleId(xXGroupPermission.getModuleId());
						if(xXModuleDef!=null){
							logger.warn("Deleting '" + xXModuleDef.getModule() + "' module permission for group '" + xXGroup.getName() + "'");
						}
						xXGroupPermissionDao.remove(xXGroupPermission.getId());
					}
				}
			}
			//delete XXGroup
			xXGroupDao.remove(id);
			//Create XXTrxLog
			List<XXTrxLog> xXTrxLogsXXGroup = xGroupService.getTransactionLog(xGroupService.populateViewBean(xXGroup),
					"delete");
			xaBizUtil.createTrxLog(xXTrxLogsXXGroup);
		} else {
			boolean hasReferences=false;

			if(vxGroupUserList.getListSize()>0){
				hasReferences=true;
			}
			if(hasReferences==false && CollectionUtils.isNotEmpty(xXPolicyList)){
				hasReferences=true;
			}
			if(hasReferences==false && vXPermMapList.getListSize()>0){
				hasReferences=true;
			}
			if(hasReferences==false && vXAuditMapList.getListSize()>0){
				hasReferences=true;
			}
			if(hasReferences==false && CollectionUtils.isNotEmpty(xXGroupGroups)){
				hasReferences=true;
			}
			if(hasReferences==false && CollectionUtils.isNotEmpty(xXGroupPermissions)){
				hasReferences=true;
			}

			if(hasReferences){ //change visibility to Hidden
				if(vXGroup.getIsVisible()==RangerCommonEnums.IS_VISIBLE){
					vXGroup.setIsVisible(RangerCommonEnums.IS_HIDDEN);
					xGroupService.updateResource(vXGroup);
				}
			}else{
				//delete XXGroup
				xXGroupDao.remove(id);
				//Create XXTrxLog
				List<XXTrxLog> xXTrxLogsXXGroup = xGroupService.getTransactionLog(xGroupService.populateViewBean(xXGroup),
						"delete");
				xaBizUtil.createTrxLog(xXTrxLogsXXGroup);
			}
		}
	}

	public void deleteXUser(Long id, boolean force) {
		checkAdminAccess();
		XXUserDao xXUserDao = daoManager.getXXUser();
		XXUser xXUser =	xXUserDao.getById(id);
		VXUser vXUser =	xUserService.populateViewBean(xXUser);
		if(vXUser==null ||StringUtil.isEmpty(vXUser.getName())){
			throw restErrorUtil.createRESTException("No user found with id=" + id);
		}
		XXPortalUserDao xXPortalUserDao=daoManager.getXXPortalUser();
		XXPortalUser xXPortalUser=xXPortalUserDao.findByLoginId(vXUser.getName().trim());
		VXPortalUser vXPortalUser=xPortalUserService.populateViewBean(xXPortalUser);
		if(vXPortalUser==null ||StringUtil.isEmpty(vXPortalUser.getLoginId())){
			throw restErrorUtil.createRESTException("No user found with id=" + id);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Force delete status="+force+" for user="+vXUser.getName());
		}
		restrictSelfAccountDeletion(vXUser.getName().trim());
		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.addParam("xUserId", id);
		VXGroupUserList vxGroupUserList = searchXGroupUsers(searchCriteria);

		searchCriteria = new SearchCriteria();
		searchCriteria.addParam("userId", id);
		VXPermMapList vXPermMapList = searchXPermMaps(searchCriteria);

		searchCriteria = new SearchCriteria();
		searchCriteria.addParam("userId", id);
		VXAuditMapList vXAuditMapList = searchXAuditMaps(searchCriteria);

		long xXPortalUserId=0;
		xXPortalUserId=vXPortalUser.getId();
		XXAuthSessionDao xXAuthSessionDao=daoManager.getXXAuthSession();
		XXUserPermissionDao xXUserPermissionDao=daoManager.getXXUserPermission();
		XXPortalUserRoleDao xXPortalUserRoleDao=daoManager.getXXPortalUserRole();
		List<XXAuthSession> xXAuthSessions=xXAuthSessionDao.getAuthSessionByUserId(xXPortalUserId);
		List<XXUserPermission> xXUserPermissions=xXUserPermissionDao.findByUserPermissionId(xXPortalUserId);
		List<XXPortalUserRole> xXPortalUserRoles=xXPortalUserRoleDao.findByUserId(xXPortalUserId);

		XXPolicyDao xXPolicyDao = daoManager.getXXPolicy();
		List<XXPolicy> xXPolicyList=xXPolicyDao.findByUserId(id);
		logger.warn("Deleting User : "+vXUser.getName());
		if (force) {
			//delete XXGroupUser mapping
			XXGroupUserDao xGroupUserDao = daoManager.getXXGroupUser();
			for (VXGroupUser groupUser : vxGroupUserList.getList()) {
				if(groupUser!=null){
					logger.warn("Removing user '" + vXUser.getName() + "' from group '" + groupUser.getName() + "'");
					xGroupUserDao.remove(groupUser.getId());
				}
			}
			//delete XXPermMap records of user
			XXPermMapDao xXPermMapDao = daoManager.getXXPermMap();
			for (VXPermMap vXPermMap : vXPermMapList.getList()) {
				if(vXPermMap!=null){
					logger.warn("Deleting '" + AppConstants.getLabelFor_XAPermType(vXPermMap.getPermType()) + "' permission from policy ID='" + vXPermMap.getResourceId() + "' for user '" + vXPermMap.getUserName() + "'");
					xXPermMapDao.remove(vXPermMap.getId());
				}
			}
			//delete XXAuditMap records of user
			XXAuditMapDao xXAuditMapDao = daoManager.getXXAuditMap();
			for (VXAuditMap vXAuditMap : vXAuditMapList.getList()) {
				if(vXAuditMap!=null){
					xXAuditMapDao.remove(vXAuditMap.getId());
				}
			}
			//delete XXPortalUser references
			if(vXPortalUser!=null){
				xPortalUserService.updateXXPortalUserReferences(xXPortalUserId);
				if(xXAuthSessions!=null && xXAuthSessions.size()>0){
					logger.warn("Deleting " + xXAuthSessions.size() + " login session records for user '" +  vXPortalUser.getLoginId() + "'");
				}
				for (XXAuthSession xXAuthSession : xXAuthSessions) {
					xXAuthSessionDao.remove(xXAuthSession.getId());
				}
				for (XXUserPermission xXUserPermission : xXUserPermissions) {
					if(xXUserPermission!=null){
						XXModuleDef xXModuleDef=daoManager.getXXModuleDef().findByModuleId(xXUserPermission.getModuleId());
						if(xXModuleDef!=null){
							logger.warn("Deleting '" + xXModuleDef.getModule() + "' module permission for user '" + vXPortalUser.getLoginId() + "'");
						}
						xXUserPermissionDao.remove(xXUserPermission.getId());
					}
				}
				for (XXPortalUserRole xXPortalUserRole : xXPortalUserRoles) {
					if(xXPortalUserRole!=null){
						logger.warn("Deleting '" + xXPortalUserRole.getUserRole() + "' role for user '" + vXPortalUser.getLoginId() + "'");
						xXPortalUserRoleDao.remove(xXPortalUserRole.getId());
					}
				}
			}
			//delete XXPolicyItemUserPerm records of user
			for(XXPolicy xXPolicy:xXPolicyList){
				RangerPolicy rangerPolicy = policyService.getPopulatedViewObject(xXPolicy);
				List<RangerPolicyItem> policyItems = rangerPolicy.getPolicyItems();
				removeUserGroupReferences(policyItems,vXUser.getName(),null);
				rangerPolicy.setPolicyItems(policyItems);
				try{
					svcStore.updatePolicy(rangerPolicy);
				}catch(Throwable excp) {
					logger.error("updatePolicy(" + rangerPolicy + ") failed", excp);
					throw restErrorUtil.createRESTException(excp.getMessage());
				}
			}
			//delete XXUser entry of user
			xXUserDao.remove(id);
			//delete XXPortal entry of user
			logger.warn("Deleting Portal User : "+vXPortalUser.getLoginId());
			xXPortalUserDao.remove(xXPortalUserId);
			List<XXTrxLog> trxLogList =xUserService.getTransactionLog(xUserService.populateViewBean(xXUser), "delete");
			xaBizUtil.createTrxLog(trxLogList);
			if (xXPortalUser != null) {
				trxLogList=xPortalUserService
						.getTransactionLog(xPortalUserService.populateViewBean(xXPortalUser), "delete");
				xaBizUtil.createTrxLog(trxLogList);
			}
		} else {
			boolean hasReferences=false;

			if(vxGroupUserList!=null && vxGroupUserList.getListSize()>0){
				hasReferences=true;
			}
			if(hasReferences==false && xXPolicyList!=null && xXPolicyList.size()>0){
				hasReferences=true;
			}
			if(hasReferences==false && vXPermMapList!=null && vXPermMapList.getListSize()>0){
				hasReferences=true;
			}
			if(hasReferences==false && vXAuditMapList!=null && vXAuditMapList.getListSize()>0){
				hasReferences=true;
			}
			if(hasReferences==false && xXAuthSessions!=null && xXAuthSessions.size()>0){
				hasReferences=true;
			}
			if(hasReferences==false && xXUserPermissions!=null && xXUserPermissions.size()>0){
				hasReferences=true;
			}
			if(hasReferences==false && xXPortalUserRoles!=null && xXPortalUserRoles.size()>0){
				hasReferences=true;
			}
			if(hasReferences){
				if(vXUser.getIsVisible()!=RangerCommonEnums.IS_HIDDEN){
					logger.info("Updating visibility of user '"+vXUser.getName()+"' to Hidden!");
					vXUser.setIsVisible(RangerCommonEnums.IS_HIDDEN);
					xUserService.updateResource(vXUser);
				}
			}else{
				xPortalUserService.updateXXPortalUserReferences(xXPortalUserId);
				//delete XXUser entry of user
				xXUserDao.remove(id);
				//delete XXPortal entry of user
				logger.warn("Deleting Portal User : "+vXPortalUser.getLoginId());
				xXPortalUserDao.remove(xXPortalUserId);
				List<XXTrxLog> trxLogList =xUserService.getTransactionLog(xUserService.populateViewBean(xXUser), "delete");
				xaBizUtil.createTrxLog(trxLogList);
				if (xXPortalUser != null) {
					trxLogList=xPortalUserService
							.getTransactionLog(xPortalUserService.populateViewBean(xXPortalUser), "delete");
					xaBizUtil.createTrxLog(trxLogList);
				}
			}
		}
	}

	private void removeUserGroupReferences(List<RangerPolicyItem> policyItems, String user, String group) {
		List<RangerPolicyItem> itemsToRemove = null;
		for(RangerPolicyItem policyItem : policyItems) {
			if(!StringUtil.isEmpty(user)) {
				policyItem.getUsers().remove(user);
			}
			if(!StringUtil.isEmpty(group)) {
				policyItem.getGroups().remove(group);
			}
			if(policyItem.getUsers().isEmpty() && policyItem.getGroups().isEmpty()) {
				if(itemsToRemove == null) {
					itemsToRemove = new ArrayList<RangerPolicyItem>();
				}
				itemsToRemove.add(policyItem);
			}
		}
		if(CollectionUtils.isNotEmpty(itemsToRemove)) {
			policyItems.removeAll(itemsToRemove);
		}
	}

	protected VXUser createServiceConfigUser(String userName){
	        if (userName == null || "null".equalsIgnoreCase(userName) || userName.trim().isEmpty()) {
                logger.error("User Name: "+userName);
                throw restErrorUtil.createRESTException("Please provide a valid username.",MessageEnums.INVALID_INPUT_DATA);
	        }
	        VXUser vXUser = null;
	        VXPortalUser vXPortalUser=null;
	        XXUser xxUser = daoManager.getXXUser().findByUserName(userName);
	        XXPortalUser xXPortalUser = daoManager.getXXPortalUser().findByLoginId(userName);
	        String actualPassword = "";
	        if(xxUser!=null && xXPortalUser!=null){
                vXUser = xUserService.populateViewBean(xxUser);
                return vXUser;
	        }
	        if(xxUser==null){
                vXUser=new VXUser();
                vXUser.setName(userName);
                vXUser.setUserSource(RangerCommonEnums.USER_EXTERNAL);
                vXUser.setDescription(vXUser.getName());
                actualPassword = vXUser.getPassword();
	        }
	        if(xXPortalUser==null){
                vXPortalUser=new VXPortalUser();
                vXPortalUser.setLoginId(userName);
                vXPortalUser.setEmailAddress(guidUtil.genGUID());
                vXPortalUser.setFirstName(vXUser.getFirstName());
                vXPortalUser.setLastName(vXUser.getLastName());
                vXPortalUser.setPassword(vXUser.getPassword());
                vXPortalUser.setUserSource(RangerCommonEnums.USER_EXTERNAL);
                ArrayList<String> roleList = new ArrayList<String>();
                roleList.add(RangerConstants.ROLE_USER);
                vXPortalUser.setUserRoleList(roleList);
                xXPortalUser = userMgr.mapVXPortalUserToXXPortalUser(vXPortalUser);
                xXPortalUser=userMgr.createUser(xXPortalUser, RangerCommonEnums.STATUS_ENABLED, roleList);
	        }
	        VXUser createdXUser=null;
	        if(xxUser==null && vXUser!=null){
                createdXUser = xUserService.createResource(vXUser);
	        }
	        if(createdXUser!=null){
                logger.info("User created: "+createdXUser.getName());
                createdXUser.setPassword(actualPassword);
                List<XXTrxLog> trxLogList = xUserService.getTransactionLog(createdXUser, "create");
                String hiddenPassword = PropertiesUtil.getProperty("ranger.password.hidden", "*****");
                createdXUser.setPassword(hiddenPassword);
                xaBizUtil.createTrxLog(trxLogList);
                if(xXPortalUser!=null){
                        vXPortalUser=userMgr.mapXXPortalUserToVXPortalUserForDefaultAccount(xXPortalUser);
                        assignPermissionToUser(vXPortalUser, true);
                }
	        }
	        return createdXUser;
	}
	public void restrictSelfAccountDeletion(String loginID) {
		UserSessionBase session = ContextUtil.getCurrentUserSession();
		if (session != null) {
			if (!session.isUserAdmin()) {
				throw restErrorUtil.create403RESTException("Operation denied. LoggedInUser= "+session.getXXPortalUser().getLoginId() + " isn't permitted to perform the action.");
			}else{
				if(!StringUtil.isEmpty(loginID) && loginID.equals(session.getLoginId())){
					throw restErrorUtil.create403RESTException("Operation denied. LoggedInUser= "+session.getXXPortalUser().getLoginId() + " isn't permitted to delete his own profile.");
				}
			}
		} else {
			VXResponse vXResponse = new VXResponse();
			vXResponse.setStatusCode(HttpServletResponse.SC_UNAUTHORIZED);
			vXResponse.setMsgDesc("Bad Credentials");
			throw restErrorUtil.generateRESTException(vXResponse);
		}
	}
}
