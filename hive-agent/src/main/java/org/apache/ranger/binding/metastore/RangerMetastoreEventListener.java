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

package org.apache.ranger.binding.metastore;

import com.google.common.base.Splitter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.*;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.hive.authorizer.HiveAccessType;
import org.apache.ranger.authorization.hive.authorizer.HiveObjectType;
import org.apache.ranger.authorization.hive.authorizer.RangerHiveAuditHandler;
import org.apache.ranger.authorization.hive.authorizer.RangerHiveResource;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.policyengine.*;
import org.apache.ranger.plugin.util.GrantRevokeRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerMetastoreEventListener extends MetaStoreEventListener {
  private static final Log LOGGER = LogFactory.getLog(RangerMetastoreEventListener.class);
  private static final char COLUMN_SEP = ',';

  private HiveConf hiveConf;
  private RangerAdminRESTClient rangerAdminClient = new RangerAdminRESTClient();

  protected List<RangerMetastoreListenerPlugin> rangerPlugins = new ArrayList<RangerMetastoreListenerPlugin>();

  public RangerMetastoreEventListener(Configuration config) {
    super(config);

    if (!(config instanceof HiveConf)) {
      String error = "Could not initialize Plugin - Configuration is not an instanceof HiveConf";
      LOGGER.error(error);
      throw new RuntimeException(error);
    }
    hiveConf = (HiveConf)config;

    String serviceType = "hive", appId = "metastore";
    String propertyPrefix = "ranger.plugin." + serviceType;
    RangerConfiguration.getInstance().addResourcesForServiceType(serviceType);
    RangerConfiguration.getInstance().initAudit(appId);

    String serviceName = RangerConfiguration.getInstance().get(propertyPrefix + ".service.name");
    rangerAdminClient.init(serviceName, appId, propertyPrefix);
  }

  @Override
  public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip sync paths/privileges with Sentry server for onCreateTable event," +
          " since the operation failed. \n");
      return;
    }

    // drop the privileges on the given table, in case if anything was left
    // behind during the drop
    if (!rangerConfigureIsTrue(RangerHadoopConstants.SYNC_CREATE_WITH_POLICY_STORE)) {
      return;
    }

    UserGroupInformation ugi;
    try {
      ugi = Utils.getUGI();
    } catch (Exception exp) {
      throw new MetaException(exp.getMessage());
    }

    HivePrincipal grantorPrincipal = null;
    List<HivePrincipal> hivePrincipals = new ArrayList<HivePrincipal>();
    String[] groupNames = ugi.getGroupNames();
    for (int i = 0; i < groupNames.length; i ++) {
      HivePrincipal hivePrincipal = new HivePrincipal(groupNames[i], HivePrincipal.HivePrincipalType.GROUP);
      hivePrincipals.add(hivePrincipal);

      grantorPrincipal = new HivePrincipal(groupNames[i], HivePrincipal.HivePrincipalType.GROUP);
    }

    HivePrivilegeObject hivePrivilegeObject = new HivePrivilegeObject(
        HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW,
        tableEvent.getTable().getDbName(),
        tableEvent.getTable().getTableName());

    HivePrivilege hivePrivilege = new HivePrivilege(HiveAccessType.CREATE.name(), (List)null);
    List<HivePrivilege> hivePrivileges = new ArrayList<HivePrivilege>();
    hivePrivileges.add(hivePrivilege);

    grantPrivileges(hivePrincipals, hivePrivileges, hivePrivilegeObject, grantorPrincipal, false);
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip syncing paths/privileges with Ranger server for onDropTable event," +
          " since the operation failed. \n");
      return;
    }

    // drop the privileges on the given table
    if (!rangerConfigureIsTrue(RangerHadoopConstants.SYNC_DROP_WITH_POLICY_STORE)) {
      return;
    }

    HivePrivilegeObject hivePrivilegeObject = new HivePrivilegeObject(
        HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW,
        tableEvent.getTable().getDbName(),
        tableEvent.getTable().getTableName());

    HivePrivilege hivePrivilege = new HivePrivilege(HiveAccessType.DROP.name(), (List)null);
    List<HivePrivilege> hivePrivilegeList = new ArrayList<HivePrivilege>();
    hivePrivilegeList.add(hivePrivilege);

    grantPrivileges(new ArrayList<HivePrincipal>(), hivePrivilegeList, hivePrivilegeObject, null, false);
  }

  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
                              List<HivePrivilege> hivePrivileges,
                              HivePrivilegeObject hivePrivObject,
                              HivePrincipal       grantorPrincipal,
                              boolean             grantOption)
      throws MetaException {
    RangerHiveAuditHandler auditHandler = new RangerHiveAuditHandler();

    try {
      RangerHiveResource resource = getHiveResource(HiveOperationType.GRANT_PRIVILEGE, hivePrivObject);
      GrantRevokeRequest request  = createGrantRevokeData(resource, hivePrincipals, hivePrivileges, grantorPrincipal, grantOption);

      LOGGER.info("grantPrivileges(): " + request);
      if(LOGGER.isDebugEnabled()) {
        LOGGER.debug("grantPrivileges(): " + request);
      }

      rangerAdminClient.grantAccess(request);
      auditGrantRevoke(request, "grant", true, auditHandler);
    } catch(Exception excp) {
      throw new MetaException(excp.getMessage());
    } finally {
      auditHandler.flushAudit();
    }
  }

  private void auditGrantRevoke(GrantRevokeRequest request, String action, boolean isSuccess, RangerAccessResultProcessor resultProcessor) {
    if(request != null && resultProcessor != null) {
      RangerAccessRequestImpl accessRequest = new RangerAccessRequestImpl();

      accessRequest.setResource(new RangerAccessResourceImpl(request.getResource()));
      accessRequest.setUser(request.getGrantor());
      accessRequest.setAccessType(RangerPolicyEngine.ADMIN_ACCESS);
      accessRequest.setAction(action);
      accessRequest.setClientIPAddress(request.getClientIPAddress());
      accessRequest.setClientType(request.getClientType());
      accessRequest.setRequestData(request.getRequestData());
      accessRequest.setSessionId(request.getSessionId());

      // call isAccessAllowed() to determine if audit is enabled or not
     // RangerAccessResult accessResult = isAccessAllowed(accessRequest, null);

      /*
      if(accessResult != null && accessResult.getIsAudited()) {
        accessRequest.setAccessType(action);
        accessResult.setIsAllowed(isSuccess);

        if(! isSuccess) {
          accessResult.setPolicyId(-1);
        }

        resultProcessor.processResult(accessResult);
      }
      */
    }
  }

  private GrantRevokeRequest createGrantRevokeData(RangerHiveResource  resource,
                                                   List<HivePrincipal> hivePrincipals,
                                                   List<HivePrivilege> hivePrivileges,
                                                   HivePrincipal       grantorPrincipal,
                                                   boolean             grantOption)
      throws MetaException {
    if(resource == null ||
        ! ( resource.getObjectType() == HiveObjectType.DATABASE
            || resource.getObjectType() == HiveObjectType.TABLE
            || resource.getObjectType() == HiveObjectType.VIEW
            || resource.getObjectType() == HiveObjectType.COLUMN )) {
      throw new MetaException("grant/revoke: unexpected object type '" + (resource == null ? null : resource.getObjectType().name()));
    }

    GrantRevokeRequest ret = new GrantRevokeRequest();

    ret.setGrantor(getGrantorUsername(grantorPrincipal));
    ret.setDelegateAdmin(grantOption ? Boolean.TRUE : Boolean.FALSE);
    ret.setEnableAudit(Boolean.TRUE);
    ret.setReplaceExistingPermissions(Boolean.FALSE);

    String database = StringUtils.isEmpty(resource.getDatabase()) ? "*" : resource.getDatabase();
    String table    = StringUtils.isEmpty(resource.getTable()) ? "*" : resource.getTable();
    String column   = StringUtils.isEmpty(resource.getColumn()) ? "*" : resource.getColumn();

    Map<String, String> mapResource = new HashMap<String, String>();
    mapResource.put(RangerHiveResource.KEY_DATABASE, database);
    mapResource.put(RangerHiveResource.KEY_TABLE, table);
    mapResource.put(RangerHiveResource.KEY_COLUMN, column);

    ret.setResource(mapResource);

    SessionState ss = SessionState.get();
    if(ss != null) {
      ret.setClientIPAddress(ss.getUserIpAddress());
      ret.setSessionId(ss.getSessionId());
      ret.setRequestData(ss.getCmd());
    }

    ret.setClientType("metastore");

    for(HivePrincipal principal : hivePrincipals) {
      switch(principal.getType()) {
        case USER:
          ret.getUsers().add(principal.getName());
          break;
        case GROUP:
        case ROLE:
          ret.getGroups().add(principal.getName());
          break;
        case UNKNOWN:
          break;
      }
    }

    for(HivePrivilege privilege : hivePrivileges) {
      String privName = privilege.getName();

      if(StringUtils.equalsIgnoreCase(privName, HiveAccessType.ALL.name()) ||
          StringUtils.equalsIgnoreCase(privName, HiveAccessType.ALTER.name()) ||
          StringUtils.equalsIgnoreCase(privName, HiveAccessType.CREATE.name()) ||
          StringUtils.equalsIgnoreCase(privName, HiveAccessType.DROP.name()) ||
          StringUtils.equalsIgnoreCase(privName, HiveAccessType.INDEX.name()) ||
          StringUtils.equalsIgnoreCase(privName, HiveAccessType.LOCK.name()) ||
          StringUtils.equalsIgnoreCase(privName, HiveAccessType.SELECT.name()) ||
          StringUtils.equalsIgnoreCase(privName, HiveAccessType.UPDATE.name())) {
        ret.getAccessTypes().add(privName.toLowerCase());
      } else {
        LOGGER.warn("grant/revoke: unexpected privilege type '" + privName + "'. Ignored");
      }
    }

    return ret;
  }

  private String getGrantorUsername(HivePrincipal grantorPrincipal) {
    String grantor = grantorPrincipal != null ? grantorPrincipal.getName() : null;

    /*
    if(StringUtil.isEmpty(grantor)) {
      UserGroupInformation ugi = this.getCurrentUserGroupInfo();

      grantor = ugi != null ? ugi.getShortUserName() : null;
    }
    */

    return grantor;
  }

  private RangerHiveResource getHiveResource(HiveOperationType hiveOpType,
                                             HivePrivilegeObject hiveObj) {
    RangerHiveResource ret = null;

    HiveObjectType objectType = getObjectType(hiveObj, hiveOpType);

    switch(objectType) {
      case DATABASE:
        ret = new RangerHiveResource(objectType, hiveObj.getDbname());
        break;
      case TABLE:
      case VIEW:
      case PARTITION:
      case INDEX:
      case FUNCTION:
        ret = new RangerHiveResource(objectType, hiveObj.getDbname(), hiveObj.getObjectName());
        break;
      case COLUMN:
        ret = new RangerHiveResource(objectType, hiveObj.getDbname(), hiveObj.getObjectName(),
            StringUtils.join(hiveObj.getColumns(), COLUMN_SEP));
        break;
      case URI:
        ret = new RangerHiveResource(objectType, hiveObj.getObjectName());
        break;
      case NONE:
        break;
    }

    return ret;
  }

  private HiveObjectType getObjectType(HivePrivilegeObject hiveObj, HiveOperationType hiveOpType) {
    HiveObjectType objType = HiveObjectType.NONE;

    switch(hiveObj.getType()) {
      case DATABASE:
        objType = HiveObjectType.DATABASE;
        break;
      case PARTITION:
        objType = HiveObjectType.PARTITION;
        break;
      case TABLE_OR_VIEW:
        String hiveOpTypeName = hiveOpType.name().toLowerCase();
        if(hiveOpTypeName.contains("index")) {
          objType = HiveObjectType.INDEX;
        } else if(! StringUtil.isEmpty(hiveObj.getColumns())) {
          objType = HiveObjectType.COLUMN;
        } else if(hiveOpTypeName.contains("view")) {
          objType = HiveObjectType.VIEW;
        } else {
          objType = HiveObjectType.TABLE;
        }
        break;
      case FUNCTION:
        objType = HiveObjectType.FUNCTION;
        break;
      case DFS_URI:
      case LOCAL_URI:
        objType = HiveObjectType.URI;
        break;
      case COMMAND_PARAMS:
      case GLOBAL:
        break;
      case COLUMN:
        // Thejas: this value is unused in Hive; the case should not be hit.
        break;
    }

    return objType;
  }

  private boolean rangerConfigureIsTrue(String confVar) {
    return "true".equalsIgnoreCase(RangerConfiguration.getInstance().get(confVar, "true"));
  }

  public static class ConfUtilties {
    public static final Splitter CLASS_SPLITTER = Splitter.onPattern("[\\s,]")
        .trimResults().omitEmptyStrings();
  }
}
