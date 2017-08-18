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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.ql.security.authorization.plugin.*;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.hive.authorizer.*;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.policyengine.*;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.*;
import org.apache.ranger.plugin.util.HiveOperationType;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SyncMetastoreEventListener extends MetaStoreEventListener {
  private static final Log LOGGER = LogFactory.getLog(SyncMetastoreEventListener.class);
  private static final char COLUMN_SEP = ',';

  private RangerBasePlugin rangerPlugin = null;

  private String                    serviceType  = "hive";
  private String                    appId        = "metastore";
  private String                    serviceName  = null;

  private ConcurrentLinkedQueue<SyncRequestStruct> syncRequestQueue = new ConcurrentLinkedQueue<>();

  public SyncMetastoreEventListener(Configuration config) {
    super(config);

    if (!(config instanceof HiveConf)) {
      String error = "Could not initialize Plugin - Configuration is not an instanceof HiveConf";
      LOGGER.error(error);
      throw new RuntimeException(error);
    }

    rangerPlugin = new RangerBasePlugin(serviceType, appId);
    rangerPlugin.init();

    String propertyPrefix = "ranger.plugin." + serviceType;

    serviceName = RangerConfiguration.getInstance().get(propertyPrefix + ".service.name");

    Thread syncRequestThread = null;
    try {
      syncRequestThread = new Thread(new SyncPoliciesRunnable(syncRequestQueue));
    } catch (IOException e) {
      e.printStackTrace();
    }
    syncRequestThread.start();
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent)
      throws MetaException {

    // don't sync paths/privileges if the operation has failed
    if (!dbEvent.getStatus()) {
      LOGGER.debug("Skip syncing paths/privileges with Ranger server for onCreateDatabase event," +
          " since the operation failed. \n");
      return;
    }

    // drop the privileges on the database, in case anything left behind during
    // last drop db
    if (!rangerConfigureIsTrue(RangerHadoopConstants.SYNC_CREATE_WITH_POLICY_STORE)) {
      return;
    }

    synchronizePolicy(dbEvent, HiveOperationType.CREATEDATABASE);
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!dbEvent.getStatus()) {
      LOGGER.debug("Skip syncing paths/privileges with Ranger server for onDropDatabase event," +
          " since the operation failed. \n");
      return;
    }

    if (!rangerConfigureIsTrue(RangerHadoopConstants.SYNC_DROP_WITH_POLICY_STORE)) {
      return;
    }

    synchronizePolicy(dbEvent, HiveOperationType.DROPDATABASE);
  }

  @Override
  public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip sync paths/privileges with Ranger server for onCreateTable event," +
          " since the operation failed. \n");
      return;
    }

    // drop the privileges on the given table
    if (!rangerConfigureIsTrue(RangerHadoopConstants.SYNC_CREATE_WITH_POLICY_STORE)) {
      return;
    }

    synchronizePolicy(tableEvent, HiveOperationType.CREATETABLE);
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

    synchronizePolicy(tableEvent, HiveOperationType.DROPTABLE);
  }

  /**
   * Adjust the privileges when table is renamed
   */
  @Override
  public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {
    // don't sync privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip syncing privileges with Ranger server for onAlterTable event," +
          " since the operation failed. \n");
      return;
    }

    // drop the privileges on the given table
    if (!rangerConfigureIsTrue(RangerHadoopConstants.SYNC_ALTER_WITH_POLICY_STORE)) {
      return;
    }

    synchronizePolicy(tableEvent, HiveOperationType.ALTERTABLE);
  }

  public void synchronizePolicy(ListenerEvent tableEvent, HiveOperationType hiveOperationType) throws MetaException {
    UserGroupInformation ugi;
    try {
      ugi = Utils.getUGI();
    } catch (Exception exp) {
      throw new MetaException(exp.getMessage());
    }

    HivePrincipal grantorPrincipal = null;
    List<HivePrincipal> hivePrincipals = new ArrayList<HivePrincipal>();
    String[] groupNames = ugi.getGroupNames();
    String userName = ugi.getShortUserName();
    if (!userName.isEmpty()) {
      HivePrincipal hivePrincipal = new HivePrincipal(userName, HivePrincipal.HivePrincipalType.USER);
      hivePrincipals.add(hivePrincipal);
      grantorPrincipal = new HivePrincipal(userName, HivePrincipal.HivePrincipalType.USER);
    }
    for (int i = 0; i < groupNames.length; i ++) {
      HivePrincipal hivePrincipal = new HivePrincipal(groupNames[i], HivePrincipal.HivePrincipalType.GROUP);
      hivePrincipals.add(hivePrincipal);
    }

    HivePrivilegeObject.HivePrivilegeObjectType hivePrivilegeObjectType = HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW;
    String dbName = "", objName = "";
    String newDbName = null, newObjName = null;
    String tableType = "";
    String location = "";
    String newLocation = "";
    HiveAccessType hiveAccessType = HiveAccessType.NONE;
    switch (hiveOperationType) {
      case CREATEDATABASE:
        hivePrivilegeObjectType = HivePrivilegeObject.HivePrivilegeObjectType.DATABASE;
        hiveAccessType = HiveAccessType.CREATE;
        Database createDb = ((CreateDatabaseEvent)tableEvent).getDatabase();
        dbName = createDb.getName();
        if (createDb.getLocationUri() != null) {
          location = createDb.getLocationUri();
        }
        break;
      case DROPDATABASE:
        hivePrivilegeObjectType = HivePrivilegeObject.HivePrivilegeObjectType.DATABASE;
        hiveAccessType = HiveAccessType.DROP;
        Database dropDb = ((DropDatabaseEvent)tableEvent).getDatabase();
        dbName = dropDb.getName();
        if (dropDb.getLocationUri() != null) {
          location = dropDb.getLocationUri();
        }
        break;
      case CREATETABLE:
        hivePrivilegeObjectType = HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW;
        hiveAccessType = HiveAccessType.CREATE;
        Table createTable = ((CreateTableEvent)tableEvent).getTable();
        dbName = createTable.getDbName();
        objName = createTable.getTableName();
        tableType = createTable.getTableType();
        if (createTable.getSd().getLocation() != null) {
          location = createTable.getSd().getLocation();
        }
        break;
      case DROPTABLE:
        hivePrivilegeObjectType = HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW;
        hiveAccessType = HiveAccessType.DROP;
        Table dropTable = ((DropTableEvent)tableEvent).getTable();
        dbName = dropTable.getDbName();
        objName = dropTable.getTableName();
        if (dropTable.getSd().getLocation() != null) {
          location = dropTable.getSd().getLocation();
        }
        break;
      case ALTERTABLE:
        hivePrivilegeObjectType = HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW;
        hiveAccessType = HiveAccessType.ALTER;
        AlterTableEvent alterTable = ((AlterTableEvent)tableEvent);
        dbName = alterTable.getOldTable().getDbName();
        objName = alterTable.getOldTable().getTableName();
        if (alterTable.getOldTable().getSd().getLocation() != null) {
          location = alterTable.getOldTable().getSd().getLocation();
        }
        newDbName = alterTable.getNewTable().getDbName();
        newObjName = alterTable.getNewTable().getTableName();
        if (alterTable.getNewTable().getSd().getLocation() != null) {
          newLocation = alterTable.getNewTable().getSd().getLocation();
        }
        break;
      default:
        LOGGER.error("can not match HiveOperationType : " + hiveOperationType);
        return;
    }

    HivePrivilegeObject hivePrivilegeObject = new HivePrivilegeObject(hivePrivilegeObjectType, dbName, objName);
    RangerHiveResource resource = getHiveResource(hiveOperationType, hivePrivilegeObject);
    RangerHiveResource newResource = null;
    if (null != newDbName) {
      HivePrivilegeObject newHivePrivilegeObject = new HivePrivilegeObject(hivePrivilegeObjectType, newDbName, newObjName);
      newResource = getHiveResource(hiveOperationType, newHivePrivilegeObject);
    }
    try {
      HivePrivilege hivePrivilege = new HivePrivilege(hiveAccessType.name(), (List)null);
      List<HivePrivilege> hivePrivileges = new ArrayList<HivePrivilege>();
      hivePrivileges.add(hivePrivilege);
      SynchronizeRequest request  = createSyncPolicyRequest(resource, newResource, hivePrincipals,
          hivePrivileges, grantorPrincipal, tableType, location, newLocation);

      LOGGER.info("add queue > " + request.toString() + ", " + hiveOperationType);
      syncRequestQueue.add(new SyncRequestStruct(request, hiveOperationType));
    } catch(Exception excp) {
      throw new MetaException(excp.getMessage());
    } finally {

    }
  }

  private class SyncRequestStruct {
    SynchronizeRequest syncRequest;
    HiveOperationType hiveOperationType;

    public SyncRequestStruct(SynchronizeRequest syncRequest, HiveOperationType hiveOperationType) {
      this.syncRequest = syncRequest;
      this.hiveOperationType = hiveOperationType;
    }
  }

  // the consumer TUpdateDelta from the queue
  class SyncPoliciesRunnable implements Runnable {
    ConcurrentLinkedQueue<SyncRequestStruct> queue;

    SyncPoliciesRunnable(ConcurrentLinkedQueue<SyncRequestStruct> queue) throws IOException {
      this.queue = queue;
    }

    public void run() {
      while (true) {
        try {
          SyncRequestStruct syncRequestStruct = null;
          while ((syncRequestStruct = queue.poll()) != null) {
            LOGGER.info("SyncPoliciesRunnable " + syncRequestStruct.syncRequest.toString() + ", " + syncRequestStruct.hiveOperationType);
            rangerPlugin.getRangerAdminClient().syncPolicys(syncRequestStruct.syncRequest, syncRequestStruct.hiveOperationType);
          }
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    }
  }

  private SynchronizeRequest createSyncPolicyRequest(RangerHiveResource  resource,
                                                    RangerHiveResource  newResource,
                                                    List<HivePrincipal> hivePrincipals,
                                                    List<HivePrivilege> hivePrivileges,
                                                    HivePrincipal       grantorPrincipal,
                                                    String              tableType,
                                                    String              location,
                                                    String              newLocation)
      throws MetaException {
    if(resource == null ||
        ! ( resource.getObjectType() == HiveObjectType.DATABASE
            || resource.getObjectType() == HiveObjectType.TABLE
            || resource.getObjectType() == HiveObjectType.VIEW
            || resource.getObjectType() == HiveObjectType.COLUMN
            || resource.getObjectType() == HiveObjectType.PARTITION )) {
      throw new MetaException("createSyncPolicyRequest: unexpected object type '"
          + (resource == null ? null : resource.getObjectType().name()));
    }

    SynchronizeRequest ret = new SynchronizeRequest();

    ret.setGrantor(getGrantorUsername(grantorPrincipal));
    ret.setDelegateAdmin(Boolean.TRUE);
    ret.setEnableAudit(Boolean.TRUE);
    ret.setReplaceExistingPermissions(Boolean.FALSE);
    ret.setTableType(tableType);
    ret.setLocation(location);
    ret.setNewLocation(newLocation);

    String database = StringUtils.isEmpty(resource.getDatabase()) ? "*" : resource.getDatabase();
    String table    = StringUtils.isEmpty(resource.getTable()) ? "*" : resource.getTable();
    String column   = StringUtils.isEmpty(resource.getColumn()) ? "*" : resource.getColumn();
    Map<String, String> mapResource = new HashMap<String, String>();
    mapResource.put(RangerHiveResource.KEY_DATABASE, database);
    mapResource.put(RangerHiveResource.KEY_TABLE, table);
    mapResource.put(RangerHiveResource.KEY_COLUMN, column);
    ret.setResource(mapResource);

    if (null != newResource) {
      String newDatabase = StringUtils.isEmpty(newResource.getDatabase()) ? "*" : newResource.getDatabase();
      String newTable = StringUtils.isEmpty(newResource.getTable()) ? "*" : newResource.getTable();
      String newColumn = StringUtils.isEmpty(newResource.getColumn()) ? "*" : newResource.getColumn();
      Map<String, String> mapNewResource = new HashMap<String, String>();
      mapNewResource.put(RangerHiveResource.KEY_DATABASE, newDatabase);
      mapNewResource.put(RangerHiveResource.KEY_TABLE, newTable);
      mapNewResource.put(RangerHiveResource.KEY_COLUMN, newColumn);
      ret.setNewResource(mapNewResource);
    }

    InetAddress inetAddress = null;
    try {
      inetAddress = InetAddress.getLocalHost();
      String ipAddress = inetAddress.getHostAddress();
      ret.setClientIPAddress(ipAddress);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    ret.setClientType(appId);

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
        LOGGER.warn("createSyncPolicyRequest: unexpected privilege type '" + privName + "'. Ignored");
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

  private void auditConsistentRules(GrantRevokeRequest request, String action, boolean isSuccess,
                                    RangerAccessResultProcessor resultProcessor) {
    if(request != null && resultProcessor != null) {
      RangerHiveAccessRequest accessRequest = new RangerHiveAccessRequest();
      RangerAccessResourceImpl rangerAccessResource = new RangerAccessResourceImpl(request.getResource());
      rangerAccessResource.setServiceDef(rangerPlugin.getServiceDef());
      accessRequest.setResource(rangerAccessResource);
      accessRequest.setUser(request.getGrantor());
      accessRequest.setAccessType(action);
      accessRequest.setAction(action);
      accessRequest.setClientIPAddress(request.getClientIPAddress());
      accessRequest.setClientType(request.getClientType());
      accessRequest.setRequestData(request.getRequestData());
      accessRequest.setSessionId(request.getSessionId());
      accessRequest.getResource().getServiceDef();

      RangerAccessResult accessResult = new RangerAccessResult(serviceName, rangerPlugin.getServiceDef(), accessRequest);
      accessResult.setIsAllowed(isSuccess);
      accessResult.setIsAudited(true);
      accessResult.setReason("consistent rule");
      if(! isSuccess) {
        accessResult.setPolicyId(-1);
      }
      resultProcessor.processResult(accessResult);
    }
  }

  private RangerHiveResource getHiveResource(HiveOperationType hiveOpType, HivePrivilegeObject hiveObj) {
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
}
