///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.ranger.binding.metastore;
//
//
//import com.google.common.collect.ImmutableSet;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Sets;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hive.conf.HiveConf;
//import org.apache.hadoop.hive.metastore.ObjectStore;
//import org.apache.hadoop.hive.metastore.api.*;
//import org.apache.hadoop.hive.metastore.api.HiveObjectType;
//import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
//import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
//import org.apache.hadoop.hive.ql.security.authorization.plugin.*;
//import org.apache.hadoop.hive.ql.session.SessionState;
//import org.apache.hadoop.hive.shims.Utils;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.hadoop.security.token.Token;
//import org.apache.ranger.authorization.hive.authorizer.*;
//import org.apache.ranger.plugin.policyengine.RangerAccessResult;
//import org.apache.ranger.plugin.service.RangerBasePlugin;
//import org.apache.ranger.plugin.service.RangerPluginSingleton;
//
//import javax.security.auth.login.LoginException;
//import java.io.IOException;
//import java.util.*;
//
//public class AuthorizingObjectStore extends ObjectStore {
//  private static final Log LOG = LogFactory.getLog(AuthorizingObjectStore.class);
//
//  private static HiveConf hiveConf;
//  private static String NO_ACCESS_MESSAGE_TABLE = "Table does not exist or insufficient privileges to access: ";
//  private static String NO_ACCESS_MESSAGE_DATABASE = "Database does not exist or insufficient privileges to access: ";
//
////  private static RangerHiveAuthorizer hiveAuthorizer;
//
//  private static RangerBasePlugin hivePlugin = null;
//
//  public void AuthorizingObjectStore() {
//    hivePlugin = RangerPluginSingleton.getInstance();
//    hivePlugin.init("hive", "metastore");
//  }
//
//  @Override
//  public List<String> getDatabases(String pattern) throws MetaException {
//    return filterDatabases(super.getDatabases(pattern), true);
//  }
//
//  @Override
//  public List<String> getAllDatabases() throws MetaException {
//    return filterDatabases(super.getAllDatabases(), true);
//  }
//
//  @Override
//  public Database getDatabase(String name) throws NoSuchObjectException {
//    Database db = super.getDatabase(name);
//    try {
//      if (filterDatabases(Lists.newArrayList(name), false).isEmpty()) {
//        throw new NoSuchObjectException(getNoAccessMessageForDB(name));
//      }
//    } catch (MetaException e) {
//      throw new NoSuchObjectException("Failed to authorized access to " + name
//          + " : " + e.getMessage());
//    }
//    return db;
//  }
//
//  @Override
//  public Table getTable(String dbName, String tableName) throws MetaException {
//    Table table = super.getTable(dbName, tableName);
//
//    /*
//    List<FieldSchema> cols = new ArrayList<>();
//    FieldSchema fieldSchema = new FieldSchema("col1", "int", "");
//    cols.add(fieldSchema);
//    table.getSd().setCols(cols);
//*/
//    if (table == null || filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
//      return null;
//    }
//    return table;
//  }
//
//  @Override
//  public Partition getPartition(String dbName, String tableName,
//                                List<String> part_vals) throws MetaException, NoSuchObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
//      throw new NoSuchObjectException(getNoAccessMessageForTable(dbName, tableName));
//    }
//    return super.getPartition(dbName, tableName, part_vals);
//  }
//
//  @Override
//  public List<Partition> getPartitions(String dbName, String tableName,
//                                       int maxParts) throws MetaException, NoSuchObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
//    }
//    return super.getPartitions(dbName, tableName, maxParts);
//  }
//
//  @Override
//  public List<String> getTables(String dbName, String pattern)
//      throws MetaException {
//    return filterTables(dbName, super.getTables(dbName, pattern));
//  }
//
//  @Override
//  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
//      throws MetaException, UnknownDBException {
//    return super.getTableObjectsByName(dbname, filterTables(dbname, tableNames));
//  }
//
//  @Override
//  public List<String> getAllTables(String dbName) throws MetaException {
//    return filterTables(dbName, super.getAllTables(dbName));
//  }
//
//  @Override
//  public List<String> listTableNamesByFilter(String dbName, String filter,
//                                             short maxTables) throws MetaException {
//    return filterTables(dbName,
//        super.listTableNamesByFilter(dbName, filter, maxTables));
//  }
//
//  @Override
//  public List<String> listPartitionNames(String dbName, String tableName,
//                                         short max_parts) throws MetaException {
//    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
//    }
//    return super.listPartitionNames(dbName, tableName, max_parts);
//  }
//
//  @Override
//  public List<String> listPartitionNamesByFilter(String dbName,
//                                                 String tableName, String filter, short max_parts) throws MetaException {
//    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
//    }
//    return super.listPartitionNamesByFilter(dbName, tableName, filter,
//        max_parts);
//  }
//
//  @Override
//  public Index getIndex(String dbName, String origTableName, String indexName)
//      throws MetaException {
//    if (filterTables(dbName, Lists.newArrayList(origTableName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, origTableName));
//    }
//    return super.getIndex(dbName, origTableName, indexName);
//  }
//
//  @Override
//  public List<Index> getIndexes(String dbName, String origTableName, int max)
//      throws MetaException {
//    if (filterTables(dbName, Lists.newArrayList(origTableName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, origTableName));
//    }
//    return super.getIndexes(dbName, origTableName, max);
//  }
//
//  @Override
//  public List<String> listIndexNames(String dbName, String origTableName,
//                                     short max) throws MetaException {
//    if (filterTables(dbName, Lists.newArrayList(origTableName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, origTableName));
//    }
//    return super.listIndexNames(dbName, origTableName, max);
//  }
//
//  @Override
//  public List<Partition> getPartitionsByFilter(String dbName,
//                                               String tblName, String filter, short maxParts) throws MetaException,
//      NoSuchObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
//    }
//    return super.getPartitionsByFilter(dbName, tblName, filter, maxParts);
//  }
//
//  @Override
//  public List<Partition> getPartitionsByNames(String dbName, String tblName,
//                                              List<String> partNames) throws MetaException, NoSuchObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
//    }
//    return super.getPartitionsByNames(dbName, tblName, partNames);
//  }
//
//  @Override
//  public Partition getPartitionWithAuth(String dbName, String tblName,
//                                        List<String> partVals, String user_name, List<String> group_names)
//      throws MetaException, NoSuchObjectException, InvalidObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
//    }
//    return super.getPartitionWithAuth(dbName, tblName, partVals, user_name,
//        group_names);
//  }
//
//  @Override
//  public List<Partition> getPartitionsWithAuth(String dbName, String tblName,
//                                               short maxParts, String userName, List<String> groupNames)
//      throws MetaException, NoSuchObjectException, InvalidObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
//    }
//    return super.getPartitionsWithAuth(dbName, tblName, maxParts, userName,
//        groupNames);
//  }
//
//  @Override
//  public List<String> listPartitionNamesPs(String dbName, String tblName,
//                                           List<String> part_vals, short max_parts) throws MetaException,
//      NoSuchObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
//    }
//    return super.listPartitionNamesPs(dbName, tblName, part_vals, max_parts);
//  }
//
//  @Override
//  public List<Partition> listPartitionsPsWithAuth(String dbName,
//                                                  String tblName, List<String> part_vals, short max_parts, String userName,
//                                                  List<String> groupNames) throws MetaException, InvalidObjectException,
//      NoSuchObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
//    }
//    return super.listPartitionsPsWithAuth(dbName, tblName, part_vals,
//        max_parts, userName, groupNames);
//  }
//
//  @Override
//  public ColumnStatistics getTableColumnStatistics(String dbName,
//                                                   String tableName, List<String> colNames) throws MetaException,
//      NoSuchObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tableName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tableName));
//    }
//    return super.getTableColumnStatistics(dbName, tableName, colNames);
//  }
//
//  @Override
//  public List<ColumnStatistics> getPartitionColumnStatistics(
//      String dbName, String tblName, List<String> partNames,
//      List<String> colNames) throws MetaException, NoSuchObjectException {
//    if (filterTables(dbName, Lists.newArrayList(tblName)).isEmpty()) {
//      throw new MetaException(getNoAccessMessageForTable(dbName, tblName));
//    }
//    return super.getPartitionColumnStatistics(dbName, tblName, partNames,
//        colNames);
//  }
//
//  /**
//   * Invoke Hive database filtering that removes the entries which use has no
//   * privileges to access
//   * @param dbList
//   * @return
//   * @throws MetaException
//   */
//  private List<String> filterDatabases(List<String> dbList, boolean getAll)
//      throws MetaException {
//
//    List<String> retList = new ArrayList<>();
//    List<HivePrivilegeObject> ret = null;
//    List<HivePrivilegeObject> objs = new ArrayList<>();
//    HiveAuthzContext.Builder authzContextBuilder = new HiveAuthzContext.Builder();
//    authzContextBuilder.setUserIpAddress("");
//    authzContextBuilder.setCommandString("");
//
//    for (String dbName : dbList) {
//      HivePrivilegeObject hivePrivilegeObject
//          = new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DATABASE, dbName, dbName);
//      objs.add(hivePrivilegeObject);
//    }
//
//    try {
//      ret = getHiveAuthorizer().filterListCmdObjects(objs, authzContextBuilder.build());
//      for (HivePrivilegeObject object : ret) {
//        retList.add(object.getDbname());
//      }
//      if(!retList.contains("default") && !getAll) {
//        retList.add("default");
//      }
//    } catch (HiveAuthzPluginException e) {
//      throw new MetaException(e.getMessage());
//    } catch (HiveAccessControlException e) {
//      throw new MetaException(e.getMessage());
//    }
//
//    return retList;
//  }
//
//  /**
//   * Invoke Hive table filtering that removes the entries which use has no
//   * privileges to access
//   * @param dbName
//   * @param tabList
//   * @return
//   * @throws MetaException
//   */
//  protected List<String> filterTables(String dbName, List<String> tabList)
//      throws MetaException {
//    HiveOperationType hiveOpType;
//    List<HivePrivilegeObject> inputHObjs;
//    List<HivePrivilegeObject> outputHObjs;
//
//    List<String> retList = new ArrayList<>();
//    List<HivePrivilegeObject> ret = null;
//    List<HivePrivilegeObject> objs = new ArrayList<>();
//    HiveAuthzContext.Builder authzContextBuilder = new HiveAuthzContext.Builder();
//    authzContextBuilder.setUserIpAddress("");
//    authzContextBuilder.setCommandString("");
//
//    for (String tabName : tabList) {
//      HivePrivilegeObject hivePrivilegeObject
//          = new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.TABLE_OR_VIEW, dbName, tabName);
//      objs.add(hivePrivilegeObject);
//    }
//
//    try {
//      ret = getHiveAuthorizer().filterListCmdObjects(objs, authzContextBuilder.build());
//      for (HivePrivilegeObject object : ret) {
//        retList.add(object.getObjectName());
//      }
//    } catch (HiveAuthzPluginException e) {
//      throw new MetaException(e.getMessage());
//    } catch (HiveAccessControlException e) {
//      throw new MetaException(e.getMessage());
//    }
//
//    return retList;
//  }
//
//  /**
//   * load Hive auth provider
//   *
//   * @return
//   * @throws MetaException
//   */
//  private RangerHiveAuthorizer getHiveAuthorizer() throws MetaException {
//    String userName = getUserName();
//    RangerHiveAuthorizer hiveAuthorizer = null;
////    if (hiveAuthorizer == null) {
////      synchronized (RangerHiveAuthorizer.class) {
//        try {
//          HiveMetastoreClientFactory metastoreClientFactory = new HiveMetastoreClientFactoryImpl();
//          SessionStateUserAuthenticator hiveAuthenticator = new SessionStateUserAuthenticator();
//          SessionState sessionState = new SessionState(getHiveConf(), getUserName());
//          hiveAuthenticator.getGroupNames().addAll(getGroupNames());
//          hiveAuthenticator.setSessionState(sessionState);
//
//          HiveAuthzSessionContext.Builder authzContextBuilder = new HiveAuthzSessionContext.Builder();
//          authzContextBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.HIVECLI);
//          hiveAuthorizer = new RangerHiveAuthorizer(metastoreClientFactory, getHiveConf(), hiveAuthenticator, authzContextBuilder.build());
//        } catch (Exception e) {
//          throw new MetaException("Failed to load Hive binding " + e.getMessage());
//        }
////      }
////    }
//    return hiveAuthorizer;
//  }
//
//  private HiveConf getHiveConf() {
//    if (hiveConf == null) {
//      hiveConf = new HiveConf(getConf(), this.getClass());
//    }
//    return hiveConf;
//  }
//
//  public UserGroupInformation getCurrentUserGroupInfo() {
//    UserGroupInformation ugi = null;
//    try {
//      ugi = Utils.getUGI();
//    } catch (Exception exp) {
//      LOG.error(exp.getMessage());
//    }
//    return ugi;
//  }
//
//  /**
//   * Check if user has privileges to do this action on these objects
//   * @param hiveOpType
//   * @param context
//   * @throws MetaException
//   */
//  public void checkPrivileges(HiveOperationType hiveOpType,
//                              List<HivePrivilegeObject> inputHObjs,
//                              List<HivePrivilegeObject> outputHObjs,
//                              HiveAuthzContext          context)
//      throws MetaException {
//    try {
//      getHiveAuthorizer().checkPrivileges(hiveOpType, inputHObjs, outputHObjs, context);
//    } catch (HiveAuthzPluginException e) {
//      e.printStackTrace();
//      throw new MetaException(e.getMessage());
//    } catch (HiveAccessControlException e) {
//      throw new MetaException(e.getMessage());
//    }
//  }
//
//  /**
//   * Extract the user from underlying auth subsystem
//   * @return
//   * @throws MetaException
//   */
//  private String getUserName() throws MetaException {
//    try {
//      UserGroupInformation ugi = Utils.getUGI();
//      for (Token t : ugi.getTokens()) {
//        LOG.debug("Token {} is available" + t);
//      }
//      return ugi.getShortUserName();
//    } catch (LoginException e) {
//      throw new MetaException("Failed to get username " + e.getMessage());
//    } catch (IOException e) {
//      throw new MetaException("Failed to get username " + e.getMessage());
//    }
//  }
//
//  private List<String> getGroupNames() throws MetaException {
//    try {
//      return Arrays.asList(Utils.getUGI().getGroupNames());
//    } catch (LoginException e) {
//      throw new MetaException("Failed to get username " + e.getMessage());
//    } catch (IOException e) {
//      throw new MetaException("Failed to get username " + e.getMessage());
//    }
//  }
//
//  /**
//   * Check if the give user needs to be validated.
//   * @param userName
//   * @return
//   */
//  private boolean needsAuthorization(String userName) throws MetaException {
//    return false; // !getServiceUsers().contains(userName.trim());
//  }
//
//  private static Set<String> toTrimed(Set<String> s) {
//    Set<String> result = Sets.newHashSet();
//    for (String v : s) {
//      result.add(v.trim());
//    }
//    return result;
//  }
//
//  protected String getNoAccessMessageForTable(String dbName, String tableName) {
//    return NO_ACCESS_MESSAGE_TABLE + "<" + dbName + ">.<" + tableName + ">";
//  }
//
//  private String getNoAccessMessageForDB(String dbName) {
//    return NO_ACCESS_MESSAGE_DATABASE + "<" + dbName + ">";
//  }
//}
