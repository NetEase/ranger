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

import java.sql.*;
import java.util.*;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.apache.ranger.view.VXTrxLog;
import org.apache.ranger.view.VXTrxLogList;
import org.junit.Test;

public class TestSyncHdfsPolicyREST {
  public static final String KEY_DATABASE = "database";
  public static final String KEY_TABLE    = "table";
  public static final String KEY_UDF      = "udf";
  public static final String KEY_COLUMN   = "column";

  private static final String KRB_USRE = "hadoop/admin@HADOOP.HZ.NETEASE.COM";
  private static final String KRB_PATH = "/Users/apple/Downloads/hadoop.keytab";
  private static final String PROXY_USER = "hadoop";
  private static final String USER_NAME = "hadoop";
  private static final String USER_GROUP = "liuxun_group";
  private static final String CONNECT_STRING = "jdbc:hive2://hadoop354.lt.163.org:10000/default;principal=hive/app-20.photo.163.org@HADOOP.HZ.NETEASE.COM;";
  private static Connection _connection = null;

  // RangerAdmin
  private static final String RANGER_ADMIN_URL = "http://hadoop357.lt.163.org:6080";
  private static final String RANGER_AUTH_NAME = "admin";
  private static final String RANGER_AUTH_PASSWD = "admin";
  private static final String RANGER_SERVICE_NAME = "hivedev";

  // RangerAdmin REST interface
  private final String REST_URL_POLICY_CREATE = "/service/plugins/policies";
  private final String REST_URL_POLICY_DELETE = "/service/plugins/policies/";
  private final String REST_URL_ASSETS_REPORT = "/service/assets/report";
  private final long WAITING_SYNC_MILLIS = 10 * 1000; // Waiting for hive plugin synchronization policy data

  // test
  private final String TEST_DATABASE_NAME = "SyncTestDB";
  private final String TEST_TABLE_NAME = "SyncTestTable";
  private final String TEST_TABLE_RENAME = "SyncTestTable_rename";
  private final String TEST_COLUMN_NAME = "";
  private final String LOCATION = "hdfs://hz-cluster1/tmp/liuxun_test,hdfs://mycluster/tmp/liuxun_test";

  RangerPolicy dbHivePolicy = null;
  RangerPolicy tableHivePolicy = null;
  private long POLICY_ID = System.currentTimeMillis();

  @Test
  public void TestSyncHdfsPolicyREST() {
    boolean throwException = false;
    try {
      init();

      dbHivePolicy = generateHivePolicy(TEST_DATABASE_NAME, "*", "*");
      tableHivePolicy = generateHivePolicy(TEST_DATABASE_NAME, TEST_TABLE_NAME, "*");

      // Waiting for hive plugin synchronization policy
      System.out.println("==> Waiting for hive plugin synchronization policy ... [" + WAITING_SYNC_MILLIS + "ms]");
      Thread.sleep(WAITING_SYNC_MILLIS);
      System.out.println("<== Waiting for hive plugin synchronization policy ... [" + WAITING_SYNC_MILLIS + "ms]");

      databaseTest();
      checkSyncHdfsPolicy();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throwException = true;
    } catch (SQLException e) {
      e.printStackTrace();
      throwException = true;
    } catch (Exception e) {
      e.printStackTrace();
      throwException = true;
    }

    if (throwException) {
      try {
        rollbackEnv();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private void init() {
    try {
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
      Configuration conf = new Configuration();
      conf.set("hadoop.security.authentication", "kerberos");
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(KRB_USRE, KRB_PATH);

      Class.forName("org.apache.hive.jdbc.HiveDriver");
      _connection = DriverManager.getConnection(CONNECT_STRING +
              "hive.server2.proxy.user=" + PROXY_USER + "#ranger.user.name=" + USER_NAME);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  private boolean checkSyncHdfsPolicy() {
    System.out.println("==> TestSyncHdfsPolicyREST.getVXTrxLog()");

    VXTrxLogList ret = null;
    RangerRESTClient restClient = new RangerRESTClient();
    restClient.setBasicAuthInfo(RANGER_AUTH_NAME, RANGER_AUTH_PASSWD);
    restClient.setUrl(RANGER_ADMIN_URL);

    WebResource webResource = restClient.getResource(REST_URL_ASSETS_REPORT)
        .queryParam("sortBy", "createDate")
        .queryParam("sortType", "asc")
        .queryParam("page", "0")
        .queryParam("pageSize", "100")
        .queryParam("startIndex", "0")
        .queryParam("objectName", String.valueOf(POLICY_ID));
    ClientResponse response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).get(ClientResponse.class);
    if(response != null && response.getStatus() == 200) {
      ret = response.getEntity(VXTrxLogList.class);
    } else {
      RESTResponse resp = RESTResponse.fromClientResponse(response);
      System.out.print(resp.getMessage());
      System.exit(1);
    }

    System.out.println("---------------------------------------------------------------------------");
    System.out.println("action | services | preValue --> newValue");
    System.out.println("---------------------------------------------------------------------------");
    for (VXTrxLog vxTrxLog : ret.getList()) {
      System.out.println(vxTrxLog.getAction()+" | "+vxTrxLog.getParentObjectName()+" | "+vxTrxLog.getPreviousValue()+" --> " + vxTrxLog.getNewValue());
    }
    System.out.println("---------------------------------------------------------------------------");

    if (ret.getListSize() != 16) {
      System.out.println("Ranger Policy are synchronized error");
      return false;
    } else {
      System.out.println("Ranger Policy are synchronized correct");
    }

    System.out.println("<== TestSyncHdfsPolicyREST.getVXTrxLog()");
    return true;
  }

  private RangerPolicy createHivePolicy(String databaseName, String tableName, String columnName, List<String> accessTypes) {
    RangerPolicy policy = new RangerPolicy();

    policy.setService(RANGER_SERVICE_NAME);
    policy.setName("TestSyncHdfsPolicyREST-" + databaseName + (tableName.equalsIgnoreCase("*")?"":tableName) + POLICY_ID);
    policy.setDescription("auto created by TestSyncHdfsPolicyREST");
    policy.setIsAuditEnabled(true);
    policy.setCreatedBy(USER_NAME);

    Map<String, RangerPolicy.RangerPolicyResource> policyResource = new HashMap<String, RangerPolicy.RangerPolicyResource>();
    RangerPolicy.RangerPolicyResource dbResource = new RangerPolicy.RangerPolicyResource();
    dbResource.setIsExcludes(false);
    dbResource.setIsRecursive(false);
    dbResource.setValue(databaseName);
    policyResource.put(KEY_DATABASE, dbResource);

    RangerPolicy.RangerPolicyResource tabResource = new RangerPolicy.RangerPolicyResource();
    tabResource.setIsExcludes(false);
    tabResource.setIsRecursive(false);
    tabResource.setValue(tableName);
    policyResource.put(KEY_TABLE, tabResource);

    RangerPolicy.RangerPolicyResource columnResource = new RangerPolicy.RangerPolicyResource();
    columnResource.setIsExcludes(false);
    columnResource.setIsRecursive(false);
    columnResource.setValue(columnName);
    policyResource.put(KEY_COLUMN, columnResource);
    policy.setResources(policyResource);

    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.getUsers().add(USER_NAME);
    policyItem.getGroups().add(USER_GROUP);
    for(String accessType : accessTypes) {
      policyItem.getAccesses().add(new RangerPolicy.RangerPolicyItemAccess(accessType, Boolean.TRUE));
    }
    policyItem.setDelegateAdmin(true);
    policy.getPolicyItems().add(policyItem);

    return policy;
  }

  private RangerPolicy generateHivePolicy(String databaseName, String tableName, String columnName) throws Exception {
    System.out.println("==> TestSyncHdfsPolicyREST.generateHivePolicy(" + databaseName + ", " + tableName + ", " + columnName + ")");
    List<String> accessTypes = new ArrayList<>();
    accessTypes.add(ServiceREST.HiveAccessType.CREATE.name());
    accessTypes.add(ServiceREST.HiveAccessType.ALTER.name());
    accessTypes.add(ServiceREST.HiveAccessType.DROP.name());
    accessTypes.add(ServiceREST.HiveAccessType.SELECT.name());
    accessTypes.add(ServiceREST.HiveAccessType.UPDATE.name());
    accessTypes.add(ServiceREST.HiveAccessType.ALL.name());
    RangerPolicy policy = createHivePolicy(databaseName, tableName, columnName, accessTypes);

    RangerPolicy ret = null;
    RangerRESTClient restClient = new RangerRESTClient();
    restClient.setBasicAuthInfo(RANGER_AUTH_NAME, RANGER_AUTH_PASSWD);
    restClient.setUrl(RANGER_ADMIN_URL);

    WebResource webResource = restClient.getResource(REST_URL_POLICY_CREATE);
    ClientResponse response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON)
        .type(RangerRESTUtils.REST_MIME_TYPE_JSON).post(ClientResponse.class, restClient.toJson(policy));
    if(response != null && response.getStatus() == 200) {
      ret = response.getEntity(RangerPolicy.class);
    } else {
      RESTResponse resp = RESTResponse.fromClientResponse(response);
      throw new Exception(resp.getMessage());
    }

    System.out.println("<== TestSyncHdfsPolicyREST.generateHivePolicy(" + databaseName + ", " + tableName + ", " + columnName + "): " + ret);

    return ret;
  }

  private void rollbackEnv() throws Exception {
    System.out.println("==> TestSyncHdfsPolicyREST.rollbackEnv()");
    Statement statement = _connection.createStatement();

    String sql = "";
    try {
      sql = "use " + TEST_DATABASE_NAME;
      ASSERT_EXECUTE_SUCCESS(statement, sql);

      // ALTER TABLE tablename RENAME TO "original tablename";
      sql = "ALTER TABLE " + TEST_TABLE_RENAME + " RENAME TO " + TEST_TABLE_NAME;
      ASSERT_EXECUTE_SUCCESS(statement, sql);
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      // ALTER TABLE SyncTestTable SET LOCATION "original location"
      String[] locations = LOCATION.split(",");
      sql = "ALTER TABLE " + TEST_TABLE_NAME + " SET LOCATION \"" + locations[locations.length-1] + "\"";
      ASSERT_EXECUTE_SUCCESS(statement, sql);
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      // DROP TABLE tablename
      sql = "DROP TABLE " + TEST_TABLE_NAME;
      ASSERT_EXECUTE_SUCCESS(statement, sql);
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      // DROP DATABASE databasename
      sql = "DROP DATABASE " + TEST_DATABASE_NAME;
      ASSERT_EXECUTE_SUCCESS(statement, sql);
    } catch (Exception e) {
      e.printStackTrace();
    }
    statement.close();

    deletePolicy(tableHivePolicy.getId());
    deletePolicy(dbHivePolicy.getId());

    System.out.println("<== TestSyncHdfsPolicyREST.rollbackEnv()");

  }

  private boolean deletePolicy(Long policyId) throws Exception {
    System.out.println("==> TestSyncHdfsPolicyREST.deletePolicy(" + policyId + ")");

    boolean ret = true;
    RangerRESTClient restClient = new RangerRESTClient();
    restClient.setBasicAuthInfo(RANGER_AUTH_NAME, RANGER_AUTH_PASSWD);
    restClient.setUrl(RANGER_ADMIN_URL);

    WebResource webResource = restClient.getResource(REST_URL_POLICY_DELETE + policyId);
    ClientResponse response = webResource.accept(RangerRESTUtils.REST_MIME_TYPE_JSON).delete(ClientResponse.class);
    if(response == null || (response.getStatus() != 200 && response.getStatus() != 204)) {
      ret = false;
      RESTResponse resp = RESTResponse.fromClientResponse(response);
      throw new Exception(resp.getMessage());
    }

    System.out.println("<== TestSyncHdfsPolicyREST.deletePolicy(" + policyId + ")");

    return ret;
  }

  private void databaseTest() throws Exception {
    System.out.println("==> TestSyncHdfsPolicyREST.databaseTest()");
    Statement statement = null;

    statement = _connection.createStatement();

    // CREATE DATABASE databasename
    String sql = "CREATE DATABASE " + TEST_DATABASE_NAME;
    ASSERT_EXECUTE_SUCCESS(statement, sql);
    sql = "use " + TEST_DATABASE_NAME;
    ASSERT_EXECUTE_SUCCESS(statement, sql);

    // CREATE TABLE tablename
    sql = "CREATE TABLE " + TEST_TABLE_NAME + " (col1 int, col2 int)";
    ASSERT_EXECUTE_SUCCESS(statement, sql);

    // ALTER TABLE tablename RENAME TO tablename_rename;
    sql = "ALTER TABLE " + TEST_TABLE_NAME + " RENAME TO " + TEST_TABLE_RENAME;
    ASSERT_EXECUTE_SUCCESS(statement, sql);
    // ALTER TABLE tablename_rename RENAME TO tablename;
    sql = "ALTER TABLE " + TEST_TABLE_RENAME + " RENAME TO " + TEST_TABLE_NAME;
    ASSERT_EXECUTE_SUCCESS(statement, sql);

    // ALTER TABLE SyncTestTable SET LOCATION "/tmp/liuxun_test"
    String[] locations = LOCATION.split(",");
    for (String location : locations) {
      sql = "ALTER TABLE " + TEST_TABLE_NAME + " SET LOCATION \"" + location + "\"";
      ASSERT_EXECUTE_SUCCESS(statement, sql);
    }

    // DROP TABLE tablename
    sql = "DROP TABLE " + TEST_TABLE_NAME;
    ASSERT_EXECUTE_SUCCESS(statement, sql);

    // DROP DATABASE databasename
    sql = "DROP DATABASE " + TEST_DATABASE_NAME;
    ASSERT_EXECUTE_SUCCESS(statement, sql);

    statement.close();

    System.out.println("<== TestSyncHdfsPolicyREST.databaseTest()");
  }

  private void ASSERT_EXECUTE_SUCCESS(Statement statement, String sql) throws Exception {
    System.out.println("execute sql : " + sql);
    boolean ret = statement.execute(sql);
  }
}
