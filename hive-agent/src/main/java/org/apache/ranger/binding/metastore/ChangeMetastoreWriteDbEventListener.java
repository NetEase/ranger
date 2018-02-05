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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.ibatis.session.SqlSession;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.binding.metastore.thrift.MetaStoreUpdateServiceVersion;
import org.apache.ranger.binding.metastore.thrift.TOperation;
import org.apache.ranger.binding.metastore.thrift.TUpdateDelta;
import org.apache.ranger.binding.metastore.thrift.TUpdateMetadataRequest;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

public class ChangeMetastoreWriteDbEventListener extends MetaStoreEventListener {
  private static final Log LOGGER = LogFactory.getLog(ChangeMetastoreWriteDbEventListener.class);

  private final MetaStoreUpdateServiceVersion MetaStore_Update_Service_Version = MetaStoreUpdateServiceVersion.V1;
  private ConcurrentLinkedQueue<TUpdateDelta> tUpdateDeltaQueue_ = new ConcurrentLinkedQueue<>();
  private Map<String, TUpdateDelta> mapPartitionUpdate= new ConcurrentHashMap();
  private Long curretnTUpdateDeltaId_ = 0L;
  private String hostName_ = "";
  private int asyncInterval_ = 500;
  private int AutoClearHour = 24;
  private String MonitorShell = "";
  private String ChangelogTabelName = "";

  private SqlSessionFactory sqlSessionFactory = null;

  public ChangeMetastoreWriteDbEventListener(Configuration config) {
    super(config);

    LOGGER.info("ChangeMetastoreWriteDbEventListener >>>");
    RangerConfiguration.getInstance().addResourcesForServiceType("hive");

    HiveConf hiveConf = new HiveConf(config, this.getClass());

    Preconditions.checkNotNull(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_NAME),
        RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_NAME + " not config");
    Preconditions.checkNotNull(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_URL),
        RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_URL + " not config");
    Preconditions.checkNotNull(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_USERNAME),
        RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_USERNAME + " not config");
    Preconditions.checkNotNull(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_PASSWORD),
        RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_PASSWORD + " not config");
    Preconditions.checkNotNull(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_TABLE_NAME),
        RangerHadoopConstants.RANGER_MS_CHANGELOG_TABLE_NAME + " not config");

    AutoClearHour = RangerConfiguration.getInstance().getInt(RangerHadoopConstants.RANGER_ZK_MS_CHANGELOG_AUTO_CLEAR_TIME, 24);
    MonitorShell = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_MONITOR_SHELL, "");
    ChangelogTabelName = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_TABLE_NAME, "");
    if (ChangelogTabelName.isEmpty()) {
      ChangelogTabelName = "impala_metastore_update_log_" + RangerConfiguration.getInstance().get("ranger.plugin.hive.service.name", "");
      LOGGER.info("auto generate ChangelogTabelName = " + ChangelogTabelName);
    }

    try {
      initSqlSessionFactory();
      createImpalaMetastoreUpdateLogTable();
    } catch (Exception e) {
      e.printStackTrace();
    }

    Thread saveMetaStoreChangeThread = null;
    try {
      saveMetaStoreChangeThread = new Thread(new SaveMetaStoreChangeRunnable(tUpdateDeltaQueue_));
    } catch (IOException e) {
      e.printStackTrace();
    }
    saveMetaStoreChangeThread.start();
    LOGGER.info("ChangeMetastoreWriteDbEventListener <<<");
  }

  private void initSqlSessionFactory() {
    LOGGER.info("initSqlSessionFactory >>>");

    Reader reader = null;
    try {
      reader = Resources.getResourceAsReader("mybatis-config.xml");
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }

    String jdbcName = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_NAME);
    String jdbcUrl = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_URL);
    String jdbcUsername = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_USERNAME);
    String jdbcPassword = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_MS_CHANGELOG_JDBC_PASSWORD);

    Properties props = new Properties();
    props.setProperty("jdbc.driverClassName", jdbcName);
    props.setProperty("jdbc.url", jdbcUrl);
    props.setProperty("jdbc.username", jdbcUsername);
    props.setProperty("jdbc.password", jdbcPassword);
    LOGGER.info("initSqlSessionFactory.props = " + props.toString());

    sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader, props);
  }


  public boolean createImpalaMetastoreUpdateLogTable() {
    LOGGER.info("==> createImpalaMetastoreUpdateLogTable()");

    Preconditions.checkNotNull(RangerConfiguration.getInstance().get("ranger.plugin.hive.service.name"),
        "ranger.plugin.hive.service.name not config");

    SqlSession sqlSession = null;
    try {
      sqlSession = sqlSessionFactory.openSession();

      String statement = "org.apache.ranger.binding.metastore.ChangeMetastoreWriteDbEventListener.ChangeMetastoreMapper.createImpalaMetastoreUpdateLog";

      Map<String, Object> params = new HashMap<>();
      params.put("tableName", ChangelogTabelName);

      sqlSession.update(statement, params);
      sqlSession.commit();
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e.getMessage());
      return false;
    } finally {
      sqlSession.close();
    }

    LOGGER.info("createImpalaMetastoreUpdateLogTable() <==");

    return true;
  }

  public boolean insertImpalaMetastoreUpdateLog(List<TUpdateDelta> tUpdateDeltas) {
    if (tUpdateDeltas.size() == 0)
      return true;

    LOGGER.info("==> insertImpalaMetastoreUpdateLog(" + tUpdateDeltas.size() + ")");

    SqlSession sqlSession = null;
    try {
      sqlSession = sqlSessionFactory.openSession();

      Long nowMillis = System.currentTimeMillis();
      String statement = "org.apache.ranger.binding.metastore.ChangeMetastoreWriteDbEventListener.ChangeMetastoreMapper.insertImpalaMetastoreUpdateLog";

      List<String> strUpdateMetadataRequestList = new ArrayList<>();
      for (int n = 0; n < tUpdateDeltas.size(); n ++) {
        TUpdateDelta tUpdateDelta = tUpdateDeltas.get(n);

        TUpdateMetadataRequest tUpdateMetadataRequest = new TUpdateMetadataRequest();
        tUpdateMetadataRequest.setProtocol_version(MetaStore_Update_Service_Version);
        tUpdateMetadataRequest.setDeltas(Arrays.asList(tUpdateDelta));
        tUpdateMetadataRequest.setHostname(hostName_);

        TMemoryBuffer memoryBuffer = new TMemoryBuffer(8);
        TProtocol protocol = new org.apache.thrift.protocol.TJSONProtocol(memoryBuffer);
        tUpdateMetadataRequest.write(protocol);

        byte[] bytesUpdateMetadata = memoryBuffer.getArray();
        String strUpdateMetadata = new String(bytesUpdateMetadata);
        strUpdateMetadata = strUpdateMetadata.trim();
        LOGGER.info("tUpdateMetadataRequest = " + strUpdateMetadata);

        int updateMetadataLen = strUpdateMetadata.getBytes("UTF-8").length;
        if (updateMetadataLen > 256) {
          LOGGER.warn("tUpdateMetadataRequest.length > 256");
        }

        strUpdateMetadataRequestList.add(strUpdateMetadata);
      }

      Map<String, Object> params = new HashMap<>();
      params.put("tableName", ChangelogTabelName);
      params.put("list", strUpdateMetadataRequestList);
      int numInert = sqlSession.insert(statement, params);
      sqlSession.commit();
      LOGGER.info("insertImpalaMetastoreUpdateLog(" + nowMillis + ") insert count = " + numInert);
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e.getMessage());
      return false;
    } finally {
      sqlSession.close();
    }

    return true;
  }

  public boolean delImpalaMetastoreUpdateLog() {
    Date now = new Date();
    java.util.Calendar calendar = java.util.Calendar.getInstance();
    calendar.setTime(now);
    calendar.add(java.util.Calendar.HOUR_OF_DAY, -AutoClearHour);
    SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String strDate = fmt.format(calendar.getTime());

    LOGGER.info("delImpalaMetastoreUpdateLog(" + strDate + ")");

    SqlSession sqlSession = null;
    try {
      sqlSession = sqlSessionFactory.openSession();

      Map<String, Object> params = new HashMap<>();
      params.put("tableName", ChangelogTabelName);
      params.put("createTime", strDate);
      String statement = "org.apache.ranger.binding.metastore.ChangeMetastoreWriteDbEventListener.ChangeMetastoreMapper.deleteImpalaMetastoreUpdateLog";
      int delCount = sqlSession.delete(statement, params);
      sqlSession.commit();
      LOGGER.info("delImpalaMetastoreUpdateLog(" + strDate + ") delete count = " + delCount);
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e.getMessage());
      return false;
    } finally {
      sqlSession.close();
    }

    return true;
  }

  // the consumer TUpdateDelta from the queue
  class SaveMetaStoreChangeRunnable implements Runnable {
    ConcurrentLinkedQueue<TUpdateDelta> queue;

    SaveMetaStoreChangeRunnable(ConcurrentLinkedQueue<TUpdateDelta> queue) throws IOException {
      this.queue = queue;
    }

    public void run() {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("==> SaveMetaStoreChangeRunnable.run()");
      }

      while (true) {
        try {
          TUpdateDelta tUpdateDelta;
          List<TUpdateDelta> tUpdateDeltaList = new ArrayList<>();

          while ((tUpdateDelta = queue.poll()) != null) {
            LOGGER.info("SaveMetaStoreChangeRunnable() queue get > " + tUpdateDelta.toString());

            tUpdateDeltaList.add(tUpdateDelta);
          }

          Iterator<Map.Entry<String, TUpdateDelta>> iterator = mapPartitionUpdate.entrySet().iterator();
          while (iterator.hasNext()){
            Map.Entry<String, TUpdateDelta> entry = iterator.next();
            tUpdateDeltaList.add(entry.getValue());
            LOGGER.info("SaveMetaStoreChangeRunnable() mapPartitionUpdate get > " + entry.getValue().toString());

            iterator.remove();
          }

          boolean result = insertImpalaMetastoreUpdateLog(tUpdateDeltaList);
          if (false == result) {
            LOGGER.error("insertImpalaMetastoreUpdateLog error!!!");
            callMonitorShell();
          }

          Date now = new Date();
          if(now.getMinutes()%10 == 0) {
            result = delImpalaMetastoreUpdateLog();
            if (false == result) {
              LOGGER.error("delImpalaMetastoreUpdateLog error!!!");
              callMonitorShell();
            }
          }

          Thread.currentThread().sleep(asyncInterval_);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    }
  }

  boolean callMonitorShell() {
    boolean runResult = true;

    if (MonitorShell.isEmpty()) {
      LOGGER.warn("MonitorShell is empty!");
      return false;
    }
    LOGGER.info("callMonitorShell() " + MonitorShell);

    Process process = null;
    try {
      process = Runtime.getRuntime().exec(MonitorShell);

      process.waitFor();

      // 读取标准输出流
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line = null;
      while ((line = bufferedReader.readLine()) != null) {
        LOGGER.debug(line);
      }

      // 读取标准错误流
      BufferedReader brError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      String errline = null;
      while ((errline = brError.readLine()) != null) {
        LOGGER.error(errline);
      }

      // waitFor() 判断Process进程是否终止，通过返回值判断是否正常终止。0 代表正常终止
      int result = process.waitFor();
      if(result != 0){
        LOGGER.error("callMonitorShell() faild!");
        runResult = false;
      } else {
        runResult = true;
      }
    } catch (Exception e) {
      e.printStackTrace();
      runResult = false;
    } finally {
      if (process != null) {
        process.destroy();
      }
    }

    return runResult;
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent)
      throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!dbEvent.getStatus()) {
      LOGGER.info("Skip notify onCreateDatabase event, since the operation failed. \n");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(dbEvent)) {
      LOGGER.info("Table lifecycle parameters is not empty, No synchronization event.");
      return;
    }

    String databaseName = dbEvent.getDatabase().getName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(databaseName);
    tUpdateDelta.setTable("");
    tUpdateDelta.setOperation(TOperation.CREATE_DATABASE);
    tUpdateDeltaQueue_.add(tUpdateDelta);

    LOGGER.info("onCreateDatabase()" + tUpdateDelta.toString());
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!dbEvent.getStatus()) {
      LOGGER.info("Skip notify onDropDatabase event, since the operation failed. \n");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(dbEvent)) {
      LOGGER.info("Table lifecycle parameters is not empty, No synchronization event.");
      return;
    }

    String databaseName = dbEvent.getDatabase().getName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(databaseName);
    tUpdateDelta.setTable("");
    tUpdateDelta.setOperation(TOperation.DROP_DATABASE);
    tUpdateDeltaQueue_.add(tUpdateDelta);

    LOGGER.info("onDropDatabase()" + tUpdateDelta.toString());
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.info("Skip notify onCreateTable event, since the operation failed. \n");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(tableEvent)) {
      LOGGER.info("Table lifecycle parameters is not empty, No synchronization event.");
      return;
    }

    String databaseName = tableEvent.getTable().getDbName();
    String tableName = tableEvent.getTable().getTableName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(databaseName);
    tUpdateDelta.setTable(tableName);
    tUpdateDelta.setOperation(TOperation.CREATE_TABLE);
    tUpdateDeltaQueue_.add(tUpdateDelta);

    LOGGER.info("onCreateTable()" + tUpdateDelta.toString());
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip notify onDropTable event, since the operation failed. \n");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(tableEvent)) {
      LOGGER.info("Table lifecycle parameters is not empty, No synchronization event.");
      return;
    }

    String databaseName = tableEvent.getTable().getDbName();
    String tableName = tableEvent.getTable().getTableName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(databaseName);
    tUpdateDelta.setTable(tableName);
    tUpdateDelta.setOperation(TOperation.DROP_TABLE);
    tUpdateDeltaQueue_.add(tUpdateDelta);

    LOGGER.info("onDropTable()" + tUpdateDelta.toString());
  }

  /**
   * Adjust the privileges when table is renamed
   */
  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    // don't sync privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip notify onAlterTable event, since the operation failed.");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(tableEvent)) {
      LOGGER.info("Table lifecycle parameters is not empty, No synchronization event.");
      return;
    }

    String oldDataName = tableEvent.getOldTable().getDbName();
    String oldTableName = tableEvent.getOldTable().getTableName();
    String newTableName = tableEvent.getNewTable().getTableName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(oldDataName);
    tUpdateDelta.setTable(oldTableName);
    tUpdateDelta.setNew_name(newTableName);
    if (newTableName.equalsIgnoreCase(oldTableName))
      tUpdateDelta.setOperation(TOperation.ALTER_TABLE);
    else
      tUpdateDelta.setOperation(TOperation.REMAME_TABLE);
    tUpdateDeltaQueue_.add(tUpdateDelta);

    LOGGER.info("onAlterTable()" + tUpdateDelta.toString());
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.debug("Skip notify onAddPartition event, since the operation failed.");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(partitionEvent)) {
      LOGGER.info("Table lifecycle parameters is not empty, No synchronization event.");
      return;
    }

    // the DROP TABLE or TRUNCATE TABLE operation triggers a large number of partition events
    String dbName = partitionEvent.getTable().getDbName();
    String tableName = partitionEvent.getTable().getTableName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(dbName);
    tUpdateDelta.setTable(tableName);
    tUpdateDelta.setPartition("");
    tUpdateDelta.setNew_name(tableName);
    tUpdateDelta.setOperation(TOperation.ALTER_TABLE);

    String key = dbName + "--1234567890--" + tableName;
    if (false == mapPartitionUpdate.containsKey(key)) {
      LOGGER.info("onAddPartition() " + tUpdateDelta.toString());
      mapPartitionUpdate.put(key, tUpdateDelta);
    }

    LOGGER.info("onAddPartition() <<<<<<");
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.info("Skip notify onDropPartition event, since the operation failed.");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(partitionEvent)) {
      LOGGER.info("Table lifecycle parameters is not empty, No synchronization event.");
      return;
    }

    // the DROP TABLE or TRUNCATE TABLE operation triggers a large number of partition events
    String dbName = partitionEvent.getTable().getDbName();
    String tableName = partitionEvent.getTable().getTableName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(dbName);
    tUpdateDelta.setTable(tableName);
    tUpdateDelta.setPartition("");
    tUpdateDelta.setNew_name(tableName);
    tUpdateDelta.setOperation(TOperation.ALTER_TABLE);

    String key = dbName + "--1234567890--" + tableName;
    if (false == mapPartitionUpdate.containsKey(key)) {
      LOGGER.info("onAlterPartition() " + tUpdateDelta.toString());
      mapPartitionUpdate.put(key, tUpdateDelta);
    }

    LOGGER.info("onAlterPartition() <<<<<<");
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.info("Skip notify onDropPartition event, since the operation failed.");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(partitionEvent)) {
      LOGGER.info("Table lifecycle parameters is not empty, No synchronization event.");
      return;
    }

    // the DROP TABLE or TRUNCATE TABLE operation triggers a large number of partition events
    String dbName = partitionEvent.getTable().getDbName();
    String tableName = partitionEvent.getTable().getTableName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(dbName);
    tUpdateDelta.setTable(tableName);
    tUpdateDelta.setPartition("");
    tUpdateDelta.setNew_name(tableName);
    tUpdateDelta.setOperation(TOperation.ALTER_TABLE);

    String key = dbName + "--1234567890--" + tableName;
    if (false == mapPartitionUpdate.containsKey(key)) {
      LOGGER.info("onDropPartition() " + tUpdateDelta.toString());
      mapPartitionUpdate.put(key, tUpdateDelta);
    }

    LOGGER.info("onDropPartition() <<<<<<");
  }
}
