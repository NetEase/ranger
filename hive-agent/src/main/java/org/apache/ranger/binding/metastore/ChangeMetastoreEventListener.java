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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.binding.metastore.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.TProtocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.datanucleus.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

class zkListener implements ConnectionStateListener {
  public static final Log LOGGER = LogFactory.getLog(ChangeMetastoreEventListener.class);

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState state) {
    switch (state) {
      case LOST:
        LOGGER.error("DistributedLock lost session with zookeeper");
        break;
      case CONNECTED:
        LOGGER.warn("DistributedLock connected with zookeeper");
        break;
      case RECONNECTED:
        LOGGER.warn("DistributedLock reconnected with zookeeper");
        break;
    }
  }
}

public class ChangeMetastoreEventListener extends MetaStoreEventListener {
  private static final Log LOGGER = LogFactory.getLog(ChangeMetastoreEventListener.class);

  protected static CuratorFramework zkClient;
  private static zkListener listener = null;
  private static ThreadLocal<InterProcessMutex> lockLocal = new ThreadLocal<InterProcessMutex>();

  private static String zkPath_ = "/hive-metastore-changelog";
  private final static String MAX_ID_FILE_NAME = "/maxid";
  private final static String LOCK_RELATIVE_PATH = "/lock";

  private final MetaStoreUpdateServiceVersion MetaStore_Update_Service_Version = MetaStoreUpdateServiceVersion.V1;
  private ConcurrentLinkedQueue<TUpdateDelta> tUpdateDeltaQueue_ = new ConcurrentLinkedQueue<>();
  private Map<String, TUpdateDelta> mapPartitionUpdate= new ConcurrentHashMap();
  private Long curretnTUpdateDeltaId_ = 0L;
  private String hostName_ = "";
  private int asyncInterval_ = 3000;
  private int autoClearTime_ = 5;
  private int writeZkBatchSize_ = 10;
  private int zkReconnectInterval_ = 30; // minute
  private Date zkReconnectTime = new Date();
  private String zookeeperShellPath_ = "";
  private String zookeeperQuorum_ = "";
  private int callZookeeperShellTimeoutMS_ = 10*1000;

  public ChangeMetastoreEventListener(Configuration config) {
    super(config);

    LOGGER.info("ChangeMetastoreEventListener >>>");
    HiveConf hiveConf = new HiveConf(config, this.getClass());

    RangerConfiguration.getInstance().addResourcesForServiceType("hive");
    Preconditions.checkNotNull(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_ZK_QUORUM),
        RangerHadoopConstants.RANGER_ZK_QUORUM + " not config");
    Preconditions.checkNotNull(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_ZK_MS_CHANGELOG_PATH),
        RangerHadoopConstants.RANGER_ZK_MS_CHANGELOG_PATH + " not config");
    zkPath_ = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_ZK_MS_CHANGELOG_PATH);
    autoClearTime_ = RangerConfiguration.getInstance().getInt(RangerHadoopConstants.RANGER_ZK_MS_CHANGELOG_AUTO_CLEAR_TIME, 5);
    writeZkBatchSize_ = RangerConfiguration.getInstance().getInt(RangerHadoopConstants.RANGER_ZK_MS_CHANGELOG_WRITE_BATCH_SIZE, 10);
    zkReconnectInterval_ = RangerConfiguration.getInstance().getInt(RangerHadoopConstants.RANGER_ZK_MS_RECONNECT_INTERVAL, 30);
    zookeeperShellPath_ = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_ZK_MS_WRITE_SHELL_PATH, "");
    zookeeperQuorum_ = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_ZK_QUORUM, "");

    try {
      InetAddress inetAddress = InetAddress.getLocalHost();
      hostName_ = inetAddress.getHostAddress();
      setUpZooKeeperAuth(hiveConf);
      getSingletonClient();
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
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
    LOGGER.info("ChangeMetastoreEventListener <<<");
  }

  /**
   * ACLProvider for providing appropriate ACLs to CuratorFrameworkFactory
   */
  private final ACLProvider zooKeeperAclProvider = new ACLProvider() {
    List<ACL> nodeAcls = new ArrayList<ACL>();

    @Override
    public List<ACL> getDefaultAcl() {
      if (UserGroupInformation.isSecurityEnabled()) {
        // Read all to the world
        nodeAcls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
        // Create/Delete/Write/Admin to the authenticated user
        nodeAcls.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
      } else {
        // ACLs for znodes on a non-kerberized cluster
        // Create/Read/Delete/Write/Admin to the world
        nodeAcls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE);
      }
      return nodeAcls;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };

  private void closeZkClient() {
    zkClient.getConnectionStateListenable().removeListener(listener);
    zkClient.close();
    zkClient = null;
  }

  private void getSingletonClient() throws Exception {
    synchronized (ChangeMetastoreEventListener.class) {
      listener = new zkListener();

      Date now = new Date();
      long zkRunMinute = (now.getTime() - zkReconnectTime.getTime())/60000;
      if ((zkRunMinute >= zkReconnectInterval_) && (zkClient != null)) {
        LOGGER.info("zkClient need reconnect zookeeper server");
        closeZkClient();
      }

      if (zkClient == null) {
        LOGGER.info("zkClient connect zookeeper server ...");

        zkClient = CuratorFrameworkFactory.builder()
            .connectString(zookeeperQuorum_)
            .aclProvider(zooKeeperAclProvider)
            .retryPolicy(
                new RetryNTimes(RangerConfiguration.getInstance().getInt(RangerHadoopConstants.RANGER_ZK_RETRYCNT, 10),
                    RangerConfiguration.getInstance().getInt(RangerHadoopConstants.RANGER_ZK_TIMEOUT, 5000)))
            .build();

        zkClient.getConnectionStateListenable().addListener(listener);
        zkClient.start();
        zkReconnectTime = new Date();

        Stat stat = zkClient.checkExists().forPath(zkPath_);
        if (null == stat) {
          zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkPath_, new byte[0]);
        }
      }
    }
  }

  /**
   * For a kerberized cluster, we dynamically set up the client's JAAS conf.
   *
   * @param hiveConf
   * @return
   * @throws Exception
   */
  private void setUpZooKeeperAuth(HiveConf hiveConf) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      String principal = hiveConf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL);
      if (StringUtils.isEmpty(principal)) {
        throw new IOException("Hive Metastore Kerberos principal is empty");
      }
      String keyTabFile = hiveConf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE);
      if (StringUtils.isEmpty(keyTabFile)) {
        throw new IOException("Hive Metastore Kerberos keytab is empty");
      }
      // Install the JAAS Configuration for the runtime
      Utils.setZookeeperClientKerberosJaasConfig(principal, keyTabFile);
    }
  }

  private void writeZNodeData(List<TUpdateDelta> tUpdateDeltas) {
    if (tUpdateDeltas.size() == 0)
      return;

    LOGGER.info("==> writeZNodeData(" + tUpdateDeltas.size() + ")");

    try {
      getSingletonClient();
    } catch (Exception ex) {
      ex.printStackTrace();
      return;
    }

    try {
      InterProcessMutex lock = new InterProcessMutex(zkClient, zkPath_ + LOCK_RELATIVE_PATH);
      if (!lock.acquire(10, TimeUnit.SECONDS)) {
        LOGGER.warn("writeZNodeData() could not acquire the lock");
        return;
      }
      lockLocal.set(lock);

      // delete expired data
      GetChildrenBuilder childrenBuilder = zkClient.getChildren();
      List<String> children = childrenBuilder.forPath(zkPath_);

      Date now = new Date();
      if(now.getMinutes()%5 == 0) {
        // execute once every ten minutes
        for (String child : children) {
          child = "/" + child;
          if (child.equalsIgnoreCase(LOCK_RELATIVE_PATH)
              || child.equalsIgnoreCase(MAX_ID_FILE_NAME)) {
            // do not delete maxid and lock file
            continue;
          }
          String childPath = zkPath_ + child;
          Stat stat = zkClient.checkExists().forPath(childPath);
          if (null != stat && ((now.getTime() - stat.getCtime()) >= autoClearTime_*60*1000)) {
            LOGGER.info("zkClient delete expired node : " + childPath);
            if (zookeeperShellPath_.isEmpty()) {
              zkClient.delete().forPath(childPath);
            } else {
              callZookeeperShell("delete", childPath, "");
            }
          }
        }
      }

      Stat stat = zkClient.checkExists().forPath(zkPath_ + MAX_ID_FILE_NAME);
      Long newMaxFileId = 1L;
      if (null != stat) {
        byte[] byteFileMaxId = zkClient.getData().forPath(zkPath_ + MAX_ID_FILE_NAME);
        String strFileMaxId = new String(byteFileMaxId);
        try {
          newMaxFileId = Long.parseLong(strFileMaxId) + 1;
        } catch (NumberFormatException e) {
          LOGGER.error(e.getMessage());
        }
      }
      TUpdateMetadataRequest tUpdateMetadataRequest = new TUpdateMetadataRequest();
      tUpdateMetadataRequest.setProtocol_version(MetaStore_Update_Service_Version);
      tUpdateMetadataRequest.setDeltas(tUpdateDeltas);
      tUpdateMetadataRequest.setHostname(hostName_);

      TMemoryBuffer memoryBuffer = new TMemoryBuffer(8);
      TProtocol protocol = new org.apache.thrift.protocol.TJSONProtocol(memoryBuffer);
      tUpdateMetadataRequest.write(protocol);

      // bytesUpdateMetadata[] need trim 0x0,0x0,0x0,0x0,...
      byte[] bytesUpdateMetadata = memoryBuffer.getArray();
      String strUpdateMetadata = new String(bytesUpdateMetadata);
      strUpdateMetadata = strUpdateMetadata.trim();
      LOGGER.info("strUpdateMetadata = " + strUpdateMetadata);

      int updateMetadataLen = strUpdateMetadata.getBytes("UTF-8").length;
      if (updateMetadataLen >= 0xfffff) {
        LOGGER.error("updateMetadataBytes.length > zookeeper BinaryInputArchive.maxBuffer(0xfffff)");
      } else {
        LOGGER.info("updateMetadataBytes.length = " + updateMetadataLen);

        Stat statMaxId = zkClient.checkExists().forPath(zkPath_ + MAX_ID_FILE_NAME);
        Stat statNewMaxFileId = zkClient.checkExists().forPath(zkPath_ + "/" + newMaxFileId);

        if (zookeeperShellPath_.isEmpty()) {
          if (null == statMaxId) {
            LOGGER.info("create : " + zkPath_ + MAX_ID_FILE_NAME);
            zkClient.create().withMode(CreateMode.PERSISTENT).forPath(zkPath_ + MAX_ID_FILE_NAME, String.valueOf(newMaxFileId).getBytes());
          } else {
            LOGGER.info("update : " + zkPath_ + MAX_ID_FILE_NAME);
            zkClient.setData().forPath(zkPath_ + MAX_ID_FILE_NAME, String.valueOf(newMaxFileId).getBytes());
          }
          if (null == statNewMaxFileId) {
            LOGGER.info("create : " + zkPath_ + "/" + newMaxFileId);
            zkClient.create().withMode(CreateMode.PERSISTENT).forPath(zkPath_ + "/" + String.valueOf(newMaxFileId), strUpdateMetadata.getBytes());
          } else {
            LOGGER.info("update : " + zkPath_ + "/" + newMaxFileId);
            zkClient.setData().forPath(zkPath_ + "/" + String.valueOf(newMaxFileId), strUpdateMetadata.getBytes());
          }
        } else {
          if (null == statMaxId) {
            LOGGER.info("create : " + zkPath_ + MAX_ID_FILE_NAME);
            callZookeeperShell("create",zkPath_ + MAX_ID_FILE_NAME, String.valueOf(newMaxFileId));
          } else {
            LOGGER.info("update : " + zkPath_ + MAX_ID_FILE_NAME);
            callZookeeperShell("set",zkPath_ + MAX_ID_FILE_NAME, String.valueOf(newMaxFileId));
          }
          if (null == statNewMaxFileId) {
            LOGGER.info("create : " + zkPath_ + "/" + String.valueOf(newMaxFileId));
            callZookeeperShell("create",zkPath_ + "/" + String.valueOf(newMaxFileId), strUpdateMetadata);
          } else {
            LOGGER.info("update : " + zkPath_ + "/" + String.valueOf(newMaxFileId));
            callZookeeperShell("set",zkPath_ + "/" + String.valueOf(newMaxFileId), strUpdateMetadata);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e.getMessage());
      closeZkClient();
    } finally {
      try {
        InterProcessMutex lock = lockLocal.get();
        if (lock.isAcquiredInThisProcess()) {
          lock.release();
        }
      } catch (Exception e) {
        e.printStackTrace();
        LOGGER.error(e.getMessage());
      }
    }

    LOGGER.info("<== writeZNodeData()");
  }

  boolean callZookeeperShell(String cmd, String znodePath, String params) {
    boolean runResult = false;

    if (zookeeperShellPath_.isEmpty()) {
      LOGGER.error("writeZkShell() zkWiteShellPath is empty!");
      return false;
    }
    LOGGER.info("writeZkShell() zkWiteShellPath = " + zookeeperShellPath_);

    Process process = null;
    try {
      String command = " " + zookeeperQuorum_ + " " + cmd + " " + znodePath + " " + params;
      process = Runtime.getRuntime().exec(zookeeperShellPath_ + command);
      LOGGER.info("writeZkShell = " + zookeeperShellPath_ + command);

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
        LOGGER.error("writeZkShell() faild, " + znodePath + "， " + params);
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

          int limit1 = writeZkBatchSize_;
          while ((tUpdateDelta = queue.poll()) != null) {
            if (limit1-- < 0) {
              LOGGER.warn("tUpdateDeltaQueue_.size = " + queue.size() + " > " + writeZkBatchSize_);
              break;
            }

            tUpdateDeltaList.add(tUpdateDelta);
          }

          int limit2 = writeZkBatchSize_;
          Iterator<Map.Entry<String, TUpdateDelta>> iterator = mapPartitionUpdate.entrySet().iterator();
          while (iterator.hasNext()){
            if (limit2-- < 0) {
              LOGGER.warn("mapPartitionUpdate.size = " + mapPartitionUpdate.size() + " > " + writeZkBatchSize_);
              break;
            }

            Map.Entry<String, TUpdateDelta> entry = iterator.next();
            tUpdateDeltaList.add(entry.getValue());

            iterator.remove();
          }

          writeZNodeData(tUpdateDeltaList);

          if (limit1 > 0 && limit2 > 0) {
            Thread.currentThread().sleep(asyncInterval_);
          }
        } catch (Exception ex) {
          ex.printStackTrace();
        }

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("<== NofityMetaStoreRunnable.run()");
        }
      }
    }
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
      LOGGER.info("Table lifecycle parameters is empty, it needs to be synchronized");
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
      LOGGER.info("Table lifecycle parameters is empty, it needs to be synchronized");
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
      LOGGER.info("Table lifecycle parameters is empty, it needs to be synchronized");
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
      LOGGER.info("Table lifecycle parameters is empty, it needs to be synchronized");
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
      LOGGER.info("Table lifecycle parameters is empty, it needs to be synchronized");
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
      LOGGER.info("Table lifecycle parameters is empty, it needs to be synchronized");
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
      mapPartitionUpdate.put(key, tUpdateDelta);
    }
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.info("Skip notify onDropPartition event, since the operation failed.");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(partitionEvent)) {
      LOGGER.info("Table lifecycle parameters is empty, it needs to be synchronized");
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
      mapPartitionUpdate.put(key, tUpdateDelta);
    }

    LOGGER.info("onAlterPartition()" + tUpdateDelta.toString());
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.info("Skip notify onDropPartition event, since the operation failed.");
      return;
    }
    if (!MetaStoreEventListenerUtils.needSynchronize(partitionEvent)) {
      LOGGER.info("Table lifecycle parameters is empty, it needs to be synchronized");
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
      mapPartitionUpdate.put(key, tUpdateDelta);
    }
  }
}
