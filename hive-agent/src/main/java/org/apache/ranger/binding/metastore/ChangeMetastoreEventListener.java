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

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
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

//  protected HiveConf conf;
  private static String zkHostPort_ = "";
  private static String zkPath_ = "/hive-metastore-changelog";
  private final static String MAX_ID_FILE_NAME = "/maxid";
  private final static String LOCK_RELATIVE_PATH = "/lock";

  private final MetaStoreUpdateServiceVersion MetaStore_Update_Service_Version = MetaStoreUpdateServiceVersion.V1;
  private ConcurrentLinkedQueue<TUpdateDelta> tUpdateDeltaQueue_ = new ConcurrentLinkedQueue<TUpdateDelta>();;
  private Long curretnTUpdateDeltaId_ = 0L;
  private String hostName_ = "";
  private int asyncInterval_ = 3000;

  public ChangeMetastoreEventListener(Configuration config) {
    super(config);
    HiveConf hiveConf = new HiveConf(config, this.getClass());

    RangerConfiguration.getInstance().addResourcesForServiceType("hive");
    Preconditions.checkNotNull(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_ZK_QUORUM),
        RangerHadoopConstants.RANGER_ZK_QUORUM + " not config");
    Preconditions.checkNotNull(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_ZK_MS_CHANGELOG_PATH),
        RangerHadoopConstants.RANGER_ZK_MS_CHANGELOG_PATH + " not config");
    zkHostPort_ = RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_ZK_MS_CHANGELOG_PATH);

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

  private void getSingletonClient() throws Exception {
    if (zkClient == null) {
      synchronized (ChangeMetastoreEventListener.class) {
        if (zkClient == null) {
          zkClient =
              CuratorFrameworkFactory
                  .builder()
                  .connectString(RangerConfiguration.getInstance().get(RangerHadoopConstants.RANGER_ZK_QUORUM))
                  .aclProvider(zooKeeperAclProvider)
                  .retryPolicy(
                      new RetryNTimes(RangerConfiguration.getInstance().getInt(RangerHadoopConstants.RANGER_ZK_RETRYCNT, 3),
                      RangerConfiguration.getInstance().getInt(RangerHadoopConstants.RANGER_ZK_TIMEOUT, 3000)))
                  .build();
          listener = new zkListener();
          zkClient.getConnectionStateListenable().addListener(listener);
          zkClient.start();
          Stat stat = zkClient.checkExists().forPath(zkPath_);
          if (null == stat) {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkPath_, new byte[0]);
          }
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
    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("==> writeZNodeData()");
    }

    if (tUpdateDeltas.size() == 0)
      return;

    try {
      InterProcessMutex lock = new InterProcessMutex(zkClient, zkPath_ + LOCK_RELATIVE_PATH);
      lock.acquire(3, TimeUnit.SECONDS);
      lockLocal.set(lock);

      // delete expired data 3 days ago
      GetChildrenBuilder childrenBuilder = zkClient.getChildren();
      List<String> children = childrenBuilder.forPath(zkPath_);
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(new Date());
      calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) - 2);
      if(calendar.getTime().getMinutes()%10 == 0) {
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
          if (null != stat && (stat.getCtime() - calendar.getTimeInMillis()) < 0) {
            zkClient.delete().forPath(childPath);
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
      byte[] updateMetadataBytes = memoryBuffer.getArray();

      Stat statMaxId = zkClient.checkExists().forPath(zkPath_+MAX_ID_FILE_NAME);
      Stat statNewMaxFileId = zkClient.checkExists().forPath(zkPath_ + "/" + newMaxFileId);
      CuratorTransaction transaction = zkClient.inTransaction();
      CuratorTransactionFinal transactionFinal = null;
      if (null == statMaxId) {
        transactionFinal = transaction.create().withMode(CreateMode.PERSISTENT)
            .forPath(zkPath_+MAX_ID_FILE_NAME, String.valueOf(newMaxFileId).getBytes()).and();
      } else {
        transactionFinal = transaction.setData()
            .forPath(zkPath_+MAX_ID_FILE_NAME, String.valueOf(newMaxFileId).getBytes()).and();
      }
      if (null == statNewMaxFileId) {
        transactionFinal.create().withMode(CreateMode.PERSISTENT)
            .forPath(zkPath_+"/"+newMaxFileId, updateMetadataBytes).and().commit();
      } else {
        transactionFinal.setData()
            .forPath(zkPath_+"/"+newMaxFileId, updateMetadataBytes).and().commit();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        InterProcessMutex lock = lockLocal.get();
        lock.release();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    if(LOGGER.isDebugEnabled()) {
      LOGGER.debug("<== writeZNodeData()");
    }
  }
  // the consumer TUpdateDelta from the queue
  class SaveMetaStoreChangeRunnable implements Runnable {
    ConcurrentLinkedQueue<TUpdateDelta> queue;

    SaveMetaStoreChangeRunnable(ConcurrentLinkedQueue<TUpdateDelta> queue) throws IOException {
      this.queue = queue;
    }

    public void run() {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("==> NofityMetaStoreRunnable.run()");
      }

      while (true) {
        try {
          TUpdateDelta tUpdateDelta;
          List<TUpdateDelta> tUpdateDeltaList = new ArrayList<TUpdateDelta>();
          while ((tUpdateDelta = queue.poll()) != null) {
            tUpdateDeltaList.add(tUpdateDelta);
          }
          writeZNodeData(tUpdateDeltaList);
          Thread.currentThread().sleep(asyncInterval_);
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
      LOGGER.debug("Skip notify onCreateDatabase event, since the operation failed. \n");
      return;
    }

    String databaseName = dbEvent.getDatabase().getName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(databaseName);
    tUpdateDelta.setTable("");
    tUpdateDelta.setOperation(TOperation.CREATE_DATABASE);
    tUpdateDeltaQueue_.add(tUpdateDelta);
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!dbEvent.getStatus()) {
      LOGGER.debug("Skip notify onDropDatabase event, since the operation failed. \n");
      return;
    }

    String databaseName = dbEvent.getDatabase().getName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(databaseName);
    tUpdateDelta.setTable("");
    tUpdateDelta.setOperation(TOperation.DROP_DATABASE);
    tUpdateDeltaQueue_.add(tUpdateDelta);
  }

  @Override
  public void onCreateTable (CreateTableEvent tableEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip notify onCreateTable event, since the operation failed. \n");
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
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    // don't sync paths/privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip notify onDropTable event, since the operation failed. \n");
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
  }

  /**
   * Adjust the privileges when table is renamed
   */
  @Override
  public void onAlterTable (AlterTableEvent tableEvent) throws MetaException {
    // don't sync privileges if the operation has failed
    if (!tableEvent.getStatus()) {
      LOGGER.debug("Skip notify onAlterTable event, since the operation failed.");
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
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.debug("Skip notify onAddPartition event, since the operation failed.");
      return;
    }

    List<String> partCols = new ArrayList<>();
    List<List<String>> partValsList = new ArrayList<>();
    if (partitionEvent != null && partitionEvent.getTable().getPartitionKeysIterator() != null) {
      Iterator<FieldSchema> iter = partitionEvent.getTable().getPartitionKeysIterator();
      while (iter.hasNext()) {
        FieldSchema fieldSchema = iter.next();
        String partName = fieldSchema.getName();
        partCols.add(partName);
      }
    }

    if (partitionEvent != null && partitionEvent.getPartitionIterator() != null) {
      Iterator<Partition> it = partitionEvent.getPartitionIterator();
      while (it.hasNext()) {
        Partition part = it.next();
        partValsList.add(part.getValues());
      }
    }

    for (List<String> partVals : partValsList) {
      String dbName = partitionEvent.getTable().getDbName();
      String tableName = partitionEvent.getTable().getTableName();
      String partName = FileUtils.makePartName(partCols, partVals);
      TUpdateDelta tUpdateDelta = new TUpdateDelta();
      tUpdateDelta.setId(curretnTUpdateDeltaId_++);
      tUpdateDelta.setDatabase(dbName);
      tUpdateDelta.setTable(tableName);
      tUpdateDelta.setPartition(partName);
      tUpdateDelta.setOperation(TOperation.ADD_PARTITION);
      tUpdateDeltaQueue_.add(tUpdateDelta);
    }
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.debug("Skip notify onDropPartition event, since the operation failed.");
      return;
    }

    List<String> partCols = new ArrayList<>();
    String newPartName = "", oldPartName = "";
    if (partitionEvent != null && partitionEvent.getTable().getPartitionKeysIterator() != null) {
      Iterator<FieldSchema> iter = partitionEvent.getTable().getPartitionKeysIterator();
      while (iter.hasNext()) {
        FieldSchema fieldSchema = iter.next();
        String partName = fieldSchema.getName();
        partCols.add(partName);
      }
    }

    if (partitionEvent != null && partitionEvent.getNewPartition() != null) {
      List<String> partVals = partitionEvent.getNewPartition().getValues();
      newPartName = FileUtils.makePartName(partCols, partVals);
    }

    if (partitionEvent != null && partitionEvent.getOldPartition() != null) {
      List<String> partVals = partitionEvent.getOldPartition().getValues();
      oldPartName = FileUtils.makePartName(partCols, partVals);
    }

    String dbName = partitionEvent.getTable().getDbName();
    String tableName = partitionEvent.getTable().getTableName();
    TUpdateDelta tUpdateDelta = new TUpdateDelta();
    tUpdateDelta.setId(curretnTUpdateDeltaId_++);
    tUpdateDelta.setDatabase(dbName);
    tUpdateDelta.setTable(tableName);
    tUpdateDelta.setPartition(oldPartName);
    tUpdateDelta.setNew_name(newPartName);
    tUpdateDelta.setOperation(TOperation.ALTER_PARTITION);
    tUpdateDeltaQueue_.add(tUpdateDelta);
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
    // don't sync path if the operation has failed
    if (!partitionEvent.getStatus()) {
      LOGGER.debug("Skip notify onDropPartition event, since the operation failed.");
      return;
    }

    List<String> partCols = new ArrayList<>();
    List<List<String>> partValsList = new ArrayList<>();
    if (partitionEvent != null && partitionEvent.getTable().getPartitionKeysIterator() != null) {
      Iterator<FieldSchema> iter = partitionEvent.getTable().getPartitionKeysIterator();
      while (iter.hasNext()) {
        FieldSchema fieldSchema = iter.next();
        String partName = fieldSchema.getName();
        partCols.add(partName);
      }
    }

    if (partitionEvent != null && partitionEvent.getPartitionIterator() != null) {
      Iterator<Partition> it = partitionEvent.getPartitionIterator();
      while (it.hasNext()) {
        Partition part = it.next();
        partValsList.add(part.getValues());
      }
    }

    for (List<String> partVals : partValsList) {
      String dbName = partitionEvent.getTable().getDbName();
      String tableName = partitionEvent.getTable().getTableName();
      String partName = FileUtils.makePartName(partCols, partVals);
      TUpdateDelta tUpdateDelta = new TUpdateDelta();
      tUpdateDelta.setId(curretnTUpdateDeltaId_++);
      tUpdateDelta.setDatabase(dbName);
      tUpdateDelta.setTable(tableName);
      tUpdateDelta.setPartition(partName);
      tUpdateDelta.setOperation(TOperation.DROP_PARTITION);
      tUpdateDeltaQueue_.add(tUpdateDelta);
    }
  }
}
