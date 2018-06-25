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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.events.*;

import java.util.Map;

public class MetaStoreEventListenerUtils {
  private static final Log LOGGER = LogFactory.getLog(MetaStoreEventListenerUtils.class);

  static String NOT_SYNC_POLICY = "NOT_SYNC_POLICY";

  static String SYNC_METASTORE = "SYNC_METASTORE";

  static public boolean needSynchronizeImpala(ListenerEvent listenerEvent) {

    String syncMetastore = "";
    try {
      if (listenerEvent instanceof CreateDatabaseEvent) {
        CreateDatabaseEvent event = (CreateDatabaseEvent) listenerEvent;
        Map<String, String> mapParameters = event.getDatabase().getParameters();

        if (null != mapParameters && mapParameters.containsKey(SYNC_METASTORE)) {
          syncMetastore = mapParameters.get(SYNC_METASTORE);
        }
      } else if (listenerEvent instanceof DropDatabaseEvent) {
        DropDatabaseEvent event = (DropDatabaseEvent) listenerEvent;
        Map<String, String> mapParameters = event.getDatabase().getParameters();

        if (null != mapParameters && mapParameters.containsKey(SYNC_METASTORE)) {
          syncMetastore = mapParameters.get(SYNC_METASTORE);
        }
      } else if (listenerEvent instanceof CreateTableEvent) {
        CreateTableEvent event = (CreateTableEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();

        if (null != mapParameters && mapParameters.containsKey(SYNC_METASTORE)) {
          syncMetastore = mapParameters.get(SYNC_METASTORE);
        }
      } else if (listenerEvent instanceof DropTableEvent) {
        DropTableEvent event = (DropTableEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();

        if (null != mapParameters && mapParameters.containsKey(SYNC_METASTORE)) {
          syncMetastore = mapParameters.get(SYNC_METASTORE);
        }
      } else if (listenerEvent instanceof AlterTableEvent) {
        AlterTableEvent event = (AlterTableEvent) listenerEvent;
        Map<String, String> oldParameters = event.getOldTable().getParameters();

        if (null != oldParameters && oldParameters.containsKey(SYNC_METASTORE)) {
          syncMetastore = oldParameters.get(SYNC_METASTORE);
        }
        Map<String, String> newParameters = event.getNewTable().getParameters();

        if (null != oldParameters && oldParameters.containsKey(SYNC_METASTORE)) {
          syncMetastore = newParameters.get(SYNC_METASTORE);
        }
      } else if (listenerEvent instanceof AddPartitionEvent) {
        AddPartitionEvent event = (AddPartitionEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();

        if (null != mapParameters && mapParameters.containsKey(SYNC_METASTORE)) {
          syncMetastore = mapParameters.get(SYNC_METASTORE);
        }
      } else if (listenerEvent instanceof AlterPartitionEvent) {
        AlterPartitionEvent event = (AlterPartitionEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();

        if (null != mapParameters && mapParameters.containsKey(SYNC_METASTORE)) {
          syncMetastore = mapParameters.get(SYNC_METASTORE);
        }
      } else if (listenerEvent instanceof DropPartitionEvent) {
        DropPartitionEvent event = (DropPartitionEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();

        if (null != mapParameters && mapParameters.containsKey(SYNC_METASTORE)) {
          syncMetastore = mapParameters.get(SYNC_METASTORE);
        }
      }

      if (null != syncMetastore && syncMetastore.trim().equalsIgnoreCase("on")) {
        LOGGER.info("notSyncMetastore = " + syncMetastore);
        return true;
      }
    } catch (Throwable e) {
      LOGGER.info("catch a Exception " + e);
      LOGGER.info("return needSync true by default");
      return false;
    }
    return true;
  }

  static public boolean needSynchronizePolicy(ListenerEvent listenerEvent) {
    String notSyncPolicy = "";

    try {
      if (listenerEvent instanceof CreateDatabaseEvent) {
        CreateDatabaseEvent event = (CreateDatabaseEvent) listenerEvent;
        Map<String, String> mapParameters = event.getDatabase().getParameters();
        if (null != mapParameters && mapParameters.containsKey(NOT_SYNC_POLICY)) {
          notSyncPolicy = mapParameters.get(NOT_SYNC_POLICY);
        }

      } else if (listenerEvent instanceof DropDatabaseEvent) {
        DropDatabaseEvent event = (DropDatabaseEvent) listenerEvent;
        Map<String, String> mapParameters = event.getDatabase().getParameters();
        if (null != mapParameters && mapParameters.containsKey(NOT_SYNC_POLICY)) {
          notSyncPolicy = mapParameters.get(NOT_SYNC_POLICY);
        }
      } else if (listenerEvent instanceof CreateTableEvent) {
        CreateTableEvent event = (CreateTableEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();
        if (null != mapParameters && mapParameters.containsKey(NOT_SYNC_POLICY)) {
          notSyncPolicy = mapParameters.get(NOT_SYNC_POLICY);
        }

      } else if (listenerEvent instanceof DropTableEvent) {
        DropTableEvent event = (DropTableEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();
        if (null != mapParameters && mapParameters.containsKey(NOT_SYNC_POLICY)) {
          notSyncPolicy = mapParameters.get(NOT_SYNC_POLICY);
        }

      } else if (listenerEvent instanceof AlterTableEvent) {
        AlterTableEvent event = (AlterTableEvent) listenerEvent;
        Map<String, String> oldParameters = event.getOldTable().getParameters();
        if (null != oldParameters && oldParameters.containsKey(NOT_SYNC_POLICY)) {
          notSyncPolicy = oldParameters.get(NOT_SYNC_POLICY);
        }

        Map<String, String> newParameters = event.getNewTable().getParameters();
        if (null != newParameters && newParameters.containsKey(NOT_SYNC_POLICY)) {
          notSyncPolicy = notSyncPolicy + newParameters.get(NOT_SYNC_POLICY);
        }

      } else if (listenerEvent instanceof AddPartitionEvent) {
        AddPartitionEvent event = (AddPartitionEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();
        if (null != mapParameters && mapParameters.containsKey(NOT_SYNC_POLICY)) {
          notSyncPolicy = mapParameters.get(NOT_SYNC_POLICY);
        }

      } else if (listenerEvent instanceof AlterPartitionEvent) {
        AlterPartitionEvent event = (AlterPartitionEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();
        if (null != mapParameters && mapParameters.containsKey(NOT_SYNC_POLICY)) {
          notSyncPolicy = mapParameters.get(NOT_SYNC_POLICY);
        }

      } else if (listenerEvent instanceof DropPartitionEvent) {
        DropPartitionEvent event = (DropPartitionEvent) listenerEvent;
        Map<String, String> mapParameters = event.getTable().getParameters();
        if (null != mapParameters && mapParameters.containsKey(NOT_SYNC_POLICY)) {
          notSyncPolicy = mapParameters.get(NOT_SYNC_POLICY);
        }

      }

      if (null != notSyncPolicy && notSyncPolicy.trim().equalsIgnoreCase("on")) {
        return false;
      } else {
        return true;
      }
    } catch (Throwable e) {
      LOGGER.info("catch a Exception " + e);
      LOGGER.info("return needSync true by default");
      return true;
    }
  }

}
