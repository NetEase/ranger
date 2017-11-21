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

  static String PARAMETER_LIFECYCLE = "LIFECYCLE";

  static public boolean needSynchronize(ListenerEvent listenerEvent) {
    String lifecycleParam = "";
    if (listenerEvent instanceof CreateDatabaseEvent) {
      CreateDatabaseEvent event = (CreateDatabaseEvent)listenerEvent;
      Map<String, String> mapParameters = event.getDatabase().getParameters();
      if (null != mapParameters && mapParameters.containsKey(PARAMETER_LIFECYCLE)) {
        lifecycleParam = mapParameters.get(PARAMETER_LIFECYCLE);
      }
    } else if (listenerEvent instanceof DropDatabaseEvent) {
      DropDatabaseEvent event = (DropDatabaseEvent)listenerEvent;
      Map<String, String> mapParameters = event.getDatabase().getParameters();
      if (null != mapParameters && mapParameters.containsKey(PARAMETER_LIFECYCLE)) {
        lifecycleParam = mapParameters.get(PARAMETER_LIFECYCLE);
      }
    } else if (listenerEvent instanceof CreateTableEvent) {
      CreateTableEvent event = (CreateTableEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters && mapParameters.containsKey(PARAMETER_LIFECYCLE)) {
        lifecycleParam = mapParameters.get(PARAMETER_LIFECYCLE);
      }
    } else if (listenerEvent instanceof DropTableEvent) {
      DropTableEvent event = (DropTableEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters && mapParameters.containsKey(PARAMETER_LIFECYCLE)) {
        lifecycleParam = mapParameters.get(PARAMETER_LIFECYCLE);
      }
    } else if (listenerEvent instanceof AlterTableEvent) {
      AlterTableEvent event = (AlterTableEvent)listenerEvent;
      Map<String, String> oldParameters = event.getOldTable().getParameters();
      if (null != oldParameters && oldParameters.containsKey(PARAMETER_LIFECYCLE)) {
        lifecycleParam = oldParameters.get(PARAMETER_LIFECYCLE);
      }
      Map<String, String> newParameters = event.getOldTable().getParameters();
      if (null != newParameters && newParameters.containsKey(PARAMETER_LIFECYCLE)) {
        lifecycleParam = lifecycleParam + newParameters.get(PARAMETER_LIFECYCLE);
      }
    } else if (listenerEvent instanceof AddPartitionEvent) {
      AddPartitionEvent event = (AddPartitionEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters && mapParameters.containsKey(PARAMETER_LIFECYCLE)) {
        lifecycleParam = mapParameters.get(PARAMETER_LIFECYCLE);
      }
    } else if (listenerEvent instanceof AlterPartitionEvent) {
      AlterPartitionEvent event = (AlterPartitionEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters && mapParameters.containsKey(PARAMETER_LIFECYCLE)) {
        lifecycleParam = mapParameters.get(PARAMETER_LIFECYCLE);
      }
    } else if (listenerEvent instanceof DropPartitionEvent) {
      DropPartitionEvent event = (DropPartitionEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters && mapParameters.containsKey(PARAMETER_LIFECYCLE)) {
        lifecycleParam = mapParameters.get(PARAMETER_LIFECYCLE);
      }
    }

    // When the lifecycle parameters are empty, no synchronization is required
    if (null == lifecycleParam || lifecycleParam.isEmpty()) {
      return true;
    } else {
      LOGGER.info("lifecycleParam = " + lifecycleParam);
      return false;
    }
  }
}
