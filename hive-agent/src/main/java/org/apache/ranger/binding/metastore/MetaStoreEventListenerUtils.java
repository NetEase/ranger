package org.apache.ranger.binding.metastore;

import org.apache.hadoop.hive.metastore.events.*;

import java.util.Map;

public class MetaStoreEventListenerUtils {
  static public boolean needSynchronize(ListenerEvent listenerEvent) {
    String lifecycleParam = "";
    if (listenerEvent instanceof CreateDatabaseEvent) {
      CreateDatabaseEvent event = (CreateDatabaseEvent)listenerEvent;
      Map<String, String> mapParameters = event.getDatabase().getParameters();
      if (null != mapParameters) {
        lifecycleParam = mapParameters.get("LIFECYCLE");
      }
    } else if (listenerEvent instanceof DropDatabaseEvent) {
      DropDatabaseEvent event = (DropDatabaseEvent)listenerEvent;
      Map<String, String> mapParameters = event.getDatabase().getParameters();
      if (null != mapParameters) {
        lifecycleParam = mapParameters.get("LIFECYCLE");
      }
    } else if (listenerEvent instanceof CreateTableEvent) {
      CreateTableEvent event = (CreateTableEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters) {
        lifecycleParam = mapParameters.get("LIFECYCLE");
      }
    } else if (listenerEvent instanceof DropTableEvent) {
      DropTableEvent event = (DropTableEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters) {
        lifecycleParam = mapParameters.get("LIFECYCLE");
      }
    } else if (listenerEvent instanceof AlterTableEvent) {
      AlterTableEvent event = (AlterTableEvent)listenerEvent;
      Map<String, String> oldParameters = event.getOldTable().getParameters();
      if (null != oldParameters) {
        lifecycleParam = oldParameters.get("LIFECYCLE");
      }
      Map<String, String> newParameters = event.getOldTable().getParameters();
      if (null != newParameters) {
        lifecycleParam = lifecycleParam + newParameters.get("LIFECYCLE");
      }
    } else if (listenerEvent instanceof AddPartitionEvent) {
      AddPartitionEvent event = (AddPartitionEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters) {
        lifecycleParam = mapParameters.get("LIFECYCLE");
      }
    } else if (listenerEvent instanceof AlterPartitionEvent) {
      AlterPartitionEvent event = (AlterPartitionEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters) {
        lifecycleParam = mapParameters.get("LIFECYCLE");
      }
    } else if (listenerEvent instanceof DropPartitionEvent) {
      DropPartitionEvent event = (DropPartitionEvent)listenerEvent;
      Map<String, String> mapParameters = event.getTable().getParameters();
      if (null != mapParameters) {
        lifecycleParam = mapParameters.get("LIFECYCLE");
      }
    }

    // When the lifecycle parameters are empty, no synchronization is required
    if (null == lifecycleParam || lifecycleParam.isEmpty()) {
      return true;
    } else {
      return false;
    }
  }
}
