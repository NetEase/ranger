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
