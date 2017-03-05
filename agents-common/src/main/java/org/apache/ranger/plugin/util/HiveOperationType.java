package org.apache.ranger.plugin.util;

// fork package org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
public enum HiveOperationType {
  TYPE_INIT,
  CREATEDATABASE,
  DROPDATABASE,
  CREATETABLE,
  ALTERTABLE,
  DROPTABLE,
  ALTERTABLE_ADDPARTS,
  ALTERTABLE_DROPPARTS,
  ALTERTABLE_ALTERPARTITION;

  private HiveOperationType() {
  }
}