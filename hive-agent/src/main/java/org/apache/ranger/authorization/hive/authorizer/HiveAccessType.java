package org.apache.ranger.authorization.hive.authorizer;

public enum HiveAccessType {
  NONE, CREATE, ALTER, DROP, INDEX, LOCK, SELECT, UPDATE, USE, ALL, ADMIN
}
