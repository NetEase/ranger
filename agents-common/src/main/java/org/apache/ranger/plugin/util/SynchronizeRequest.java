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
package org.apache.ranger.plugin.util;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.*;

@JsonAutoDetect(getterVisibility= JsonAutoDetect.Visibility.NONE, setterVisibility= JsonAutoDetect.Visibility.NONE, fieldVisibility= JsonAutoDetect.Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class SynchronizeRequest extends GrantRevokeRequest{
  private String              tableType                  = null;
  private String              location                   = null;
  private String              newLocation                = null;
  private Map<String, String> newResource                = null;

  public SynchronizeRequest() {
    this(null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null);
  }

  public SynchronizeRequest(String grantor, Map<String, String> resource, Set<String> users,
                            Set<String> groups, Set<String> accessTypes, Boolean delegateAdmin,
                            Boolean enableAudit, Boolean replaceExistingPermissions, Boolean isRecursive,
                            String clientIPAddress, String clientType, String requestData,
                            String sessionId, String tableType, String location,
                            String newLocation, Map<String, String> newResource) {
    setGrantor(grantor);
    setResource(resource);
    setUsers(users);
    setGroups(groups);
    setAccessTypes(accessTypes);
    setDelegateAdmin(delegateAdmin);
    setEnableAudit(enableAudit);
    setReplaceExistingPermissions(replaceExistingPermissions);
    setIsRecursive(isRecursive);
    setClientIPAddress(clientIPAddress);
    setClientType(clientType);
    setRequestData(requestData);
    setSessionId(sessionId);
    setTableType(tableType);
    setLocation(location);
    setNewLocation(newLocation);
    setNewResource(newResource);
  }

  /**
   * @return the newLocation
   */
  public String getTableType() {
    return tableType;
  }

  /**
   * @param tableType the location to set
   */
  public void setTableType(String tableType) {
    this.tableType = tableType == null ? "" : tableType;
  }

  /**
   * @return the newLocation
   */
  public String getNewLocation() {
    return newLocation;
  }

  /**
   * @param location the location to set
   */
  public void setNewLocation(String location) {
    this.newLocation = location == null ? "" : location;
  }

  /**
   * @return the location
   */
  public String getLocation() {
    return location;
  }

  /**
   * @param location the location to set
   */
  public void setLocation(String location) {
    this.location = location == null ? "" : location;
  }

  /**
   * @return the resource
   */
  public Map<String, String> getNewResource() {
    return newResource;
  }

  /**
   * @param resource the resource to set
   */
  public void setNewResource(Map<String, String> resource) {
    this.newResource = resource == null ? new HashMap<String, String>() : resource;
  }

  @Override
  public String toString( ) {
    StringBuilder sb = new StringBuilder();

    toString(sb);

    return sb.toString();
  }

  public StringBuilder toString(StringBuilder sb) {
    sb.append("SyncHdfsPolicyRequest={");

    sb.append("grantor={").append(this.getGrantor()).append("} ");

    sb.append("resource={");
    if(this.getResource() != null) {
      for(Map.Entry<String, String> e : this.getResource().entrySet()) {
        sb.append(e.getKey()).append("=").append(e.getValue()).append("; ");
      }
    }
    sb.append("} ");
    sb.append("newResource={");
    if(this.getNewResource() != null) {
      for(Map.Entry<String, String> e : this.getNewResource().entrySet()) {
        sb.append(e.getKey()).append("=").append(e.getValue()).append("; ");
      }
    }
    sb.append("} ");

    sb.append("users={");
    if(this.getUsers() != null) {
      for(String user : this.getUsers()) {
        sb.append(user).append(" ");
      }
    }
    sb.append("} ");

    sb.append("groups={");
    if(this.getGroups() != null) {
      for(String group : this.getGroups()) {
        sb.append(group).append(" ");
      }
    }
    sb.append("} ");

    sb.append("accessTypes={");
    if(this.getAccessTypes() != null) {
      for(String accessType : this.getAccessTypes()) {
        sb.append(accessType).append(" ");
      }
    }
    sb.append("} ");

    sb.append("delegateAdmin={").append(this.getDelegateAdmin()).append("} ");
    sb.append("enableAudit={").append(this.getEnableAudit()).append("} ");
    sb.append("replaceExistingPermissions={").append(this.getReplaceExistingPermissions()).append("} ");
    sb.append("isRecursive={").append(this.getIsRecursive()).append("} ");
    sb.append("clientIPAddress={").append(this.getClientIPAddress()).append("} ");
    sb.append("clientType={").append(this.getClientType()).append("} ");
    sb.append("requestData={").append(this.getRequestData()).append("} ");
    sb.append("sessionId={").append(this.getSessionId()).append("} ");
    sb.append("location={").append(location).append("} ");
    sb.append("newLocation={").append(newLocation).append("} ");

    sb.append("}");

    return sb;
  }
}
