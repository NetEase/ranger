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

 package org.apache.ranger.unixusersync.model;

import java.util.ArrayList;
import java.util.List;

public class XUserInfo {
	private String id ;
	private String name ;
	private String 	description ;
	
	private List<String>  	groupNameList = new ArrayList<String>() ;
	private List<String>    groupIdList   = new ArrayList<String>() ;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	
	public void setGroupNameList(List<String> groupNameList) {
		this.groupNameList = groupNameList;
	}
	
	public List<String> getGroupNameList() {
		return groupNameList;
	}
	
	public List<String> getGroupIdList() {
		return groupIdList;
	}
	
	public void setGroupIdList(List<String> groupIdList) {
		this.groupIdList = groupIdList;
	}

	public List<String> getGroups() {
		return groupNameList;
	}
	
}
