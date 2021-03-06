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

package org.apache.ranger.services.hdfs.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.apache.ranger.services.hdfs.client.HdfsClient;


public class HdfsConnectionMgr {

	protected ConcurrentMap<String, HdfsClient> 	hdfsConnectionCache = null;
	protected ConcurrentMap<String, Boolean> 		repoConnectStatusMap = null;

	private static Logger LOG = Logger.getLogger(HdfsConnectionMgr.class);
	
	public HdfsConnectionMgr(){
		hdfsConnectionCache  = new ConcurrentHashMap<String, HdfsClient>();
		repoConnectStatusMap = new ConcurrentHashMap<String, Boolean>();
	}
	
	
	public HdfsClient getHadoopConnection(final String serviceName, final String serviceType, final Map<String,String> configs) {
		HdfsClient hdfsClient = null;
		if (serviceType != null) {
			// get it from the cache
				hdfsClient = hdfsConnectionCache.get(serviceName);
				if (hdfsClient == null) {
					if(configs == null) {
						final Callable<HdfsClient> connectHDFS = new Callable<HdfsClient>() {
							@Override
							public HdfsClient call() throws Exception {
								return new HdfsClient(serviceName, configs);
							}
						};
						
						try {
							hdfsClient = TimedEventUtil.timedTask(connectHDFS, 10, TimeUnit.SECONDS);
						} catch(Exception e){
							LOG.error("Error establishing connection for HDFS repository : "
									+ serviceName, e);
						}
						
					} else {
												
						final Callable<HdfsClient> connectHDFS = new Callable<HdfsClient>() {
							@Override
							public HdfsClient call() throws Exception {
								return new HdfsClient(serviceName, configs);
							}
						};
						
						try {
							hdfsClient = TimedEventUtil.timedTask(connectHDFS, 5, TimeUnit.SECONDS);
						} catch(Exception e){
							LOG.error("Error establishing connection for HDFS repository : "
									+ serviceName + " using configuration : " + configs, e);
						}
					}	
					HdfsClient oldClient = hdfsConnectionCache.putIfAbsent(serviceName, hdfsClient);
					if (oldClient != null) {
						// in the meantime someone else has put a valid client into the cache, let's use that instead.
						hdfsClient = oldClient;
					}
					repoConnectStatusMap.put(serviceName, true);
 				} else {
					List<String> testConnect = hdfsClient.listFiles("/", "*",null);
					if(testConnect == null){
						hdfsConnectionCache.put(serviceName, hdfsClient);
						hdfsClient = getHadoopConnection(serviceName,serviceType,configs);
					}
				}
		} else {
			LOG.error("Serice not found with name "+serviceName, new Throwable());
		}

		return hdfsClient;
	}
}
