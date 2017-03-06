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
package org.apache.ranger.utils;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;


public class DistributedLock {
  private static final Log LOGGER = LogFactory.getLog(DistributedLock.class);

  public static long value = 1;
  public static String servers = "192.168.30.107:2181";

  private static CuratorFramework curator = null;
  private static zkListener listener = null;
  private static ThreadLocal<InterProcessMutex> lockLocal = new ThreadLocal<InterProcessMutex>();

  public static void init(String servers) {
    synchronized (DistributedLock.class) {
      if(curator == null){
        curator = CuratorFrameworkFactory.builder().retryPolicy(new ExponentialBackoffRetry(1000, 3)).connectString(servers).build();
        listener = new zkListener();
        curator.getConnectionStateListenable().addListener(listener) ;
        curator.start();
      }
    }
  }

  public static boolean acquire(String key, long timeout){
    try {
      InterProcessMutex lock = new InterProcessMutex(DistributedLock.curator, key);
      lock.acquire(timeout, TimeUnit.SECONDS);
      lockLocal.set(lock);
    } catch (IllegalMonitorStateException e) {
      LOGGER.warn("DistributedLock acquire error", e);
      return false;
    } catch (Exception e) {
      LOGGER.warn("DistributedLock acquire error", e);
      return false;
    }
    return true;
  }
  public static void release(String key) {
    try {
      InterProcessMutex lock = lockLocal.get();
      lock.release();
    } catch (Exception e) {
      LOGGER.warn("DistributedLock release error",e);
    }
  }

  // test
  public static void main (String[] args) {
    DistributedLock.init("192.168.30.107:2181");

    final long start = System.currentTimeMillis();
    Executor pool = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 300; i ++) {
      pool.execute(new Runnable() {
        public void run() {
          while(true){
            boolean getLock = false;
            try {
              if(DistributedLock.acquire("/lock", 2)){
                getLock = true;
                value++;
                System.out.println(Thread.currentThread().getName()+","+value);
                //Thread.sleep(1);
                long end = System.currentTimeMillis();
                long tps = value/((end-start)/1000);
                System.out.println("tps count/s : "+tps);
              }
            } catch (Exception e) {
              e.printStackTrace();
            }finally{
              if(getLock){
                DistributedLock.release("/lock");
              }
            }
          }
        }
      });
    }
  }
}

class zkListener implements ConnectionStateListener{
  public static final Log LOGGER = LogFactory.getLog(DistributedLock.class);

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState state) {
    switch (state) {
      case LOST:
        LOGGER.warn("DistributedLock lost session with zookeeper");
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