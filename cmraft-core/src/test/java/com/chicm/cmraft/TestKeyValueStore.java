/**
* Copyright 2014 The CmRaft Project
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at:
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations
* under the License.
*/

package com.chicm.cmraft;

import static org.junit.Assert.*;

import org.apache.log4j.Level;
import org.junit.Test;

import com.chicm.cmraft.core.LocalCluster;
import com.google.common.base.Preconditions;

public class TestKeyValueStore {
  private static final int TESTS_NUMBER = 500;
  
  public static void main(String[] args) throws Exception {
    TestKeyValueStore t = new TestKeyValueStore();
    t.testKeyValueStore();
  }
  
  public Connection getConnection(LocalCluster cluster, int timeout) throws Exception {
    int totalTime = 0;
    Connection conn = null;
    while(true) {
      if(totalTime >= timeout) {
        return conn;
      }
      try {
        conn = ConnectionManager.getConnection(cluster.getConf(0));
      } catch(Exception e) {
        System.out.println("connect failed, reconnect...");
      }
      if(conn == null) {
        int interval = 1000;
        Thread.sleep(interval);
        totalTime+= interval;
      } else {
        //System.out.println("connected");
        return conn;
      }
    }
  }

  @Test
  public void testKeyValueStore() throws Exception {
    org.apache.log4j.LogManager.getRootLogger().setLevel(Level.INFO);
    LocalCluster cluster = LocalCluster.create(3,3, 12688);
    
    //cluster.checkNodesState();
    
    for(int i = 1; i <= TESTS_NUMBER; i++) {
      Connection conn = getConnection(cluster, 15000);
      Preconditions.checkNotNull(conn);
      KeyValueStore kvs = conn.getKeyValueStore();
      
      kvs.set("key" + i, "value"+i);
      assertTrue( kvs.get("key" + i).equals("value" + i));
      kvs.list("");
      kvs.delete("key" + i);
      
      conn.close();
    }
 
    cluster.checkNodesState();
  }
}
