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

import org.apache.log4j.Level;
import org.junit.Test;

import com.chicm.cmraft.core.LocalCluster;

public class TestConnectionManager {
  
  public static void main(String[] args) throws Exception {
    TestConnectionManager t = new TestConnectionManager();
    t.testGetConnection();
  }

  @Test
  public void testGetConnection() throws Exception {
    org.apache.log4j.LogManager.getRootLogger().setLevel(Level.ERROR);
    LocalCluster cluster = LocalCluster.create(5, 22888);
    Thread.sleep(10000);
    
    cluster.checkNodesState();
    
    Connection conn = ConnectionManager.getConnection(cluster.getConf(0));
    for(int i = 0; i < 100; i++) {
      conn.set("key" + i, "value"+i);
    }
    
    Result r = conn.list("");
    for(byte[] b: r.keySet()) {
      System.out.println(new String(b));
    }
    
    cluster.killLeader();
    
    Thread.sleep(10000);
    cluster.checkNodesState();
    
    Connection conn2 = ConnectionManager.getConnection(cluster.getConf(0));
    Result r2 = conn.list("");
    for(byte[] b: r2.keySet()) {
      System.out.println(new String(b));
    }
    cluster.checkNodesState();
  }
}
