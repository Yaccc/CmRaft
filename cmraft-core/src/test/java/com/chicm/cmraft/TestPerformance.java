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

public class TestPerformance {
  private static final int TEST_NUMBERS = 2000;
  
  public static void main(String[] args) throws Exception {
    TestPerformance t = new TestPerformance();
    t.testAllNodes();
  }

  @Test
  public void testAllNodes() throws Exception {
    long tm = doTests(5, 5);  
    System.out.println("all nodes total time:" + tm);
  }
  
  @Test
  public void testPartialNodes() throws Exception {
    long tm = doTests(5, 3);  
    
    System.out.println("partial nodes total time:" + tm);
  }
  
  public long doTests(int nNodes, int startNodes) throws Exception {
    org.apache.log4j.LogManager.getRootLogger().setLevel(Level.DEBUG);
    LocalCluster cluster = LocalCluster.create(nNodes, startNodes, 14688);
    Thread.sleep(10000);
    cluster.checkNodesState();
    
    long tm = System.currentTimeMillis();
    Connection conn = ConnectionManager.getConnection(cluster.getConf(0));
    KeyValueStore kvs = conn.getKeyValueStore();
    for(int i = 1; i <= TEST_NUMBERS; i++) {
      kvs.set("key" + i, "value"+i);
      kvs.get("key" + i);
    }
    conn.close();
    long total = System.currentTimeMillis() - tm;
    
    cluster.checkNodesState();
    return total;
  }
}
