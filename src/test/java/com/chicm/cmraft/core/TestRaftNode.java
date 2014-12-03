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

package com.chicm.cmraft.core;

import static org.junit.Assert.*;

import java.lang.reflect.Method;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestRaftNode {
  private static LocalCluster cluster;
  private static RaftNode[] nodes;
  
  public static void main(String[] args) throws Exception {
    init();
    Thread.sleep(100000);
  }
  
  @BeforeClass
  public static void init() {
    org.apache.log4j.LogManager.getRootLogger().setLevel(Level.ERROR);
    cluster = LocalCluster.create(3, 13555);
    nodes = cluster.getNodes();
    
  }
  
  @Test
  public void testRaftNode() throws Exception{
    Thread.sleep(10000);
    cluster.printNodesState();
    
    for(int i =0; i< 100; i++) { 
      for(RaftNode n :nodes)
      n.testHearBeat();
      
    }
    cluster.printNodesState();
    Thread.sleep(10000);
    cluster.printNodesState();
  }
  
  @Test
  public void testGetElectionTimeout() throws Exception {
    int confTimeout = cluster.getConf(0).getInt("raft.election.timeout");
    Method m = nodes[0].getClass().getDeclaredMethod("getElectionTimeout", new Class[] {});
    m.setAccessible(true);
    
    for(int i = 0; i < 100; i++) {
      Object result = m.invoke(nodes[0], null);
      int resultInt = ((Integer)result).intValue();
      assertTrue(resultInt >= confTimeout);
      assertTrue(resultInt <= confTimeout*2);
    }
  }

}
