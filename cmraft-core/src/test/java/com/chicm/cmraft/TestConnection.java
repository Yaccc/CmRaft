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
import com.chicm.cmraft.core.RaftNode;

public class TestConnection {
  
  public static void main(String[] args) throws Exception {
    TestConnection t = new TestConnection();
    t.testConnection();
  }

  @Test
  public void testConnection() throws Exception {
    //org.apache.log4j.LogManager.getRootLogger().setLevel(Level.DEBUG);
    LocalCluster cluster = LocalCluster.create(2, 12688);
    Thread.sleep(10000);
    RaftNode[] nodes = cluster.getNodes();
    cluster.checkNodesState();
    
    for(int i = 0; i < nodes.length; i++) {
      System.out.println(nodes[i].getServerInfo() + " commit:" + nodes[i].getRaftLog().getCommitIndex());
      System.out.println(nodes[i].getServerInfo() +" lastapplied:" + nodes[i].getRaftLog().getLastApplied());
      System.out.println(nodes[i].getServerInfo() +" flushed:" + nodes[i].getRaftLog().getFlushedIndex());
      System.out.println(nodes[i].getServerInfo() +" log term:" + nodes[i].getRaftLog().getLogTerm(nodes[i].getRaftLog().getCommitIndex()));
      System.out.println(nodes[i].getServerInfo() +" current term:" + nodes[i].getCurrentTerm());
    }
    System.out.println("*****************************************");
    
    Connection conn = ConnectionManager.getConnection(cluster.getConf(0));
    KeyValueStore kvs = conn.getKeyValueStore();
    
    for(int i = 1; i <= 50; i++) {
      
      kvs.set("key" + i, "value"+i);
    }
    
    Thread.sleep(3000);
    
    for(int i = 0; i < nodes.length; i++) {
      System.out.println(nodes[i].getServerInfo() + " commit:" + nodes[i].getRaftLog().getCommitIndex());
      System.out.println(nodes[i].getServerInfo() +" lastapplied:" + nodes[i].getRaftLog().getLastApplied());
      System.out.println(nodes[i].getServerInfo() +" flushed:" + nodes[i].getRaftLog().getFlushedIndex());
      System.out.println(nodes[i].getServerInfo() +" log term:" + nodes[i].getRaftLog().getLogTerm(nodes[i].getRaftLog().getCommitIndex()));
      System.out.println(nodes[i].getServerInfo() +" current term:" + nodes[i].getCurrentTerm());
    }
    /*
    KeyValue r = conn.list("");
    for(byte[] b: r.keySet()) {
      System.out.println(new String(b));
    }
    
    //cluster.killLeader();
    
    Thread.sleep(10000);
    cluster.checkNodesState();
    
    Connection conn2 = ConnectionManager.getConnection(cluster.getConf(0));
    KeyValue r2 = conn.list("");
    for(byte[] b: r2.keySet()) {
      System.out.println(new String(b));
    }
    cluster.checkNodesState();*/
  }
}
