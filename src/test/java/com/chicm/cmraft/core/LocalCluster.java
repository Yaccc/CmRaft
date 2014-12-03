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

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;

import static org.junit.Assert.*;

public class LocalCluster {
  
  private static final int NODE_NUMBER = 3;
  private static final int START_PORT = 12888;
  
  private int nodeNumber = NODE_NUMBER;
  private int startPort = START_PORT;
  
  private Configuration[] confs;
  private RaftNode[] nodes;

  public LocalCluster() {
  }
  
  public static LocalCluster create(int n, int startPort) {
    LocalCluster cluster = new LocalCluster();
    cluster.createCluster(n, startPort);
    return cluster;
  }
  
  public Configuration getConf(int index) {
    return confs[index];
  }
  
  public RaftNode[] getNodes() {
    return nodes;
  }
  
  private void createCluster(int n, int startPort) {
    this.nodeNumber = n;
    this.startPort = startPort;
    
    createConfiguration();
    
    nodes = new RaftNode[nodeNumber];
    for(int i = 0; i < nodeNumber; i++) {
      nodes[i] = new RaftNode(confs[i]);
    }
  }
  
  public void checkNodesState() {
    int nLeader = 0;
    int nFollower = 0;
    
    System.out.println("******************");
    for (int i =0; i < nodeNumber; i++) {
      System.out.println(nodes[i].getName() + ":" + nodes[i].getState() 
        + "(" + nodes[i].getCurrentTerm() + ")");
      if(nodes[i].getState() == State.LEADER) {
        nLeader++;
      } else if(nodes[i].getState() == State.FOLLOWER) {
        nFollower++;
      }
    }
    assertTrue(nLeader == 1);
    assertTrue(nFollower == (nodeNumber -1));
  }
  
  public void checkGetCurrentLeader() {

    for (int i =0; i < nodeNumber; i++) {
      if(i != 0) {
        assertTrue(nodes[i].getCurrentLeader().equals(nodes[i-1].getCurrentLeader()));
      }
    }
  }
  
  public void killLeader() {
    for(RaftNode node: nodes) {
      //System.out.println(node.getName() + ":" + node.getState() + ":" + node.getCurrentTerm());
      if(node.isLeader()) {
        System.out.println(node.getServerInfo() + " is leader, killing it");
        node.kill();
      }
    }
  }
  
  private void createConfiguration() {
    confs = new Configuration[nodeNumber];
    for(int i = 0; i < nodeNumber; i++) {
      confs[i] = CmRaftConfiguration.create();
      confs[i].useResource("cmraft_cluster_test.properties");
      for(int j=0; j < nodeNumber;j++) {
        confs[i].set("raft.server.remote." + j, "localhost:" + (startPort+j));
      }
      confs[i].remove("raft.server.remote." + i);
      confs[i].set("raft.server.local", "localhost:" + (startPort+i));
      
      System.out.println("confs[" + i + "]:\n" + confs[i].toString());
    }
  }

}
