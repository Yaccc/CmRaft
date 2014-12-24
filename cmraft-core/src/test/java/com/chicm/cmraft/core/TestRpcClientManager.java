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

import java.nio.channels.AsynchronousChannel;

import org.junit.BeforeClass;
import org.junit.Test;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.rpc.RpcClient;

public class TestRpcClientManager {
  private static NodeConnectionManager mgr;
  
  @BeforeClass
  public static void init() {
    Configuration conf = CmRaftConfiguration.create();
    conf.set("raft.server.local", "chicm1:1111");
    conf.set("raft.server.remote.1", "chicm2:2222");
    conf.set("raft.server.remote.2", "chicm3:3333");
    
    mgr = new NodeConnectionManager(conf, null);
  }
  
  @Test
  public void testGetOtherServers() {
    assertFalse(mgr.getRemoteServers().contains(new ServerInfo("chicm1", 1111)));
    assertTrue(mgr.getRemoteServers().contains(new ServerInfo("chicm2", 2222)));
    assertTrue(mgr.getRemoteServers().contains(new ServerInfo("chicm3", 3333)));
  }
  
  
  @Test
  public void testRpcClientMap() {
    RaftNode[] nodes =LocalCluster.create(3, 3, 14888).getNodes(); 
    try {
      Thread.sleep(5000);
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
    for(int i = 0; i < nodes.length; i++) {
      NodeConnectionManager mgr = nodes[i].getNodeConnectionManager();
      
      //System.out.println(mgr.getThisServer());
      for(ServerInfo server: mgr.getRemoteServers()) {
        System.out.println(server);
        //RpcClient client = mgr.getRpcClient(server);
        //AsynchronousChannel channel = client.getChannel();
        //System.out.println("channel:" + channel);
      }
    }
    
  }
}
