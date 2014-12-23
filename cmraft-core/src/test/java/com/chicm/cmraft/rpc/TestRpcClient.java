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

package com.chicm.cmraft.rpc;


import org.junit.Test;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.core.RaftRpcService;

public class TestRpcClient {

  public static void main(String[] args) throws Exception {
    //RpcServer server = new RpcServer(CmRaftConfiguration.create(), RaftRpcService.create());
    //server.startRpcServer();
    //server.startTPSReport();
    Configuration conf = CmRaftConfiguration.create();
    int port = ServerInfo.parseFromString(conf.getString("raft.server.local")).getPort();
      
      for(int i =0; i < 20; i++) {
        final RpcClient client = new RpcClient(CmRaftConfiguration.create(), new ServerInfo( "localhost", 12888));
  
        client.sendRequest(1024*1024);
    
        //Thread.sleep(100);
      }
      System.out.println("closing");
      //client.close();
      System.out.println("closed");
  }

}


