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

package com.chicm.cmraft.common;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;

public class TestServerInfo {
  private static Configuration conf = CmRaftConfiguration.create();
  
  @BeforeClass
  public static void init() {
    conf.set("raft.server.local", "chicm:5555");
    conf.set("raft.server.remote.1", "chicm:1111");
    conf.set("raft.server.remote.2", "chicm:2222");
    conf.set("raft.server.remote.3", "chicm:3333");
  }

  @Test
  public void testServerId() {
    ServerInfo thisServer = ServerInfo.parseFromString(conf.getString("raft.server.local"));
    for (String key: conf.getKeys("raft.server.remote")) {
      System.out.println(key);
      ServerInfo server = ServerInfo.parseFromString(conf.getString(key));
      System.out.println(server);
    }
    ServerId sid = thisServer.toServerId();
    System.out.println(sid);
  }
 
}
