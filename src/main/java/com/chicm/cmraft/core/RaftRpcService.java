/**
* Copyright 2014 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerListRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerListResponse;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RaftRpcService implements RaftService.BlockingInterface{
  static final Log LOG = LogFactory.getLog(RaftRpcService.class);
  
  private RaftNode node = null;
  
  public static RaftRpcService create() {
    return new RaftRpcService(null);
  }
  
  public static RaftRpcService create(RaftNode node) {
    return new RaftRpcService(node);
  }
  
  private RaftRpcService(RaftNode node) {
    this.node = node;
  }
  
  @Override
  public HeartBeatResponse beatHeart(RpcController controller, HeartBeatRequest request)
      throws ServiceException {
    LOG.debug("beatHeart called");
    
    HeartBeatResponse.Builder builder = HeartBeatResponse.newBuilder();
    
    return builder.build();
  }

  @Override
  public ServerListResponse listServer(RpcController controller, ServerListRequest request)
      throws ServiceException {
    return null;
  }

  public BlockingService getService() {
    return RaftService.newReflectiveBlockingService(this);
  }

  @Override
  public CollectVoteResponse collectVote(RpcController controller, CollectVoteRequest request)
      throws ServiceException {
    CollectVoteResponse.Builder builder = CollectVoteResponse.newBuilder();
    builder.setGranted(true);
    builder.setTerm(0);
    
    return builder.build();
  }
}
