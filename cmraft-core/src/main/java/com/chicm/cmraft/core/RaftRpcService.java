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

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.protobuf.generated.RaftProtos.AppendEntriesRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.AppendEntriesResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.DeleteRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.DeleteResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.GetRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.GetResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.KeyValuePair;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ListRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ListResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.LookupLeaderRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.LookupLeaderResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftLogEntry;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;
import com.chicm.cmraft.protobuf.generated.RaftProtos.SetRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.SetResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.TestRpcRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.TestRpcResponse;
import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RaftRpcService implements RaftService.BlockingInterface{
  static final Log LOG = LogFactory.getLog(RaftRpcService.class);
  private final static String RPC_TIMEOUT_KEY = "raft.rpc.timeout";
  private final static int DEFAULT_RPC_TIMEOUT = 3000;
  private Configuration config;
  
  private RaftNode node = null;
  //private TimeoutListener listener = null;
  
  // While created from RpcClient, node and listener will be null
  public static RaftRpcService create() {
    return new RaftRpcService(null);
  }
  
  public static RaftRpcService create(RaftNode node, Configuration conf) {
    RaftRpcService service = new RaftRpcService(node);
    service.config = conf;
    return service;
  }
  
  private RaftRpcService(RaftNode node) {
    this.node = node;
    /*
    if(node != null) {
      this.listener = node.getEventListener();
    }*/
  }
  
  public RaftNode getRaftNode() {
    return node;
  }
  
  public BlockingService getService() {
    return RaftService.newReflectiveBlockingService(this);
  }
  
  @Override
  public TestRpcResponse testRpc(RpcController controller, TestRpcRequest request)
      throws ServiceException {
    TestRpcResponse.Builder builder = TestRpcResponse.newBuilder();
    byte[] bytes = new byte[500];
    builder.setResult( ByteString.copyFrom(bytes));
    
    return builder.build();    
  }
  
  // For followers, handle appendEntries RPC from Leader
  @Override
  public AppendEntriesResponse appendEntries(RpcController controller, AppendEntriesRequest request)
      throws ServiceException {
    LOG.debug(getRaftNode().getName() + "appendEntries CALLED, FROM:" + ServerInfo.copyFrom(request.getLeaderId()));
    
    Preconditions.checkNotNull(node);
    Preconditions.checkArgument(!node.getServerInfo().equals(ServerInfo.copyFrom( request.getLeaderId())), 
      "appendEntries RPC should not be called from local node:%s", node.getServerInfo());
    
    getRaftNode().discoverLeader(ServerInfo.copyFrom(request.getLeaderId()), request.getTerm());
    if(request.getTerm() > node.getCurrentTerm()) {
      getRaftNode().discoverHigherTerm(ServerInfo.copyFrom(request.getLeaderId()), request.getTerm());
    }
    
    // always reset timer not mater whether it is a heart beat,
    // normal appendEntries RPC can also be treated as heart beat.
    node.resetTimer(); 
    boolean heartbeat = true;
    AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder();
    if(request.getEntriesCount() <= 0) {
      LOG.debug(getRaftNode().getName() + ": heartbeat, from: " + request.getLeaderId());
    } else {
      LOG.info("appendEntries RPC, from: " + request.getLeaderId() + ", to: " + node.getName());
      LOG.info("log entries:" + request.getEntriesList().toString());
      heartbeat = false;
    }
    boolean success = false;
    
    try {
    success = node.getRaftLog().appendEntries(request.getTerm(), 
        ServerInfo.copyFrom(request.getLeaderId()), 
        request.getLeaderCommit(), request.getPrevLogIndex(), 
        request.getPrevLogTerm(), 
        request.getEntriesList());
    } catch(Exception e) {
      LOG.error("appendEntries exception", e);
    }
    
    if(!heartbeat) {
      LOG.info(node.getName() + "appendEntries, returned: " + success);
    }
    
    builder.setTerm(node.getCurrentTerm());
    builder.setSuccess(/*request.getTerm() >= node.getCurrentTerm()*/success);
    
    return builder.build();
  }
  
  @Override
  public CollectVoteResponse collectVote(RpcController controller, CollectVoteRequest request)
      throws ServiceException {
    LOG.debug(getRaftNode().getName() + ": received vote request from: " + "{" + request + "}" );
    
    if(request.getTerm() > node.getCurrentTerm()) {
      getRaftNode().discoverHigherTerm(ServerInfo.copyFrom(request.getCandidateId()), request.getTerm());
    }
    
    boolean granted = getRaftNode().voteRequest(new ServerInfo(request.getCandidateId().getHostName(), 
      request.getCandidateId().getPort()), request.getTerm(), request.getLastLogIndex(), request.getLastLogTerm());
    
    LOG.debug(getRaftNode().getName() + ": voted: " + granted + " candidate: " 
      + request.getCandidateId().getHostName() + ":" + request.getCandidateId().getPort());
    
    CollectVoteResponse.Builder builder = CollectVoteResponse.newBuilder();
    builder.setGranted(granted);
    builder.setTerm(getRaftNode().getCurrentTerm());
    builder.setFromHost(getRaftNode().getServerInfo().toServerId());
    
    return builder.build();
  }

  @Override
  public LookupLeaderResponse lookupLeader(RpcController controller, LookupLeaderRequest request)
      throws ServiceException {
    Preconditions.checkNotNull(node);
    ServerInfo leader = node.getCurrentLeader();
    if(leader == null) {
      int rpcTimeout = config.getInt(RPC_TIMEOUT_KEY, DEFAULT_RPC_TIMEOUT);
      try {
        Thread.sleep(rpcTimeout/2);
      } catch(Exception e) {
        LOG.error("sleep exception", e);
      }
      leader = node.getCurrentLeader();
    }
    //ServerId leaderId = leader.toServerId();
    LookupLeaderResponse.Builder builder = LookupLeaderResponse.newBuilder();
    if(leader != null) {
      builder.setSuccess(true);
      builder.setLeader(leader.toServerId());
      LOG.debug(getRaftNode().getName() + ": lookupLeader responded: [" + leader.getHost() 
        + ":" + leader.getPort() + "]");
    } else {
      builder.setSuccess(false);
      LOG.debug(getRaftNode().getName() + ": lookupLeader responded: null");
    }
    
    return builder.build();
  }

  @Override
  public GetResponse get(RpcController controller, GetRequest request) throws ServiceException {
    byte[] key = request.getKey().toByteArray();
    byte[] value = node.getRaftLog().get(key);
    
    GetResponse.Builder builder = GetResponse.newBuilder();
    if(value != null) {
      builder.setSuccess(true);
      builder.setValue(ByteString.copyFrom(value));
    } else {
      builder.setSuccess(false);
    }
    
    return builder.build();
  }

  @Override
  public SetResponse set(RpcController controller, SetRequest request) throws ServiceException {
    LOG.debug(node.getName() + ": set request responded");
    SetResponse.Builder builder = SetResponse.newBuilder();
    
    boolean success = node.getRaftLog().set(request.getKv());
    builder.setSuccess(success);
    
    return builder.build();
  }

  @Override
  public DeleteResponse delete(RpcController controller, DeleteRequest request)
      throws ServiceException {
    DeleteResponse.Builder builder = DeleteResponse.newBuilder();
    
    builder.setSuccess(node.getRaftLog().delete(request.getKey().toByteArray()));
        
    return builder.build();
  }

  @Override
  public ListResponse list(RpcController controller, ListRequest request) throws ServiceException {
    LOG.info(node.getName() + ": list request responded");
    ListResponse.Builder builder = ListResponse.newBuilder();
    Collection<KeyValuePair> col = node.getRaftLog().list(request.getPattern().toByteArray());
    for(KeyValuePair entry: col) {
      builder.addResults(entry);
    }
    builder.setSuccess(true);
    return builder.build();
  }
}
