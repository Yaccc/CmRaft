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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.log.LogEntry;
import com.chicm.cmraft.log.LogMutationType;
import com.chicm.cmraft.protobuf.generated.RaftProtos.AppendEntriesRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.AppendEntriesResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.DeleteRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.DeleteResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.GetRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.GetResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.KeyValue;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ListRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ListResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.LookupLeaderRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.LookupLeaderResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftEntry;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;
import com.chicm.cmraft.protobuf.generated.RaftProtos.SetRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.SetResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.TestRpcRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.TestRpcResponse;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RaftRpcService implements RaftService.BlockingInterface{
  static final Log LOG = LogFactory.getLog(RaftRpcService.class);
  
  private RaftNode node = null;
  //private TimeoutListener listener = null;
  
  // While created from RpcClient, node and listener will be null
  public static RaftRpcService create() {
    return new RaftRpcService(null);
  }
  
  public static RaftRpcService create(RaftNode node) {
    return new RaftRpcService(node);
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
  
  
  @Override
  public AppendEntriesResponse appendEntries(RpcController controller, AppendEntriesRequest request)
      throws ServiceException {
    LOG.info(getRaftNode().getName() + "appendEntries CALLED, FROM:" + ServerInfo.parseFromServerId(request.getLeaderId()));
    if(node == null) {
      LOG.error("RaftNode is null");
      return null;
    }
    
    getRaftNode().discoverLeader(ServerInfo.parseFromServerId(request.getLeaderId()), request.getTerm());
    if(request.getTerm() > node.getCurrentTerm()) {
      getRaftNode().discoverHigherTerm(ServerInfo.parseFromServerId(request.getLeaderId()), request.getTerm());
    }
    
    if(node.getServerInfo().getPort() == request.getLeaderId().getPort()) {
      Exception e = new Exception("heartbeat From my self");
      e.printStackTrace(System.out);
    }
    
    AppendEntriesResponse.Builder builder = AppendEntriesResponse.newBuilder();
    if(request.getEntriesCount() <= 0) {
      LOG.info(getRaftNode().getName() + ": HEARTBEAT CALLED**!");
      node.resetTimer();
    } else { // entry number >0
      node.getLogManager().appendEntries(request.getTerm(), 
        ServerInfo.parseFromServerId(request.getLeaderId()), 
        request.getLeaderCommit(), request.getPrevLogIndex(), 
        request.getPrevLogTerm(), 
        buildLogEntriesFromRaftEntries(request.getEntriesList(), request.getTerm()));
    }
    builder.setTerm(node.getCurrentTerm());
    builder.setSuccess(request.getTerm() >= node.getCurrentTerm());
    
    return builder.build();
  }
  
  private List<LogEntry> buildLogEntriesFromRaftEntries(List<RaftEntry> raftEntries, long term) {
    if(raftEntries == null) {
      return null;
    }
    List<LogEntry> result = new ArrayList<LogEntry>();
    for(RaftEntry r: raftEntries) {
      LogEntry logEntry = new LogEntry(r.getIndex(), term, r.getKey().toByteArray(), 
        r.getValue().toByteArray(), LogMutationType.SET); //todo: add type to raft entry
      result.add(logEntry);
    }
    return result;
  }

  @Override
  public CollectVoteResponse collectVote(RpcController controller, CollectVoteRequest request)
      throws ServiceException {
    LOG.debug(getRaftNode().getName() + ": received vote request from: " + "{" + request + "}" );
    
    if(request.getTerm() > node.getCurrentTerm()) {
      getRaftNode().discoverHigherTerm(ServerInfo.parseFromServerId(request.getCandidateId()), request.getTerm());
    }
    
    ServerId.Builder sbuilder = ServerId.newBuilder();
    sbuilder.setHostName(getRaftNode().getServerInfo().getHost());
    sbuilder.setPort(getRaftNode().getServerInfo().getPort());
    sbuilder.setStartCode(getRaftNode().getServerInfo().getStartCode());
    
    boolean granted = getRaftNode().voteRequest(new ServerInfo(request.getCandidateId().getHostName(), 
      request.getCandidateId().getPort()), request.getTerm(), request.getLastLogIndex(), request.getLastLogTerm());
    
    LOG.info(getRaftNode().getName() + ": voted: " + granted + " candidate: " 
      + request.getCandidateId().getHostName() + ":" + request.getCandidateId().getPort());
    
    CollectVoteResponse.Builder builder = CollectVoteResponse.newBuilder();
    builder.setGranted(granted);
    builder.setTerm(getRaftNode().getCurrentTerm());
    builder.setFromHost(sbuilder.build());
    
    return builder.build();
  }

  @Override
  public LookupLeaderResponse lookupLeader(RpcController controller, LookupLeaderRequest request)
      throws ServiceException {
    ServerId leader = node.getCurrentLeader().toServerId();
    LookupLeaderResponse.Builder builder = LookupLeaderResponse.newBuilder();
    if(leader != null) {
      builder.setLeader(leader);
      LOG.info(getRaftNode().getName() + ": lookupLeader responded: [" + leader.getHostName() 
        + ":" + leader.getPort() + "]");
    } else {
      LOG.info(getRaftNode().getName() + ": lookupLeader responded: null");
    }
    
    return builder.build();
  }

  @Override
  public GetResponse get(RpcController controller, GetRequest request) throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SetResponse set(RpcController controller, SetRequest request) throws ServiceException {
    LOG.info(node.getName() + ": set request responded");
    SetResponse.Builder builder = SetResponse.newBuilder();
    
    boolean success = node.getLogManager().set(request.getKey().toByteArray(), request.getValue().toByteArray());
    builder.setSuccess(success);
    
    return builder.build();
  }

  @Override
  public DeleteResponse delete(RpcController controller, DeleteRequest request)
      throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ListResponse list(RpcController controller, ListRequest request) throws ServiceException {
    LOG.info(node.getName() + ": list request responded");
    ListResponse.Builder builder = ListResponse.newBuilder();
    Collection<LogEntry> col = node.getLogManager().list(request.getPattern().toByteArray());
    //int i=0;
    for(LogEntry entry: col) {
      KeyValue.Builder kvbuilder = KeyValue.newBuilder();
      kvbuilder.setKey(ByteString.copyFrom(entry.getKey()));
      kvbuilder.setValue(ByteString.copyFrom(entry.getValue()));
      builder.addResults(kvbuilder.build());
      //builder.
    }
    builder.setSuccess(true);
    return builder.build();
  }
}
