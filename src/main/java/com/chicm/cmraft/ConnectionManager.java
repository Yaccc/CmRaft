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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.protobuf.generated.RaftProtos.KeyValuePair;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ListRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ListResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.LookupLeaderRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.LookupLeaderResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.SetRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.SetResponse;
import com.chicm.cmraft.rpc.RpcClient;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

public class ConnectionManager {
  static final Log LOG = LogFactory.getLog(ConnectionManager.class);
  private RpcClient rpcClient;
  private ConnectionImpl userConnection;
  private KeyValueStore kvs;
  
  private ConnectionManager(Configuration conf, ServerInfo server) {
    rpcClient = new RpcClient(conf, server);
    userConnection = new ConnectionImpl();
    kvs = new KeyValueStoreImpl();
  }
  
  public static Connection getConnection() {
    return getConnection(CmRaftConfiguration.create());
  }
  
  public static Connection getConnection(Configuration conf) {
    
    ServerInfo server = ServerInfo.parseFromString(conf.getString("raft.server.local"));
    ConnectionManager mgr = new ConnectionManager(conf, server);
    
    ServerInfo leader = mgr.lookupLeader();
    if(leader == null)
      return null;
    LOG.info("LOOKUPLEADER RETURNED:" + leader);
    if(leader.equals(server)) {
      return mgr.userConnection;
    } else {
      mgr.close();
      ConnectionManager leaderMgr = new ConnectionManager(conf, leader);
      return leaderMgr.userConnection;
    }
  }
  
  public void close() {
    rpcClient.close();
  }
  
  private ServerInfo lookupLeader() {
    LookupLeaderRequest.Builder builder = LookupLeaderRequest.newBuilder();
    try {
      LookupLeaderResponse response = rpcClient.getStub().lookupLeader(null, builder.build());
      if(response.getLeader() != null) {
        return ServerInfo.copyFrom(response.getLeader());
      }
    } catch(ServiceException e) {
      LOG.error("lookupLeader failed", e);
    }
    return null;
  }
  
  private class ConnectionImpl implements Connection {

    @Override
    public KeyValueStore getKeyValueStore() {
      return kvs;
    }

    @Override
    public void close() {
      rpcClient.close();
    }
    
  }
  
  private class KeyValueStoreImpl implements KeyValueStore {

    @Override
    public boolean set(byte[] key, byte[] value) {
      
      KeyValuePair.Builder kvBuilder = KeyValuePair.newBuilder();
      kvBuilder.setKey(ByteString.copyFrom(key));
      kvBuilder.setValue(ByteString.copyFrom(value));
      
      SetRequest.Builder builder = SetRequest.newBuilder();
      builder.setKv(kvBuilder.build());
      
      try {
        SetResponse response = rpcClient.getStub().set(null, builder.build());
        if(response != null && response.getSuccess())
          return true;
      } catch(ServiceException e) {
        LOG.error("set failed");
      }
      return false;
    }

    @Override
    public boolean set(String key, String value) {
      Preconditions.checkNotNull(key);
      Preconditions.checkNotNull(value);
      
      return set(key.getBytes(), value.getBytes());
    }

    @Override
    public byte[] get(byte[] key) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String get(String key) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public boolean delete(byte[] key) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public boolean delete(String key) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public List<KeyValue> list(byte[] pattern) {
      ListRequest.Builder builder = ListRequest.newBuilder();
      if(pattern != null) {
        builder.setPattern(ByteString.copyFrom(pattern));
      }
      List<KeyValue> result = new ArrayList<>();
      
      try {
        ListResponse response = rpcClient.getStub().list(null, builder.build());
        if(response != null && response.getSuccess()) {
          for(KeyValuePair kvp: response.getResultsList()) {
            KeyValue kv = KeyValue.copyFrom(kvp);
            result.add(kv);
          }
        }
      } catch(ServiceException e) {
        LOG.error("list failed");
      }
      return result;
    }

    @Override
    public List<KeyValue> list(String pattern) {
      if(pattern != null)
        return list(pattern.getBytes());
      else
        return list("".getBytes());
    }
    
 
  }
}

