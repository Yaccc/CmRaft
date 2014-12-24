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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.core.ClusterMemberManager;
import com.chicm.cmraft.protobuf.generated.RaftProtos.DeleteRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.DeleteResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.GetRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.GetResponse;
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
  private ServerInfo remoteServer;
  
  private ConnectionManager(Configuration conf, Set<ServerInfo> servers) {
    Preconditions.checkNotNull(servers);
    Preconditions.checkArgument(!servers.isEmpty());
    boolean connected = false;
    LOG.info("Raft servers:" + servers);
    for(ServerInfo server: servers) {
      LOG.info("Trying connect to:" + server);
      try {
        RpcClient rpc = new RpcClient(conf, server);
        if(rpc.connect()) {
          rpcClient = rpc;
          remoteServer = server;
          connected = true;
          break;
        }
      } catch(Exception e) {
        LOG.error("Failed connecting to:" + server, e);
      }
    }
    
    if(!connected) {
      String msg = "Could not connect to all raft servers";
      LOG.error(msg);
      RuntimeException e = new RuntimeException(msg);
      throw e;
    } else {
      LOG.info("Connected to: " + remoteServer);
    }
    
    connected = false;
    ServerInfo leader = lookupLeader();
    LOG.info("lookupLeader:" + leader);
    if(leader != null && !leader.equals(remoteServer)) {
      LOG.info("closing connection to " + remoteServer + ", connecting to " + leader);
      try {
        rpcClient.close();
        RpcClient rpcLeader = new RpcClient(conf, leader);
        if(rpcLeader.connect()) {
          rpcClient = rpcLeader;
          remoteServer = leader;
          connected = true;
        }
      } catch(Exception e) {
        LOG.error("Failed connecting to:" + leader, e);
      }
    } else if(leader != null && leader.equals(remoteServer)) {
      LOG.info("This connected server is leader:" + leader);
      connected = true;
    } else if(leader == null) {
      LOG.error("Could not get Leader info");
    }
    
    if(!connected) {
      RuntimeException e = new RuntimeException("Could not connect to leader:" + leader);
      throw e;
    }
    
    userConnection = new ConnectionImpl();
    kvs = new KeyValueStoreImpl();
  }
  
  public ServerInfo getRemoteServer() {
    return remoteServer;
  }
  
  public static Connection getConnection() {
    return getConnection(CmRaftConfiguration.create());
  }
  
  public static Connection getConnection(Configuration conf) {
    ConnectionManager mgr = new ConnectionManager(conf, ClusterMemberManager.getRaftServers(conf));
    return mgr.userConnection;
  }
  
  public void close() {
    rpcClient.close();
  }
  
  private ServerInfo lookupLeader() {
    LookupLeaderRequest.Builder builder = LookupLeaderRequest.newBuilder();
    try {
      LookupLeaderResponse response = rpcClient.getStub().lookupLeader(null, builder.build());
      if(response.getSuccess()) {
        return ServerInfo.copyFrom(response.getLeader());
      }
    } catch(Exception e) {
      LOG.error("lookupLeader failed:" + e.getMessage(), e);
    }
    LOG.error("lookupLeader returned null");
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
      Preconditions.checkNotNull(key);
      Preconditions.checkArgument(key.length > 0);
      
      Preconditions.checkNotNull(value);
      Preconditions.checkArgument(value.length > 0);
      
      KeyValuePair.Builder kvBuilder = KeyValuePair.newBuilder();
      kvBuilder.setKey(ByteString.copyFrom(key));
      kvBuilder.setValue(ByteString.copyFrom(value));
      
      SetRequest.Builder builder = SetRequest.newBuilder();
      builder.setKv(kvBuilder.build());
      
      try {
        SetResponse response = rpcClient.getStub().set(null, builder.build());
        if(response != null && response.getSuccess())
          return true;
      } catch(Exception e) {
        LOG.error("set failed:" + e.getMessage());
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
      Preconditions.checkNotNull(key);
      GetRequest.Builder builder = GetRequest.newBuilder();
      builder.setKey(ByteString.copyFrom(key));
      
      try {
        GetResponse response = rpcClient.getStub().get(null, builder.build());
        if(response.getSuccess()) {
          return response.getValue().toByteArray();
        } else {
          return null;
        }
      } catch (ServiceException e) {
        LOG.error("Get exception,", e);
      } catch (Exception e) {
        LOG.error("Get exception,", e);
      }
      return null;
    }

    @Override
    public String get(String key) {
      Preconditions.checkNotNull(key);
      Preconditions.checkArgument(!key.isEmpty());
      byte[] result = get(key.getBytes());
      
      return result==null ? null : new String(result) ;
    }

    @Override
    public boolean delete(byte[] key) {
      Preconditions.checkNotNull(key);
      Preconditions.checkArgument(key.length > 0);
      
      DeleteRequest.Builder builder = DeleteRequest.newBuilder();
      builder.setKey(ByteString.copyFrom(key));
      
      try {
        DeleteResponse response = rpcClient.getStub().delete(null, builder.build());
        return response.getSuccess();
      } catch (ServiceException e) {
        LOG.error("Delete exception,", e);
      } catch (Exception e) {
        LOG.error("Delete exception,", e);
      }
      return false;
    }

    @Override
    public boolean delete(String key) {
      return delete(key.getBytes());
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
      } catch(Exception e) {
        LOG.error("list failed:" + e.getMessage());
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

