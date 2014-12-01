package com.chicm.cmraft;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.protobuf.generated.RaftProtos.KeyValue;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ListRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ListResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.LookupLeaderRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.LookupLeaderResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.SetRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.SetResponse;
import com.chicm.cmraft.rpc.RpcClient;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

public class ConnectionManager {
  static final Log LOG = LogFactory.getLog(ConnectionManager.class);
  private RpcClient rpcClient;
  private UserConnectionImpl userConnection;
  
  private ConnectionManager(Configuration conf, ServerInfo server) {
    rpcClient = new RpcClient(conf, server.getHost(), server.getPort());
    userConnection = new UserConnectionImpl();
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
        return ServerInfo.parseFromServerId(response.getLeader());
      }
    } catch(ServiceException e) {
      LOG.error("lookupLeader failed", e);
    }
    return null;
  }
  
  private class UserConnectionImpl implements Connection {

    @Override
    public boolean set(byte[] key, byte[] value) {
      SetRequest.Builder builder = SetRequest.newBuilder();
      builder.setKey(ByteString.copyFrom(key));
      builder.setValue(ByteString.copyFrom(value));
      
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
      if(key == null || value == null) {
        throw new IllegalArgumentException("key or value is null");
      }
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
    public Result list(byte[] pattern) {
      ListRequest.Builder builder = ListRequest.newBuilder();
      if(pattern != null) {
        builder.setPattern(ByteString.copyFrom(pattern));
      }
      
      try {
        ListResponse response = rpcClient.getStub().list(null, builder.build());
        if(response != null && response.getSuccess()) {
          Result rs = new Result();
          List<KeyValue> list = response.getResultsList();
          for(KeyValue kv: list) {
            rs.put(kv.getKey().toByteArray(), kv.getValue().toByteArray());
          }
          return rs;
        }
      } catch(ServiceException e) {
        LOG.error("list failed");
      }
      return null;
    }

    @Override
    public Result list(String pattern) {
      if(pattern != null)
        return list(pattern.getBytes());
      else
        return list("".getBytes());
    }
    
    @Override
    public void close() {
      rpcClient.close();
    }
  }
}

