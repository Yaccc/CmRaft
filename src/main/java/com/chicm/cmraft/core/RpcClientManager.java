package com.chicm.cmraft.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.rpc.RpcClient;
import com.google.protobuf.ServiceException;

public class RpcClientManager {
  static final Log LOG = LogFactory.getLog(RpcClientManager.class);
  private Configuration conf;
  private Map<ServerInfo, RpcClient> rpcClients;
  private ServerInfo thisServer;
  
  public RpcClientManager(Configuration conf) {
    this.conf = conf;
    initServerList(conf);
  }
  
  private void initServerList(Configuration conf) {
    rpcClients = new ConcurrentHashMap<>();
    
    thisServer = ServerInfo.parseFromString(conf.getString("raft.server.local"));
    for (String key: conf.getKeys("raft.server.remote")) {
      ServerInfo server = ServerInfo.parseFromString(conf.getString(key));
      RpcClient client = new RpcClient(conf, server.getHost(), server.getPort());
      rpcClients.put(server, client);
    }
  }
  
  public Set<ServerInfo> getOtherServers() {
    return rpcClients.keySet();
  }
  
  public Set<ServerInfo> getAllServers() {
    HashSet<ServerInfo> s = new HashSet<>();
    s.addAll(getOtherServers());
    s.add(thisServer);
    return s;
  }
  
  public void beatHeart() {
    for(ServerInfo server: getOtherServers()) {
      RpcClient client = rpcClients.get(server);
      try {
        client.heartBeat(thisServer);
      } catch(ServiceException e) {
        LOG.error("RPC: beatHeart failed:" + server.getHost() + ":" + server.getPort(), e);
      }
    }
  }
  
}
