package com.chicm.cmraft.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;
import com.chicm.cmraft.rpc.RpcClient;
import com.google.protobuf.ServiceException;

public class RpcClientManager {
  static final Log LOG = LogFactory.getLog(RpcClientManager.class);
  private Configuration conf;
  private Map<ServerInfo, RpcClient> rpcClients;
  private ServerInfo thisServer;
  private RaftNode raftNode;
  
  public RpcClientManager(Configuration conf, RaftNode node) {
    this.conf = conf;
    this.raftNode = node;
    initServerList(conf);
  }
  
  public ServerInfo getThisServer() {
    return thisServer;
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
  
  public RaftNode getRaftNode() {
    return raftNode;
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
  
  public int collectVote(long term) {
    int voted = 0;
    
    try {
      CollectVoteResponse res = collectVoteFromMyself(thisServer, term, 0, 0);
      if(res != null && res.getGranted()) {
        voted++;
        getRaftNode().addEvent(new StateEvent(StateEventType.VOTE_RECEIVED_ONE, thisServer, res.getTerm() ));
      }
    } catch(Exception exp) {
      LOG.error("collect vote from myself", exp);
    }
    
    for(ServerInfo server: getOtherServers()) {
      RpcClient client = rpcClients.get(server);
      try {
        CollectVoteResponse response = client.collectVote(thisServer, term, 0, 0);
        if(response != null && response.getGranted()) {
          voted++;
          getRaftNode().addEvent(new StateEvent(StateEventType.VOTE_RECEIVED_ONE, server, response.getTerm() ));
        }
      } catch(ServiceException e) {
        LOG.error("RPC: beatHeart failed:" + server.getHost() + ":" + server.getPort(), e);
        return voted;
      }
    }
    return voted;
  }
  
  private CollectVoteResponse collectVoteFromMyself(ServerInfo candidate, long term, long lastLogIndex,
      long lastLogTerm) throws ServiceException  {
    ServerId.Builder sbuilder = ServerId.newBuilder();
    sbuilder.setHostName(candidate.getHost());
    sbuilder.setPort(candidate.getPort());
    sbuilder.setStartCode(candidate.getStartCode());
    
    CollectVoteRequest.Builder builder = CollectVoteRequest.newBuilder();
    builder.setCandidateId(sbuilder.build());
    builder.setTerm(term);
    builder.setLastLogIndex(lastLogIndex);
    builder.setLastLogTerm(lastLogTerm);
    
    return (CollectVoteResponse) getRaftNode().getRaftService().collectVote(null, builder.build());

  }
  
}
