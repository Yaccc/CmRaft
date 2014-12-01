package com.chicm.cmraft.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.log.LogEntry;
import com.chicm.cmraft.protobuf.generated.RaftProtos.AppendEntriesResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteResponse;
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
    initServerList(this.conf);
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
  
  public RpcClient getRpcClient(ServerInfo server) {
    return rpcClients.get(server);
  }
  
  public void beatHeart(long term, ServerInfo leaderId, long leaderCommit,
      long prevLogIndex, long prevLogTerm) {
    appendEntries(term, leaderId, leaderCommit, prevLogIndex, prevLogTerm, null);
    
  }
  
  public void appendEntries(long term, ServerInfo leaderId, long leaderCommit,
      long prevLogIndex, long prevLogTerm, LogEntry[] entries) {
    int nServers = getOtherServers().size();
    ExecutorService executor = Executors.newFixedThreadPool(nServers,
      new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName(getRaftNode().getName() + "-AsyncRpcCaller" + (byte)System.currentTimeMillis());
          return t;
        }
    });
    
    for(ServerInfo server: getOtherServers()) {
      RpcClient client = rpcClients.get(server);
      LOG.info(getRaftNode().getName() + ": SENDING appendEntries Request TO: " + server);
      Thread t = new Thread(new AsynchronousAppendEntriesWorker(getRaftNode(), client, thisServer, term,
        leaderCommit, prevLogIndex, prevLogTerm, entries));
      t.setDaemon(true);
      executor.execute(t);
    }
  }
  
  public void collectVote(long term, long lastLogIndex, long lastLogTerm) {
    int nServers = getOtherServers().size();
    ExecutorService executor = Executors.newFixedThreadPool(nServers,
      new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName(getRaftNode().getName() + "-AsyncRpcCaller" + (byte)System.currentTimeMillis());
          return t;
        }
    });
    
    for(ServerInfo server: getOtherServers()) {
      RpcClient client = rpcClients.get(server);
      LOG.info(getRaftNode().getName() + ": SENDING COLLECTVOTE Request TO: " + server);
      Thread t = new Thread(new AsynchronousVoteWorker(getRaftNode(), client, thisServer, term,
        lastLogIndex, lastLogTerm));
      t.setDaemon(true);
      executor.execute(t);
    }
  }
  
  class AsynchronousVoteWorker implements Runnable {
    private ServerInfo candidate;
    private long term;
    private long lastLogIndex;
    private long lastLogTerm;
    private RaftNode node;
    private RpcClient client;
    
    public AsynchronousVoteWorker(RaftNode node, RpcClient client, ServerInfo thisServer, long term,
        long lastLogIndex, long lastLogTerm) {
      this.client = client;
      this.node = node;
      this.candidate = thisServer;
      this.term = term;
      this.lastLogIndex = lastLogIndex;
      this.lastLogTerm = lastLogTerm;
    }
    
    @Override
    public void run () {
      CollectVoteResponse response = null;
      try {
        response = client.collectVote(candidate, term, lastLogIndex, lastLogTerm);
        if(response != null && response.getGranted()) {
          node.voteReceived(ServerInfo.parseFromServerId(response.getFromHost()), response.getTerm());
        } else if( response == null) {
          LOG.error("RPC failed, response == null");
        } else if(response.getGranted() == false) {
          LOG.info(node.getName() + "VOTE REJECTED BY " + response.getFromHost().getHostName()
            + ":" + response.getFromHost().getPort());
        }
      } catch(ServiceException e) {
        try {
        LOG.error("RPC: collectVote failed: from " + getRaftNode().getName() + 
          ", to: " + client.getChannel().getRemoteAddress(), e);
        } catch(Exception e2) {}
      }
    }
  }

  class AsynchronousAppendEntriesWorker implements Runnable {
    private ServerInfo leader;
    private long term;
    private long leaderCommit;
    private long prevLogIndex;
    private long prevLogTerm;
    private LogEntry[] entries;
    private RaftNode node;
    private RpcClient client;
    
    public AsynchronousAppendEntriesWorker(RaftNode node, RpcClient client, ServerInfo thisServer, long term,
        long leaderCommit, long prevLogIndex, long prevLogTerm, LogEntry[] entries) {
      this.client = client;
      this.node = node;
      this.leader = thisServer;
      this.term = term;
      this.leaderCommit = leaderCommit;
      this.prevLogIndex = prevLogIndex;
      this.prevLogTerm = prevLogTerm;
      this.entries = entries;
    }
    
    @Override
    public void run () {
      try {
        AppendEntriesResponse response = client.appendEntries(term, leader, leaderCommit, prevLogIndex, prevLogTerm, entries);
        //todo: set commit status for entries
      } catch(ServiceException e) {
        try {
        LOG.error("RPC: collectVote failed: from " + getRaftNode().getName() + 
          ", to: " + client.getChannel().getRemoteAddress(), e);
        } catch(Exception e2) {}
      }
    }
  }
}
