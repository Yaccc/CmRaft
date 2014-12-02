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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.log.LogManager;
import com.chicm.cmraft.rpc.RpcServer;

/**
 * This class represents a Raft node in a cluster.
 * Rule of Raft servers:
 * All Servers:
 * If commitIndex > lastApplied: increment lastApplied, apply
 * log[lastApplied] to state machine 
 * If RPC request or response contains term T > currentTerm:
 * set currentTerm = T, convert to follower 
 * 
 * Followers :
 * Respond to RPCs from candidates and leaders
 * If election timeout elapses without receiving AppendEntries
 * RPC from current leader or granting vote to candidate:
 * convert to candidate

 * Candidates :
 * On conversion to candidate, start election:
 * Increment currentTerm
 * Vote for self
 * Reset election timer
 * Send RequestVote RPCs to all other servers
 * If votes received from majority of servers: become leader
 * If AppendEntries RPC received from new leader: convert to
 * follower
 * If election timeout elapses: start new election

 * Leaders:
 * Upon election: send initial empty AppendEntries RPCs
 * (heartbeat) to each server; repeat during idle periods to
 * prevent election timeouts 
 * If command received from client: append entry to local log,
 * respond after entry applied to state machine
 * If last log index >= nextIndex for a follower: send
 * AppendEntries RPC with log entries starting at nextIndex
 * If successful: update nextIndex and matchIndex for
 * follower 
 * If AppendEntries fails because of log inconsistency:
 * decrement nextIndex and retry 
 * If there exists an N such that N > commitIndex, a majority
 * of matchIndex[i] >= N, and log[N].term == currentTerm:
 * set commitIndex = N 
 * 
 * Persistent state on all servers:
 * (Updated on stable storage before responding to RPCs)
 * currentTerm latest term server has seen (initialized to 0
 * on first boot, increases monotonically)
 * votedFor candidateId that received vote in current
 * term (or null if none)
 * log[] log entries; each entry contains command
 * for state machine, and term when entry
 * was received by leader (first index is 1)
 * 
 * Volatile state on all servers:
 * commitIndex index of highest log entry known to be
 * committed (initialized to 0, increases
 * monotonically)
 * lastApplied index of highest log entry applied to state
 * machine (initialized to 0, increases
 * monotonically)
 * 
 * Volatile state on leaders:
 * (Reinitialized after election)
 * nextIndex[] for each server, index of the next log entry
 * to send to that server (initialized to leader
 * last log index + 1)
 * matchIndex[] for each server, index of highest log entry
 * known to be replicated on server
 * (initialized to 0, increases monotonically)
 * 
 * @author chicm
 *
 */
public class RaftNode {
  static final Log LOG = LogFactory.getLog(RaftNode.class);
  private Configuration conf = null;
  private StateMachine fsm = null;
  private RpcServer rpcServer = null;
  private RpcClientManager rpcClientManager = null;
  private TimeoutWorker timeoutWorker = new TimeoutWorker();
  private RaftTimeoutListener timeoutListener = new TimeoutHandler();
  private RaftStateChangeListener stateChangeListener = new RaftStateChangeListenerImpl();
  private RaftRpcService raftService = null;
  private LogManager logManager= null;

  private ServerInfo serverInfo = null;
  private ServerInfo currentLeader = null;
  
  //persistent state for all servers
  //need to reset votedFor to null every time increasing currentTerm.
  private volatile ServerInfo votedFor = null;
  private volatile AtomicLong currentTerm = new AtomicLong(0);
  private volatile AtomicInteger voteCounter = new AtomicInteger(0);

  public RaftNode(Configuration conf) {
    this.conf = conf;
    serverInfo = ServerInfo.parseFromString(conf.getString("raft.server.local"));
    raftService = RaftRpcService.create(this);
    logManager= new LogManager(this);
    fsm = new StateMachine(stateChangeListener);
    rpcServer = new RpcServer(conf, raftService);
    rpcClientManager = new RpcClientManager(conf, this);
    rpcServer.startRpcServer();
    timeoutWorker.start(getName() + "-" + fsm.getState(), getElectionTimeout(), timeoutListener);
    
    LOG.info(String.format("%s initialized", getName()));
  }
  
  public long getCurrentTerm() {
    return currentTerm.get();
  }
  
  public ServerInfo getServerInfo() {
    return serverInfo;
  }
  
  private void setCurrentLeader(ServerInfo newLeader) {
    this.currentLeader = newLeader;
  }
  public ServerInfo getCurrentLeader() {
    return currentLeader;
  }
  
  public LogManager getLogManager() {
    return this.logManager;
  }
  
  public RaftRpcService getRaftService() {
    return raftService;
  }
  
  public RpcClientManager getRpcClientManager() {
    return this.rpcClientManager;
  }
  
  private int getElectionTimeout() {
    int confTimeout = conf.getInt("raft.election.timeout");
    int r = RandomUtils.nextInt(confTimeout);
    return confTimeout + r;    
  }
  
  public String getName() {
    if(rpcClientManager == null)
      return "";
    return String.format("RaftNode[%s:%d]",  rpcClientManager.getThisServer().getHost(),
      rpcClientManager.getThisServer().getPort());
  }
  
  public int getTotalServerNumbers () {
    return rpcClientManager.getOtherServers().size() + 1;
  }
  
  public void resetTimer() {
    if(timeoutWorker != null) { 
      timeoutWorker.reset();
    } else {
      LOG.error(getName() + ":resetTimer ERROR: timeoutWork==null");
    }
  }
  
  public boolean isLeader() {
    return fsm.getState().equals(State.LEADER);
  }
  
  public State getState() {
    return fsm.getState();
  }
  
  //For testing only
  public void kill() {
    timeoutWorker.stop();
    timeoutWorker = null;
  }
  
  public void increaseTerm() {
    this.currentTerm.getAndIncrement();
    this.votedFor = null;
    this.voteCounter.set(0);
  }
   
  public void checkRpcTerm(ServerInfo leader, long term) {
    if(term > getCurrentTerm()) {
      discoverHigherTerm(leader, term);
    }
  }
  
  public synchronized void discoverHigherTerm(ServerInfo remoteServer, long newTerm) {
    if(newTerm <= getCurrentTerm())
      return;
    
    LOG.info(String.format("%s discover higher term[%s](%d), local term:%d", 
      getName(), remoteServer, newTerm, getCurrentTerm()));
    
    currentTerm.set(newTerm);
    votedFor = null;
    voteCounter.set(0);
    fsm.discoverHigherTerm();
  }
  
  public synchronized void discoverLeader(ServerInfo leader, long term) {
    if(term < getCurrentTerm()) {
      return;
    }
    LOG.info(getName() + " discover leader, leader term:" + leader + ":" + term + ", local term:" + getCurrentTerm());
    setCurrentLeader(leader);
    if(term > getCurrentTerm()) {
      currentTerm.set(term);
      votedFor = null;
      voteCounter.set(0);
    }
    fsm.discoverLeader();
  }
  
  //This method need to be thread safe, otherwise it should be synchronized.
  // currently it is thread safe, be careful to modify it.
  public void voteReceived(ServerInfo server, long term) {
    if(fsm.getState() != State.CANDIDATE)
      return;
    voteCounter.incrementAndGet();
    
    LOG.info(getName() + "vote received from: " + server + " votecounter:" + voteCounter.get());
    
    if(voteCounter.get() > getTotalServerNumbers()/2) {
      LOG.info(String.format("%s: RECEIVED MAJORITY VOTES(%d/%d), term(%d)", 
        getName(), voteCounter.get(), getTotalServerNumbers(), getCurrentTerm()));
      voteCounter.set(0);
      fsm.voteReceived();
    }
  }
  
  private void voteMySelf() {
    LOG.info(getName() + ": VOTE MYSELF!**");
    if( voteRequest(getServerInfo(), getCurrentTerm(), 
      logManager.getCurrentIndex(), logManager.getCurrentTerm())) {
      voteReceived(getServerInfo(), getCurrentTerm());
    } else {
      LOG.error("voteMySelf failed!");
    }
  }
  
  public synchronized boolean voteRequest(ServerInfo candidate, long term, long lastLogIndex, long lastLogTerm) {
    boolean ret = false;
    if(term < getCurrentTerm())
      return ret;
    checkRpcTerm(candidate, term);
    
    if(isLeader() && term == getCurrentTerm()) {
      return false;
    }
    
    if(candidate.equals(votedFor)) {
      //for debug:
      Exception e = new Exception("already voted" + votedFor);
      e.printStackTrace(System.out);
    }
    
    if (votedFor == null || votedFor.equals(candidate)) {
      votedFor = candidate;
      ret = true;
      LOG.info(getName() + "voted for: " + candidate.getHost() + ":" + candidate.getPort());
    } else {
      LOG.info(getName() + "vote request rejected: " + candidate.getHost() + ":" + candidate.getPort());
    }
    
    return ret;
  }
  
  private void restartTimer() {
    if(timeoutWorker == null) {
      LOG.error("restartTimer ERROR, timeoutWorker == null");
      return;
    }
    
    int timeout = -1;
    switch(getState()) {
      case FOLLOWER:
      case CANDIDATE:
        timeout = getElectionTimeout();
        break;
      case LEADER:
        timeout = conf.getInt("raft.heartbeat.interval");
    }
    String threadName = getName() + "-" + fsm.getState() + "-timeoutWorker";
    timeoutWorker.stop();
    timeoutWorker.start(threadName, timeout, timeoutListener);
  }
  
  public void testHearBeat() {
    rpcClientManager.beatHeart(getCurrentTerm(), getServerInfo(), logManager.getCommitIndex(),
      logManager.getCurrentIndex(), logManager.getCurrentTerm());
  }
  
  private class RaftStateChangeListenerImpl implements RaftStateChangeListener {
    @Override
    public void stateChange(State oldState, State newState) {
      LOG.info(String.format("%s state change: %s=>%s", getName(), oldState, newState));
      
      //restart timer when state change.
      restartTimer();
      
      switch(newState) {
        case FOLLOWER:
          break;
        case CANDIDATE:
          //start up a new election term when becoming candidate
          //increaseTerm(); //Term will be increased every timeout, so we do not need to increase term here
          break;
        case LEADER:
          setCurrentLeader(getServerInfo());
          //send heartbeat right away after becoming leader, then send out heartbeat every timeout 
          rpcClientManager.beatHeart(getCurrentTerm(), getServerInfo(), logManager.getCommitIndex(),
            logManager.getCurrentIndex(), logManager.getCurrentTerm());
          break;
      }
    }
  }
  
  private class TimeoutHandler implements RaftTimeoutListener, Runnable {
    @Override
    public void timeout() {
      Thread t = new Thread(new TimeoutHandler());
      t.setName(getName() + "-TimeoutHandler"); 
      t.setDaemon(true);
      t.start();
    }
    
    @Override
    public void run() {
      LOG.info(getName() + " state:" + fsm.getState() + " timeout!!");
      //perform state change
      fsm.electionTimeout();
      
      //do initialization after state change
      if(fsm.getState() == State.LEADER) {
        //leader send heartbeat to all servers every timeout
        rpcClientManager.beatHeart(getCurrentTerm(), getServerInfo(), logManager.getCommitIndex(),
          logManager.getCurrentIndex(), logManager.getCurrentTerm());
        
      } else if(fsm.getState() == State.CANDIDATE) {
        //every timeout period, candidates start up new election
        increaseTerm();
        voteMySelf();
        rpcClientManager.collectVote(currentTerm.get(), logManager.getCurrentIndex(), logManager.getCurrentTerm());
      } else if( fsm.getState() == State.FOLLOWER ) {
        
      }
    }
  }
}
