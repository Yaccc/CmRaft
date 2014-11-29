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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
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
  private NodeTimeoutThread timeoutThread = new NodeTimeoutThread();
  private RaftEventListener listener = new RaftEventListenerImpl();
  private RaftStateChangeListener stateChangeListener = new RaftStateChangeListenerImpl();
  private RaftRpcService raftService = null;
  private BlockingQueue<StateEvent> eventQueue = null;
  private ServerInfo serverInfo = null;
  private volatile ServerInfo votedFor = null;
  
  private volatile AtomicLong currentTerm = new AtomicLong(0);

  public RaftNode(Configuration conf) {
    this.conf = conf;
    serverInfo = ServerInfo.parseFromString(conf.getString("raft.server.local"));
    raftService = RaftRpcService.create(this);
    fsm = new StateMachine(stateChangeListener);
    rpcServer = new RpcServer(conf, raftService);
    rpcClientManager = new RpcClientManager(conf, this);
    rpcServer.startRpcServer();
    timeoutThread.start(getElectionTimeout(), listener);
    eventQueue = new LinkedBlockingQueue<StateEvent>();
    startEventWorker();
    
    LOG.info(String.format("%s initialized", getName()));
  }
  
  public long getCurrentTerm() {
    return currentTerm.get();
  }
  
  public ServerInfo getServerInfo() {
    return serverInfo;
  }
  
  public RaftRpcService getRaftService() {
    return raftService;
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
   
  public void checkRpcTerm(long term) {
    if(term > getCurrentTerm()) {
      StateEvent event = new StateEvent(StateEventType.DISCOVERD_HIGHER_TERM, null, term);
      addEvent(event);
    }
  }
  
  public synchronized boolean voteRequest(ServerInfo candidate, long term, long lastLogIndex, long lastLogTerm) {
    boolean ret = false;
    if(term < getCurrentTerm())
      return ret;
    if (votedFor == null || votedFor.equals(candidate)) {
      votedFor = candidate;
      ret = true;
      LOG.info(getName() + "voted for: " + candidate.getHost() + ":" + candidate.getPort());
    } else {
      LOG.info(getName() + "vote request rejected: " + candidate.getHost() + ":" + candidate.getPort());
    }
    checkRpcTerm(term);
    return ret;
  }
  
  public synchronized void hearBeatRequest (ServerInfo leader) {
    
  }
  
  public void addEvent(StateEvent event) {
    try {
      eventQueue.put(event);
    } catch(InterruptedException e) {
      LOG.error("addEvent exception", e);
    }
  }
  
  public void testHearBeat() {
    rpcClientManager.beatHeart();
  }
  
  //public 
  
  private void becomeFollower(State oldState) {
    timeoutThread.stop();
    timeoutThread.start(getElectionTimeout(), listener);
  }
  
  private void becomeCandidate(State oldState) {
    timeoutThread.stop();
    timeoutThread.start(getElectionTimeout(), listener);
    
  }
  
  private void becomeLeader(State oldState) {
    timeoutThread.stop();
    timeoutThread.start(conf.getInt("raft.heartbeat.interval"), listener);
  }
  
  private class RaftStateChangeListenerImpl implements RaftStateChangeListener {
    @Override
    public void stateChange(State oldState, State newState) {
      LOG.info(String.format("%s state change: %s=>%s", getName(), oldState, newState));
      switch(newState) {
        case FOLLOWER:
          becomeFollower(oldState);
          break;
        case CANDIDATE:
          becomeCandidate(oldState);
          break;
        case LEADER:
          becomeLeader(oldState);
          break;
      }
    }
  }
  
  private class RaftEventListenerImpl implements RaftEventListener {
    @Override 
    public void timeout() { 
      addEvent(new StateEvent(StateEventType.ELECTION_TIMEOUT, null, getCurrentTerm()));
    }
    @Override
    public void voteReceived() {
      
    }
    @Override
    public void voteReceived(ServerInfo server, long term) {
      addEvent(new StateEvent(StateEventType.VOTE_RECEIVED_ONE, server, term) );
    }
    @Override
    public void discoverLeader() {
      
    }
    @Override
    public void discoverHigherTerm() {
      
    }
  }
  
  private void startEventWorker () {
    Thread thread = new Thread(new EventWorker());
    thread.setDaemon(true);
    thread.setName("RaftNode-EventWorker");
    
    thread.start();
  }
  
  class EventWorker implements Runnable {
    private volatile int voteCounter = 0;
    
    @Override
    public void run() {
      while(true) {
        try {
          StateEvent event = eventQueue.take();
          if(event == null) {
            continue;
          }
          switch(event.getEventType()) {
            case ELECTION_TIMEOUT:
              handleTimeout(event);
              break;
            case VOTE_RECEIVED_ONE:
              handleOneVote(event);
              break;
            case VOTE_RECEIVED_MAJORITY:
              handleVoteReceivedMajority(event);
              break;
            case DISCOVERD_LEADER:
              break;
            case DISCOVERD_HIGHER_TERM:
              break;
          }
        } catch(Exception e) {
          e.printStackTrace(System.out);
          LOG.error("exception", e);
        }
      }
    }
    
    public void handleTimeout(StateEvent e) {
      LOG.info(getName() + " state:" + fsm.getState() + " timeout!!");
      //perform state change
      fsm.electionTimeout();
      
      //do initialization after state change
      if(fsm.getState() == State.LEADER) {
        rpcClientManager.beatHeart();
      } else if(fsm.getState() == State.CANDIDATE) {
        int n = rpcClientManager.collectVote(currentTerm.get());
        LOG.info(getName() + " Collected vote:" + n);
      } else if( fsm.getState() == State.FOLLOWER ) {
        
      }
    }
    
    public void handleOneVote(StateEvent e) {
      if(fsm.getState() != State.CANDIDATE)
        return;
      voteCounter ++;
      
      if(voteCounter > getTotalServerNumbers()/2) {
        voteCounter = 0;
        addEvent(new StateEvent(StateEventType.VOTE_RECEIVED_MAJORITY, null, getCurrentTerm()) );
      }
    }
    
    public void handleVoteReceivedMajority(StateEvent e) {
      fsm.voteReceived();
    }
  }

}
