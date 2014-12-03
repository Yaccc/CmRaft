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

package com.chicm.cmraft.log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.core.RaftNode;
import com.chicm.cmraft.core.State;
import com.chicm.cmraft.util.BlockingHashMap;

public class LogManager {
  static final Log LOG = LogFactory.getLog(LogManager.class);
  
  private final static int DEFAULT_COMMIT_TIMEOUT = 5000;
  private Configuration conf;
  private SortedMap<Long, LogEntry> entries = new TreeMap<>();
  private final static long INITIAL_TERM = 0;
  private RaftNode node;
  private final AtomicLong commitIndex = new AtomicLong(0);

  /**lastApplied initialized as 0, and the first time increase it to 1,
     so the first index is 1. */
  private final AtomicLong lastApplied = new AtomicLong(0);
  
  /** followerIndexes contains:
   * nextIndex[] for each server, index of the next log entry
   *  to send to that server (initialized to leaderlast log index + 1)
   * matchIndex[] for each server, index of highest log entry
   *  known to be replicated on server (initialized to 0, increases monotonically)
   */
  private Map<ServerInfo, FollowerIndexes> followerIndexes = new ConcurrentHashMap<>();
  
  /** AppendEntries RPC response counter, to convert entry from applied to commit 
   *  once get more than half success response */
  private ResponseBag<Long> responseBag = new ResponseBag<Long>();
  
  private BlockingHashMap<Long, Boolean> rpcResults = new BlockingHashMap<>();
  
  private int nTotalServers = 0;
  
  private ServerInfo thisServer;
  
  public LogManager(RaftNode node, Configuration conf) {
    this.node = node;
    this.conf = conf;
    thisServer = ServerInfo.parseFromString(conf.getString("raft.server.local"));
  }
  
  public String getServerName() {
    return thisServer.toString();
  }
  
  private void leaderInit() {
    LOG.info(getServerName() + ": LEADER INIT");
    
    for(ServerInfo remoteServer: ServerInfo.getRemoteServersFromConfiguration(conf)) {
      FollowerIndexes fIndexes = new FollowerIndexes(getLastApplied() +1, 0);
      followerIndexes.put(remoteServer, fIndexes);
    }
    
    nTotalServers = followerIndexes.size();
  }
  
  private void cleanupLeaderWorker() {
    
  }
  
  public void stateChange(State oldState, State newState) {
    LOG.info(getServerName() + ": STATE CHANGE");
    if(oldState == State.LEADER && newState != State.LEADER) {
      cleanupLeaderWorker();
    } else if(newState == State.LEADER && oldState != State.LEADER) {
      leaderInit();
    }
  }
  
  public long getFollowerMatchIndex(ServerInfo follower) {
    return followerIndexes.get(follower).getMatchIndex();
  }
  
  public long getFollowerNextIndex(ServerInfo follower) {
    return followerIndexes.get(follower).getNextIndex();
  }
  
  public long getLogTerm(long index) {
    if(entries.get(index) == null)
      return 0;
    return entries.get(index).getTerm();
  }
  
  public List<LogEntry> getLogEntries(long startIndex, long endIndex) {
    if(startIndex < 1 || endIndex > getLastApplied())
      return null;
    List<LogEntry> result = new ArrayList<LogEntry>();
    for(long key = startIndex; key <= endIndex; key++) {
      result.add(entries.get(key)); 
    }
    return result;
  }
  
  /**
   * @return the commitIndex
   */
  public long getCommitIndex() {
    return commitIndex.get();
  }
  /**
   * @param commitIndex the commitIndex to set
   */
  public void setCommitIndex(long commitIndex) {
    this.commitIndex.set(commitIndex);;
  }
  /**
   * @return the lastApplied
   */
  public long getLastApplied() {
    return lastApplied.get();
  }
  
  public long getLastLogTerm() {
    if(entries == null || entries.size() < 1)
      return INITIAL_TERM;
    return entries.get(getLastApplied()).getTerm();
  }
  /**
   * @param lastApplied the lastApplied to set
   */
  public void setLastApplied(long lastApplied) {
    this.lastApplied.set(lastApplied);;
  }
  
  // for followers
  public boolean appendEntries(long term, ServerInfo leaderId, long leaderCommit,
      long prevLogIndex, long prevLogTerm, List<LogEntry> leaderEntries) {
    LOG.debug(getServerName() + "follower appending entry...");
    if(leaderEntries == null || leaderEntries.size() < 1)
      return false;
    if(prevLogIndex > 0) {
      if(!this.entries.containsKey(prevLogIndex)) {
        return false;
      }
      if(this.entries.get(prevLogIndex).getTerm() != prevLogTerm) {
        return false;
      }
    }
    //append entries
    //need to assure that the entries are sorted before hand.
    for(LogEntry entry: leaderEntries) {
      entries.put(entry.getIndex(), entry);
      if(entry.getIndex() > getLastApplied()) {
        setLastApplied(entry.getIndex());
      }
    } 
    
    if(leaderCommit > getCommitIndex()) {
      setCommitIndex(Math.min(getLastApplied(), leaderCommit));
    }
    LOG.debug(getServerName() + "follower appending entry... done");
    return true;
  }
  // for leaders
  public void onAppendEntriesResponse(ServerInfo follower, long followerTerm, boolean success, long followerLastApplied) {
    LOG.debug(getServerName() + ": onAppendEntriesResponse");
    updateFollowerMatchIndexes(follower, followerLastApplied);
    if(!success) {
      return;
    }
    responseBag.add(followerLastApplied, 1);
    checkAndCommit(followerLastApplied);
  }
  
  private void checkAndCommit(long index) {
    LOG.debug(getServerName() + ": checkAndCommit");
    if(index <= getCommitIndex()) {
      return;
    }
    
    if(responseBag.get(index) > nTotalServers/2) {
      LOG.info(getServerName() + ": committed");
      setCommitIndex(index);
      rpcResults.put(index, true);
    }
  }
  
  private void updateFollowerMatchIndexes(ServerInfo follower, long lastApplied) {
    if(followerIndexes.get(follower) == null) {
      LOG.error("FOLLOWER INDEXES MAP SHOULD BE INITIALIZED BEFORE HERE.");
    }
    
    if(followerIndexes.get(follower).getMatchIndex() < lastApplied) {
      followerIndexes.get(follower).setMatchIndex(lastApplied);
    }
  }
  
  public boolean set(byte[] key, byte[] value) {
    LOG.debug(getServerName() + ": set request received");
    //lastApplied initialized as 0, and the first time increase it to 1,
    //so the first index is 1
    LogEntry entry = new LogEntry(lastApplied.incrementAndGet(), node.getCurrentTerm(), key, value, LogMutationType.SET);
    entries.put(entry.getIndex(), entry);
    
    //making rpc call to followers
    node.getRpcClientManager().appendEntries(this, getLastApplied());
    //waiting for results
    boolean committed = false;
    committed = rpcResults.take(getLastApplied(), DEFAULT_COMMIT_TIMEOUT, 1);
    LOG.debug(getServerName() + ": set committed, sending response");
    return committed;
  }
  
  public void delete(byte[] key) {
    //lastApplied initialized as 0, and the first time increase it to 1,
    //so the first index is 1
    LogEntry entry = new LogEntry(lastApplied.incrementAndGet(), node.getCurrentTerm(), key, null, LogMutationType.DELETE);
    entries.put(entry.getIndex(), entry);
  }
  
  public Collection<LogEntry> list(byte[] pattern) {
    return entries.values();
  }
  
  class FollowerIndexes {
    /** for each server, index of the next log entry
        to send to that server (initialized to leader
        last log index + 1) */
    private long nextIndex;
    
    /** for each server, index of highest log entry
        known to be replicated on server
        (initialized to 0, increases monotonically) */
    private long matchIndex;
    
    public FollowerIndexes(long next, long match) {
      this.nextIndex = next;
      this.matchIndex = match;
    }
    /**
     * @return the nextIndex
     */
    public long getNextIndex() {
      return nextIndex;
    }
    /**
     * @param nextIndex the nextIndex to set
     */
    public void setNextIndex(long nextIndex) {
      this.nextIndex = nextIndex;
    }
    /**
     * @return the matchIndex
     */
    public long getMatchIndex() {
      return matchIndex;
    }
    /**
     * @param matchIndex the matchIndex to set
     */
    public void setMatchIndex(long matchIndex) {
      this.matchIndex = matchIndex;
    }
  }
}
