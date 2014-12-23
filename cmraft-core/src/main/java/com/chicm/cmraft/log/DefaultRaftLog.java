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

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.core.RaftNode;
import com.chicm.cmraft.core.State;
import com.chicm.cmraft.protobuf.generated.RaftProtos.KeyValuePair;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftLogEntry;
import com.chicm.cmraft.rpc.RpcTimeoutException;
import com.chicm.cmraft.util.BlockingHashMap;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class DefaultRaftLog implements RaftLog {
  static final Log LOG = LogFactory.getLog(DefaultRaftLog.class);
  private static final String RAFT_ROOT_DIR_KEY = "raft.root.dir";
  private static final int DEFAULT_COMMIT_TIMEOUT = 5000;
  private Configuration conf;
  private SortedMap<Long, RaftLogEntry> entries = new TreeMap<>();
  private ConcurrentHashMap<ByteString, ByteString> keyValues = new ConcurrentHashMap<>();
  private final static long INITIAL_TERM = 0;
  private RaftNode node;
  private final AtomicLong commitIndex = new AtomicLong(0);
  private final AtomicLong flushedIndex = new AtomicLong(0);
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
  
  public DefaultRaftLog(RaftNode node, Configuration conf) {
    this.node = node;
    this.conf = conf;
    thisServer = node.getServerInfo();
    
    loadPersistentData();
    startFlushWorker();
  }
  
  public String getServerName() {
    return thisServer.toString();
  }
  
  public ServerInfo getServerInfo() {
    return thisServer;
  }
  
  private void leaderInit() {
    LOG.info(getServerName() + ": LEADER INIT");
    
    for(ServerInfo remoteServer: node.getRemoteServers()) {
      FollowerIndexes fIndexes = new FollowerIndexes(getLastApplied() +1, 0);
      followerIndexes.put(remoteServer, fIndexes);
    }
    
    nTotalServers = followerIndexes.size();
  }
  
  private void cleanupLeaderWorker() {
    
  }
  
  @Override
  public void stateChange(State oldState, State newState) {
    LOG.info(getServerName() + ": STATE CHANGE");
    if(oldState == State.LEADER && newState != State.LEADER) {
      cleanupLeaderWorker();
    } else if(newState == State.LEADER && oldState != State.LEADER) {
      leaderInit();
    }
  }
  
  @Override
  public long getFollowerMatchIndex(ServerInfo follower) {
    return followerIndexes.get(follower).getMatchIndex();
  }
  
  // todo - implemnet nextIndex logic on appendEntries failure
  public long getFollowerNextIndex(ServerInfo follower) {
    return followerIndexes.get(follower).getNextIndex();
  }
  
  @Override
  public long getLogTerm(long index) {
    if(entries.get(index) == null)
      return 0;
    return entries.get(index).getTerm();
  }
  
  @Override
  public List<RaftLogEntry> getLogEntries(long startIndex, long endIndex) {
    List<RaftLogEntry> result = new ArrayList<RaftLogEntry>();
    
    if(startIndex < 1 || endIndex > getLastApplied())
      return result;
    
    for(long key = startIndex; key <= endIndex; key++) {
      result.add(entries.get(key)); 
    }
    return result;
  }
  
  /**
   * @return the commitIndex
   */
  @Override
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
  @Override
  public long getLastApplied() {
    return lastApplied.get();
  }
  
  @Override
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
  
  @Override
  public long getFlushedIndex() {
    return this.flushedIndex.get();
  }
  
  public void setFlushedIndex(long index) {
    flushedIndex.set(index);
  }
  
  // for followers
  @Override
  public boolean appendEntries(long term, ServerInfo leaderId, long leaderCommit,
      long prevLogIndex, long prevLogTerm, List<RaftLogEntry> leaderEntries) {
    LOG.debug(getServerName() + "follower appending entry...");
    
    Preconditions.checkNotNull(leaderEntries);
    
    if(term < node.getCurrentTerm()) {
      return false;
    }
    
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
    for(RaftLogEntry entry: leaderEntries) {
      entries.put(entry.getIndex(), entry);
      applyKeyValueLog(entry.getIndex());
      if(entry.getIndex() > getLastApplied()) {
        setLastApplied(entry.getIndex());
      }
    } 

    if(leaderCommit > getCommitIndex()) {
      setCommitIndex(Math.min(getLastApplied(), leaderCommit));
      //to-do: need to be done asynchronously
      flushCommitted();
    }
    
    if(leaderEntries.size() == 0) { // heart beat
      LOG.debug("heart beat");
    }
    
    LOG.debug(getServerName() + "follower appending entry... done");
    return true;
  }
  // for leaders
  @Override
  public void onAppendEntriesResponse(ServerInfo follower, long followerTerm, boolean success, 
      long followerLastApplied) {
    LOG.debug(getServerName() + ": onAppendEntriesResponse");
    updateFollowerMatchIndexes(follower, followerLastApplied);
    if(!success) {
      return;
    }
    responseBag.add(followerLastApplied, 1);
    //checkAndCommit(followerLastApplied);
    if(followerLastApplied <= getCommitIndex()) {
      return;
    }
    
    if(responseBag.get(followerLastApplied) > nTotalServers/2) {
      LOG.info(getServerName() + ": committed, index:" + followerLastApplied);
      rpcResults.put(followerLastApplied, true);
    }
  }
  
  // commit the specified log
  private void commitLog(long index) {
    setCommitIndex(index);
    replayKeyValueLogEntries(index, index);
    flushCommitted();
  //to-do: need to notify followers after commit
  }
  
  private void updateFollowerMatchIndexes(ServerInfo follower, long lastApplied) {
    if(followerIndexes.get(follower) == null) {
      LOG.error("FOLLOWER INDEXES MAP SHOULD BE INITIALIZED BEFORE HERE.");
    }
    
    if(followerIndexes.get(follower).getMatchIndex() < lastApplied) {
      followerIndexes.get(follower).setMatchIndex(lastApplied);
    }
  }
  
  @Override
  public boolean set(KeyValuePair kv) {
    LOG.debug(getServerName() + ": set request received");
    Preconditions.checkArgument(!kv.getKey().isEmpty());
    Preconditions.checkArgument(!kv.getValue().isEmpty());
    //lastApplied initialized as 0, and the first time increase it to 1,
    //so the first index is 1
    //LogEntry entry = new LogEntry(lastApplied.incrementAndGet(), node.getCurrentTerm(), key, value, LogMutationType.SET);
    RaftLogEntry.Builder builder = RaftLogEntry.newBuilder();
    builder.setKv(kv);
    builder.setIndex(lastApplied.incrementAndGet());
    builder.setTerm(node.getCurrentTerm());
    builder.setMode(RaftLogEntry.MutationMode.SET);
    RaftLogEntry entry = builder.build();
    
    entries.put(entry.getIndex(), entry);
    
    boolean committed = true;
    //making rpc call to followers
    if(!node.getNodeConnectionManager().getRemoteServers().isEmpty()) {
      committed = false;
      node.getNodeConnectionManager().appendEntries(this, entry.getIndex());
      //waiting for results
      try {
        committed = rpcResults.take(entry.getIndex(), DEFAULT_COMMIT_TIMEOUT);
      } catch(RpcTimeoutException e) {
        LOG.error(e.getMessage());
        return false;
      }
    }
    if(committed) {
      commitLog(entry.getIndex());
    }
    LOG.debug(getServerName() + ": set committed, sending response");
    return committed;
  }
  
  @Override
  public byte[] get(byte[] key) {
    Preconditions.checkNotNull(key);
    Preconditions.checkArgument(key.length > 0);
    
    ByteString result = keyValues.get(ByteString.copyFrom(key));
    if(result != null)
      return result.toByteArray();
    else
      return null;
  }
  
  @Override
  public boolean delete(byte[] key) {
    //lastApplied initialized as 0, and the first time increase it to 1,
    //so the first index is 1
    //LogEntry entry = new LogEntry(lastApplied.incrementAndGet(), node.getCurrentTerm(), key, null, LogMutationType.DELETE);
    Preconditions.checkNotNull(key);
    Preconditions.checkArgument(key.length > 0);
    
    ByteString bsKey = ByteString.copyFrom(key);
    if(!keyValues.containsKey(bsKey)) {
      return false;
    }
    
    KeyValuePair.Builder kvBuilder = KeyValuePair.newBuilder();
    kvBuilder.setKey(ByteString.copyFrom(key));
    
    RaftLogEntry.Builder builder = RaftLogEntry.newBuilder();
    builder.setKv(kvBuilder.build());
    builder.setIndex(lastApplied.incrementAndGet());
    builder.setTerm(node.getCurrentTerm());
    builder.setMode(RaftLogEntry.MutationMode.DELETE);
    RaftLogEntry entry = builder.build();
    
    entries.put(entry.getIndex(), entry);
    
    boolean committed = true;
    //making rpc call to followers
    if(!node.getNodeConnectionManager().getRemoteServers().isEmpty()) {
      committed = false;
      node.getNodeConnectionManager().appendEntries(this, entry.getIndex());
      //waiting for results
      try {
        committed = rpcResults.take(entry.getIndex(), DEFAULT_COMMIT_TIMEOUT);
      } catch(RpcTimeoutException e) {
        LOG.error(e.getMessage());
        return false;
      }
    }
    if(committed) {
      commitLog(entry.getIndex());
    }
    LOG.debug(getServerName() + ": set committed, sending response");
    return committed;
  }
  
  @Override
  public Collection<KeyValuePair> list(byte[] pattern) {
    //return entries.values();
    List<KeyValuePair> result = new ArrayList<>();
    for(ByteString k: keyValues.keySet()) {
      KeyValuePair.Builder builder = KeyValuePair.newBuilder();
      builder.setKey(k);
      builder.setValue(keyValues.get(k));
      result.add(builder.build());
    }
    return result;
  }
  
  private void replayKeyValueLogEntries(long startIndex, long endIndex) {
    Preconditions.checkArgument(startIndex <= getLastApplied() && startIndex >= 0);
    Preconditions.checkArgument(endIndex <= getLastApplied() && endIndex >= 0);
    for(long index = startIndex; index <= endIndex; index++) {
      applyKeyValueLog(index);
    }
  }
  
  private void applyKeyValueLog(long index) {
    RaftLogEntry log = entries.get(index);
    if(log.hasMode() && log.getMode() == RaftLogEntry.MutationMode.SET) {
      keyValues.put(log.getKv().getKey(), log.getKv().getValue());
    } else if(log.hasMode() && log.getMode() == RaftLogEntry.MutationMode.DELETE) {
      keyValues.remove(log.getKv().getKey());
    } else;
  }
  
  private Path dataFile;
  
  private Path getStorageFilePath() {
    if(dataFile != null)
      return dataFile;
    
    String rootDir = conf.getString(RAFT_ROOT_DIR_KEY).trim();
    String node = getServerInfo().getHost() + "-" + getServerInfo().getPort();
    dataFile = Paths.get(rootDir).resolve(node).resolve("data");

    return dataFile;
  }
  private volatile boolean persistentDataLoaded = false;
  
  private void loadPersistentData() {
    if(persistentDataLoaded) {
      return;
    }
    try ( FileInputStream fis = new FileInputStream(getStorageFilePath().toFile())) {
      long maxIndex = 0;
      while(true) {
        try {
          //LogEntry entry = (LogEntry)ois.readObject();
          RaftLogEntry.Builder builder = RaftLogEntry.newBuilder();
          if(!builder.mergeDelimitedFrom(fis)) {
            break;
          }
          RaftLogEntry entry = builder.build();
          entries.put(entry.getIndex(), entry);
          if(entry.getIndex() > maxIndex) {
            maxIndex = entry.getIndex();
          }
          
          setCommitIndex(maxIndex);
          setLastApplied(maxIndex);
          setFlushedIndex(maxIndex);
          
          replayKeyValueLogEntries(maxIndex, maxIndex);
          
        } catch(EOFException e) {
          break;
        }
      }
    } catch(FileNotFoundException e) {
      LOG.warn("no persistant data to load");
    } catch(IOException e) {
      LOG.error("ERROR loadPersistentData" + e.getMessage(), e);
      //Will not try to load again if exception occurs.
      //so still set loaded mark persistentDataLoaded
    }
    persistentDataLoaded = true;
  }
  
  /** Lock for flushing committed log */
  private final ReentrantLock flushLock = new ReentrantLock();
  
  /** Wait condition for flushing committed log */
  private final Condition needFlush = flushLock.newCondition();
  
  private void flushCommitted() {
    try {
      flushLock.lock();
      needFlush.signal();
    } finally {
      flushLock.unlock();
    }
  }
  
  private void startFlushWorker() {
    Thread t = new Thread(new LogFlushWorker());
    t.setName("LogFlushWorker");
    t.start();
  }
  
  class LogFlushWorker implements Runnable {
    @Override
    public void run() {
      while(true) {
        try {
          flushLock.lock();
          while (getFlushedIndex() >= getCommitIndex()) {
            needFlush.await();
          }
        } catch(InterruptedException e) { 
          LOG.info("Interrupted, LogFlushWorker exiting");
          return;
        } finally {
          flushLock.unlock();
        }
        doFlush();
      }
    }
     
    private void doFlush() {
      Path path = getStorageFilePath();
      boolean append = path.toFile().exists();
      if(!path.getParent().toFile().exists()) {
        try {
          Files.createDirectories(path.getParent());
        } catch(IOException e) {
          LOG.error("ERROR flushCommitted", e);
          return;
        }
      } 
      try (/* ObjectOutputStream oos = AppendableObjectOutputStream.create(*/
          FileOutputStream fos = new FileOutputStream(path.toFile(), append)) {
        for(long index = flushedIndex.get() + 1; index <= commitIndex.get(); index++) {
          RaftLogEntry entry = entries.get(index);
          entry.writeDelimitedTo(fos);
          setFlushedIndex(index);
        }
      } catch(IOException e) {
        LOG.error("ERROR flushCommitted", e);
        return;
      }
    }
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
