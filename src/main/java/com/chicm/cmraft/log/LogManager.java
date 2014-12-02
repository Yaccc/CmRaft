package com.chicm.cmraft.log;

import java.util.Collection;
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

public class LogManager {
  static final Log LOG = LogFactory.getLog(LogManager.class);
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
  
  public LogManager(RaftNode node, Configuration conf) {
    this.node = node;
    this.conf = conf;
  }
  
  private void leaderInit() {
    for(ServerInfo remoteServer: ServerInfo.getRemoteServersFromConfiguration(conf)) {
      FollowerIndexes fIndexes = new FollowerIndexes(getLastApplied() +1, 0);
      followerIndexes.put(remoteServer, fIndexes);
    }
  }
  
  private void cleanupLeaderWorker() {
    
  }
  
  public void stateChange(State oldState, State newState) {
    if(oldState == State.LEADER && newState != State.LEADER) {
      cleanupLeaderWorker();
    } else if(newState == State.LEADER && oldState != State.LEADER) {
      leaderInit();
    }
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
  public void appendEntries(long term, ServerInfo leaderId, long leaderCommit,
      long prevLogIndex, long prevLogTerm, LogEntry[] entries) {
    
  }
  // for leaders
  public void set(byte[] key, byte[] value) {
    //lastApplied initialized as 0, and the first time increase it to 1,
    //so the first index is 1
    LogEntry entry = new LogEntry(lastApplied.incrementAndGet(), node.getCurrentTerm(), key, value);
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
