package com.chicm.cmraft.log;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.core.RaftNode;
import com.chicm.cmraft.protobuf.generated.RaftProtos.AppendEntriesResponse;

public class LogManager {
  private SortedMap<Long, LogEntry> entries = new TreeMap<>();
  private final static long INITIAL_INDEX = 1;
  private final static long INITIAL_TERM = 0;
  private RaftNode node;
  private long commitIndex;
  private long lastApplied;
  private final AtomicLong currentIndex = new AtomicLong(0);
  
  public LogManager(RaftNode node) {
    this.node = node;
  }
  
  public long getCurrentIndex() {
    if(entries == null || entries.size() < 1)
      return INITIAL_INDEX;
    return entries.lastKey();
  }
  
  public long getCurrentTerm() {
    if(entries == null || entries.size() < 1)
      return INITIAL_TERM;
    return entries.get(getCurrentIndex()).getTerm();
  }
  
  /**
   * @return the commitIndex
   */
  public long getCommitIndex() {
    return commitIndex;
  }
  /**
   * @param commitIndex the commitIndex to set
   */
  public void setCommitIndex(long commitIndex) {
    this.commitIndex = commitIndex;
  }
  /**
   * @return the lastApplied
   */
  public long getLastApplied() {
    return lastApplied;
  }
  /**
   * @param lastApplied the lastApplied to set
   */
  public void setLastApplied(long lastApplied) {
    this.lastApplied = lastApplied;
  }
  
  // for followers
  public void appendEntries(long term, ServerInfo leaderId, long leaderCommit,
      long prevLogIndex, long prevLogTerm, LogEntry[] entries) {
    
  }
  // for leaders
  public void set(byte[] key, byte[] value) {
    LogEntry entry = new LogEntry(currentIndex.incrementAndGet(), node.getCurrentTerm(), key, value);
    entries.put(entry.getIndex(), entry);
  }
  
  public Collection<LogEntry> list(byte[] pattern) {
    return entries.values();
  }
}
