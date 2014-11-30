package com.chicm.cmraft.log;

import java.util.SortedMap;
import java.util.TreeMap;

public class LogManager {
  private SortedMap<Long, LogEntry> entries = new TreeMap<>();
  private final static long INITIAL_INDEX = 1;
  private final static long INITIAL_TERM = 0;
  private long commitIndex;
  private long lastApplied;
  
  public LogManager() {
    
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
}
