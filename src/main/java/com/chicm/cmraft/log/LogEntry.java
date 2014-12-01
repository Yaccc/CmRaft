package com.chicm.cmraft.log;

import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftEntry;
import com.google.protobuf.ByteString;

public class LogEntry {
  private long index;
  private long term;
  private byte[] key;
  private byte[] value;
  
  public LogEntry(long index, long term, byte[] key, byte[] value) {
    this.index = index;
    this.term = term;
    this.key = key;
    this.value = value;
  }
  
  /**
   * @return the term
   */
  public long getTerm() {
    return term;
  }
  /**
   * @param term the term to set
   */
  public void setTerm(long term) {
    this.term = term;
  }
  
  /**
   * @return the index
   */
  public long getIndex() {
    return index;
  }
  /**
   * @param index the index to set
   */
  public void setIndex(long index) {
    this.index = index;
  }
  /**
   * @return the key
   */
  public byte[] getKey() {
    return key;
  }
  /**
   * @param key the key to set
   */
  public void setKey(byte[] key) {
    this.key = key;
  }
  /**
   * @return the value
   */
  public byte[] getValue() {
    return value;
  }
  /**
   * @param value the value to set
   */
  public void setValue(byte[] value) {
    this.value = value;
  }
  
  public RaftEntry toRaftEntry() {
    RaftEntry.Builder builder = RaftEntry.newBuilder();
    builder.setIndex(getIndex());
    if(getKey() != null) {
      builder.setKey(ByteString.copyFrom(getKey()));
    }
    if(getValue() != null) {
      builder.setValue(ByteString.copyFrom(getValue()));
    }
    
    return builder.build();
  }
}
