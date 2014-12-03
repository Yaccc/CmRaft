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

import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftEntry;
import com.google.protobuf.ByteString;

public class LogEntry {
  private long index;
  private long term;
  private byte[] key;
  private byte[] value;
  private LogMutationType logType;
  
  public LogEntry(long index, long term, byte[] key, byte[] value, LogMutationType logType) {
    this.index = index;
    this.term = term;
    this.key = key;
    this.value = value;
    this.logType = logType;
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
  
  /**
   * @return the logType
   */
  public LogMutationType getLogType() {
    return logType;
  }

  /**
   * @param logType the logType to set
   */
  public void setLogType(LogMutationType logType) {
    this.logType = logType;
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
