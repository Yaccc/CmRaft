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

package com.chicm.cmraft.rpc;

import java.nio.channels.AsynchronousSocketChannel;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;

/**
 * Encapsulate RPC calls
 * 
 * @author chicm
 *
 */

public class RpcCall implements Comparable<RpcCall>{
  private int callId;
  private Message header;
  private Message message;
  private MethodDescriptor md;
  private AsynchronousSocketChannel channel;
  private long timestamp;
  
  public RpcCall(int callId, Message header, Message msg, MethodDescriptor md) {
    this.message = msg;
    this.header = header;
    this.md = md;
    this.callId = callId;
  }
  
  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public AsynchronousSocketChannel getChannel() {
    return channel;
  }

  public void setChannel(AsynchronousSocketChannel channel) {
    this.channel = channel;
  }

  /** smaller priority number has higher priority */
  private int priority = 10;
  
  /**
   * @return the priority
   */
  public int getPriority() {
    return priority;
  }

  /**
   * @param priority the priority to set
   */
  public void setPriority(int priority) {
    this.priority = priority;
  }

  public MethodDescriptor getMd() {
    return md;
  }

  public void setMd(MethodDescriptor md) {
    this.md = md;
  }
  
  public int getCallId() {
    return callId;
  }
  public void setCallId(int callId) {
    this.callId = callId;
  }
  /**
   * @return the requestHeader
   */
  public Message getHeader() {
    return header;
  }
  /**
   * @param requestHeader the requestHeader to set
   */
  public void setHeader(Message header) {
    this.header = header;
  }
  /**
   * @return the request
   */
  public Message getMessage() {
    return message;
  }
  /**
   * @param request the request to set
   */
  public void setMessage(Message msg) {
    this.message = msg;
  }
  
  @Override
  public String toString() {
    return "RpcCall{callId=" + callId + "\n" + message.toString() + "}";
  }
  
  @Override
  public int compareTo(RpcCall otherCall) {
    if(this.priority < otherCall.getPriority())
      return -1;
    if(this.priority > otherCall.getPriority())
      return 1;
    
    // priority must equal
    if(this.getCallId() < otherCall.getCallId())
      return -1;
    else if(this.callId > otherCall.getCallId())
      return 1;
    else 
        return 0;
  }
}
