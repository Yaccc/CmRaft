package com.chicm.cmraft.rpc;

import java.nio.channels.AsynchronousSocketChannel;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;

public class RpcCall implements Comparable<RpcCall>{
  private int callId;
  private Message header;
  private Message message;
  private MethodDescriptor md;
  private AsynchronousSocketChannel channel;
  private long timestamp;
  
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

  public RpcCall(int callId, Message header, Message msg, MethodDescriptor md) {
    this.message = msg;
    this.header = header;
    this.md = md;
    this.callId = callId;
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
