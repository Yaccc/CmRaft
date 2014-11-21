package com.chicm.cmraft.rpc;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;

public class RpcCall {
  private int callId;
  private Message header;
  private Message request;
  private MethodDescriptor md;
  
  public MethodDescriptor getMd() {
    return md;
  }

  public void setMd(MethodDescriptor md) {
    this.md = md;
  }

  public RpcCall(int callId, Message header, Message request, MethodDescriptor md) {
    this.request = request;
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
  public Message getRequest() {
    return request;
  }
  /**
   * @param request the request to set
   */
  public void setRequest(Message request) {
    this.request = request;
  }
  
}
