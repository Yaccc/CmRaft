package com.chicm.cmraft.rpc;

import com.google.protobuf.Message;

public class RpcCall {
  private long callId;
  private Message requestHeader;
  private Message request;
  
  public RpcCall(Message requestHeader, Message request) {
    this.request = request;
    this.requestHeader = requestHeader;
  }
  
  public long getCallId() {
    return callId;
  }
  public void setCallId(long callId) {
    this.callId = callId;
  }
  /**
   * @return the requestHeader
   */
  public Message getRequestHeader() {
    return requestHeader;
  }
  /**
   * @param requestHeader the requestHeader to set
   */
  public void setRequestHeader(Message requestHeader) {
    this.requestHeader = requestHeader;
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
