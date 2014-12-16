package com.chicm.cmraft.rpc;

public class RpcTimeoutException extends Exception {

  private static final long serialVersionUID = 3670017286407961069L;

  public RpcTimeoutException (String msg) {
    super(msg);
  }
}
