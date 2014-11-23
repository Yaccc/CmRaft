package com.chicm.cmraft.rpc;

public class TestRpcSendQueue {

  public static void main(String[] args) {
    RpcSendQueue q = RpcSendQueue.getInstance(null);
    
    for(int i = 0;i < 100; i++) {
      RpcCall call = new RpcCall(RaftRpcClient.generateCallId(), null, null, null);
      call.setPriority(10);
      if (i % 2 == 0) {
        call.setPriority(20);
      }
      try {
        q.put(call);
      } catch(Exception e) {
        e.printStackTrace(System.out);
      }
      
    }
    System.out.println("PUT done");
  }

}
