/**
* Copyright 2014 The Apache Software Foundation
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

import org.junit.Test;

public class TestRpcSendQueue {

  @Test
  public void testRpcSendQueue() {
    RpcSendQueue q = new RpcSendQueue(null);
    long tm = System.currentTimeMillis();
    for(int i = 0;i < 1000000; i++) {
      RpcCall call = new RpcCall(RpcClient.generateCallId(), null, null, null);
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
    System.out.println("PUT done****************");
    long t = System.currentTimeMillis() - tm;
    System.out.println("" + t/1000);
  }

}
