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


import org.junit.Test;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.core.RaftRpcService;

public class TestRpcServer {

  public static void main(String[] args) {
    RpcServer server = new RpcServer(CmRaftConfiguration.create(), RaftRpcService.create());
    server.startRpcServer();
    server.startTPSReport();
    
    for (int i =0; i < 2; i++) {
      final RpcClient client = new RpcClient(CmRaftConfiguration.create(), "localhost", server.getServerPort());
    
      for(int j = 0; j < 10; j++) {
        new Thread(new Runnable() {
          public void run() {
            client.sendRequest();
            
          }
        }).start();
      }
    }
  }

  @Test
  public void testStartServer() {
    RpcServer server = new RpcServer(CmRaftConfiguration.create(), RaftRpcService.create());
    server.startRpcServer();
  }
}


/*
class TestClient implements Runnable {
  private static final int NTHREADS = 100;
  
  public static void sendData() {
    ExecutorService es = Executors.newFixedThreadPool(NTHREADS);
    Thread[] threads = new Thread[NTHREADS];
    for(int i = 0; i < NTHREADS; i++) {
      threads[i] = new Thread(new TestClient());
      threads[i].setName(String.format("CLIENT%02d", i));
      es.submit(threads[i]);
      
    }
    try {
      es.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      
    }
  }
  
  public void run() {
    try (SocketChannel channel = SocketChannel.open()) {
      SocketAddress adr = new InetSocketAddress("localhost", RpcServer.DEFAULT_SERVER_PORT);
      channel.connect(adr);
      
      for(int i =0; i< 1; i++) {
      String str = Thread.currentThread().getName() + " Hello";
      byte[] bytes = str.getBytes();
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      channel.write(buf);

      
      channel.close();
      }
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
  }
}*/