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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.util.CappedPriorityBlockingQueue;

public class RpcSendQueue {
  static final Log LOG = LogFactory.getLog(RpcSendQueue.class);
  private static int DEFAULT_WORKERS = 1;
  private AsynchronousSocketChannel channel;
  private CappedPriorityBlockingQueue<RpcCall> callQueue; 
  private ExecutorService executor;
  private volatile boolean stop = false;
  
  private static int MAX_SEND_CALL_QUEUE_SIZE = 100*1024;
   
  public RpcSendQueue(AsynchronousSocketChannel channel) {
    this.channel = channel;
    callQueue = new CappedPriorityBlockingQueue<RpcCall>(MAX_SEND_CALL_QUEUE_SIZE);
    executor = Executors.newFixedThreadPool(DEFAULT_WORKERS,
      new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName("RPC-sending-worker-" + System.currentTimeMillis());
          return t;
        }
    });
    for(int i = 0; i < DEFAULT_WORKERS; i++) {
      Thread t = new Thread(new WorkerThread());
      t.setDaemon(true);
      executor.execute(t);
    }
  }
  
  public void stop() {
    stop = true;
    executor.shutdownNow();
  }
  
  public void put(RpcCall call) {
    callQueue.put(call);
  }
  
  public int size() {
    return callQueue.size();
  }
  
  class WorkerThread implements Runnable {
    @Override
    public void run() {
      String name = Thread.currentThread().getName();
      while(true) {
        try {
          RpcCall call = callQueue.take();
          if(call == null) {
            LOG.error("Thread: [" + name + "] call is null");
            return;
          }
          LOG.debug(String.format("Thread[%s], take: call id: %d", name, call.getCallId()));
          LOG.debug("send queue size:" + callQueue.size());
          try {
            if(channel != null) {
              PacketUtils.writeRpc(channel, call.getHeader(), call.getMessage());
            } else {
              LOG.error("channel == null");
            }
              
          } catch (Exception e) {
            LOG.error("Thread: " + name, e);
          }
          
        } catch (InterruptedException e) {
          if(stop) {
            LOG.info("RpcSendQueue exiting");
          } else {
            LOG.error("InterruptedException catched", e);
          }
          break;
        }
      }
    }
  }
}
