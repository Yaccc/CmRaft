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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.WritePendingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.core.RaftRpcService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ResponseHeader;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;

/**
 * A RpcServer is a socket server listening on specified port, accepting client's connection
 * and handle RPC requests. In order to prevent slow RPC calls blocking fast ones, It uses 2 
 * queues to process RPC requests.
 * @author chicm
 *
 */
public class RpcServer {
  static final Log LOG = LogFactory.getLog(RpcServer.class);
  
  private Configuration conf = null;
  private static int DEFAULT_SERVER_PORT = 12888;
  private final static int DEFAULT_RPC_LISTEN_THREADS = 10;
  private final static int DEFAULT_REQUEST_WORKER = 10;
  private final static int DEFAULT_RESPONSE_WOKER = 10;
  private SocketListener socketListener = null;
  private int rpcListenThreads = DEFAULT_RPC_LISTEN_THREADS;
  private RaftRpcService service = null;
  private static PriorityBlockingQueue<RpcCall> requestQueue = new PriorityBlockingQueue<RpcCall>();
  private static PriorityBlockingQueue<RpcCall> responseQueue = new PriorityBlockingQueue<RpcCall>();
  private final static AtomicLong callCounter = new AtomicLong(0);
  private static boolean tpsReportStarted = false;
  
  public RpcServer (Configuration conf, RaftRpcService service) {
    this.conf = conf;
    socketListener = new SocketListener();
    rpcListenThreads = conf.getInt("rpcserver.listen.threads", DEFAULT_RPC_LISTEN_THREADS);
    this.service = service;
  }
  
  public BlockingService getService() {
    return service.getService();
  }
  
  public int getServerPort() {
    int port = 0;
    try {
      port = ServerInfo.parseFromString(conf.getString("raft.server.local")).getPort();
    } catch (Exception e) {
      LOG.error("get port from config", e);
      port = DEFAULT_SERVER_PORT;
    }
    return port;
  }
  
  public boolean startRpcServer() {
    try {
      socketListener.start();
      startRequestWorker(DEFAULT_REQUEST_WORKER);
      startResponseWorker(DEFAULT_RESPONSE_WOKER);
    } catch(IOException e) {
      e.printStackTrace(System.out);
      return false;
    }
    return true;
  }
  
  class SocketHandler implements CompletionHandler<AsynchronousSocketChannel, AsynchronousServerSocketChannel> {
    @Override
    public void completed(AsynchronousSocketChannel channel, AsynchronousServerSocketChannel serverChannel) {
      
      serverChannel.accept(serverChannel, this);
      LOG.info(String.format("SERVER[%d] accepted\n", Thread.currentThread().getId()));
      startTPSReport();

      for(;;) {
        try {
          processRequest(channel);
        } catch(Exception e) {
          e.printStackTrace(System.out);
          try {
            channel.close();
          } catch(Exception e2) {
          }
          LOG.info("BREAK");
          break;
        } 
        callCounter.incrementAndGet();
        LOG.debug("request processed");
      }
      LOG.info("BREAK OUT");
    }
    @Override
    public void failed(Throwable throwable, AsynchronousServerSocketChannel attachment) {
      LOG.error("Exception", throwable);
    }
  }
  
  class SocketListener {
    public void start() throws IOException {
      AsynchronousChannelGroup group = AsynchronousChannelGroup.withFixedThreadPool(rpcListenThreads, 
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r);
              t.setName("Socket-Listener-" + System.currentTimeMillis());
              return t;
            }
          });
      LOG.info("Server binding to:" + getServerPort());
      final AsynchronousServerSocketChannel serverChannel = AsynchronousServerSocketChannel.open(group)
          .bind(new InetSocketAddress(getServerPort()));
      
      serverChannel.accept(serverChannel, new SocketHandler());
      
      LOG.info("Server started");
    }
  }
  
  private void processRequest(AsynchronousSocketChannel channel) 
      throws InterruptedException, ExecutionException, IOException, ServiceException {
    try {
      long curtime1 = System.currentTimeMillis();
      RpcCall call = PacketUtils.parseRpcRequestFromChannel(channel, getService());
      long curtime2 = System.currentTimeMillis();
      LOG.debug("Parsing request takes: " + (curtime2-curtime1) + " ms");
      if(call == null)
        return;
      LOG.debug("server recieved: call id: " + call.getCallId());
      
      //use queue
      call.setChannel(channel);
      call.setTimestamp(System.currentTimeMillis());
      requestQueue.put(call);
      
      //Message response = getService().callBlockingMethod(call.getMd(), null, call.getMessage());
      //sendResponse(channel, call, response);
      
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace(System.out);
      throw e;
    } /*catch(ServiceException e2) {
      e2.printStackTrace(System.out);
      LOG.info("EXCEPTION");
      throw e2;
    } */
  }
  
  private void sendResponse(AsynchronousSocketChannel channel, RpcCall call, Message response) {
    ResponseHeader.Builder builder = ResponseHeader.newBuilder();
    builder.setId(call.getCallId()); 
    builder.setResponseName(call.getMd().getName());
    ResponseHeader header = builder.build();
    
    try {
        PacketUtils.writeRpc(channel, header, response);
        
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
  }
  
  public void startTPSReport() {
    if (tpsReportStarted )
      return;
    
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while(true) {
          long calls = callCounter.get();
          long starttm = System.currentTimeMillis();
          try {
            Thread.sleep(5000);
          } catch(Exception e) {
            LOG.error("exception", e);
          }
          long sec = (System.currentTimeMillis() - starttm)/1000;
          if(sec == 0)
            sec =1;
          long n = callCounter.get() - calls;
          LOG.info("TPS: " + (n/sec));
          LOG.info("request queue: " + requestQueue.size() + " response queue: " + responseQueue.size());
        }
      }
    });
    thread.setDaemon(true);
    thread.setName("TPS report");
    thread.start();
    tpsReportStarted = true;
    LOG.info("TPS report started");
  }
  
  private void startRequestWorker (int workers) {
    ExecutorService executor = Executors.newFixedThreadPool(workers,
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("Request-Worker-" + System.currentTimeMillis());
            return t;
          }
      });
      for(int i = 0; i < workers; i++) {
        Thread t = new Thread(new RequestWorker());
        t.setDaemon(true);
        executor.execute(t);
      }
  }
  
  private void startResponseWorker (int workers) {
    ExecutorService executor = Executors.newFixedThreadPool(workers,
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("Response-Worker-" + System.currentTimeMillis());
            return t;
          }
      });
      for(int i = 0; i < workers; i++) {
        Thread t = new Thread(new ResponseWorker());
        t.setDaemon(true);
        executor.execute(t);
      }
  }
  
  class RequestWorker implements Runnable {
    @Override
    public void run() {
      while(true) {
        try {
          RpcCall call = requestQueue.take();
          if(call != null) {
            Message response = getService().callBlockingMethod(call.getMd(), null, call.getMessage());
            if(response != null) {
              ResponseHeader.Builder builder = ResponseHeader.newBuilder();
              builder.setId(call.getCallId()); 
              builder.setResponseName(call.getMd().getName());
              ResponseHeader header = builder.build();
              call.setHeader(header);
              call.setMessage(response);
              
              responseQueue.put(call);
            }
          }
        } catch(Exception e) {
          e.printStackTrace(System.out);
          LOG.error("exception", e);
        }
      }
    }
  }
  
  static class ResponseWorker implements Runnable {
    private static final ConcurrentHashMap<AsynchronousSocketChannel, Lock> locks = new ConcurrentHashMap<>();
    private static final ReentrantLock channelLock = new ReentrantLock();
    
    @Override
    public void run() {
      while(true) {
        try {
          RpcCall call = responseQueue.take();
          AsynchronousSocketChannel channel = call.getChannel();
          Lock lock = locks.get(channel);
          if(lock == null) {
            lock = new ReentrantLock();
            locks.put(channel, lock);
          }
          lock = locks.get(channel);
          try {
            lock.lock();
            //LOG.info("round trip in queue: " + (System.currentTimeMillis() - call.getTimestamp()));
            PacketUtils.writeRpc(call.getChannel(), call.getHeader(), call.getMessage());
          } finally {
            lock.unlock();
          }
        } catch(WritePendingException e) {
          LOG.error("retry", e);
          try {
            Thread.sleep(1);
          } catch(Exception e2) {
            
          }
        } catch(Exception e) {
          e.printStackTrace(System.out);
          LOG.error("exception", e);
          break;
        }
      }
      
    }
  }
  
  /**
   * For test purpose
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    
    if(args.length < 2) {
      System.out.println("usage: RpcServer <listening port> <listen threads number> [padding length]");
      return;
    }
    DEFAULT_SERVER_PORT = Integer.parseInt(args[0]);
    int nListenThreads = Integer.parseInt(args[1]);
    
    if(args.length == 3) {
      PacketUtils.TEST_PADDING_LEN = Integer.parseInt(args[2]);
    }
    
    RpcServer server = new RpcServer(CmRaftConfiguration.create(), RaftRpcService.create());
    LOG.info("starting server");
    server.startRpcServer();
    
  }
}
