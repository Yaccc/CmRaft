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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

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
import com.chicm.cmraft.rpc.TestNettyServer.MyProtobufDecoder;
import com.chicm.cmraft.rpc.TestNettyServer.MyRcpCallHandler;
import com.chicm.cmraft.rpc.TestNettyServer.MyRpcEncoder;
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
  private static final String LOCAL_SERVER_ADDRESS_KEY = "raft.server.local";
  private static final String RPCSERVER_LISTEN_THREADS_KEY = "rpcserver.listen.threads";
  private final static int DEFAULT_RPC_LISTEN_THREADS = 10;
  private final static int DEFAULT_REQUEST_WORKER = 10;
  private final static int DEFAULT_RESPONSE_WOKER = 10;
  private final static int MAX_PACKET_SIZE = 1024*1024*100;
  //private SocketListener socketListener = null;
  private int rpcListenThreads = 0;
  private RaftRpcService service = null;
  private PriorityBlockingQueue<RpcCall> requestQueue = new PriorityBlockingQueue<RpcCall>();
  private PriorityBlockingQueue<RpcCall> responseQueue = new PriorityBlockingQueue<RpcCall>();
  private final static AtomicLong callCounter = new AtomicLong(0);
  private boolean tpsReportStarted = false;
  private ServerInfo serverInfo;
  
  public RpcServer (Configuration conf, RaftRpcService service) {
    this.conf = conf;
    //socketListener = new SocketListener();
    rpcListenThreads = conf.getInt("rpcserver.listen.threads", DEFAULT_RPC_LISTEN_THREADS);
    this.service = service;
    this.serverInfo = ServerInfo.parseFromString(conf.getString(LOCAL_SERVER_ADDRESS_KEY));
  }
  
  public BlockingService getService() {
    return service.getService();
  }
  
  public ServerInfo getServerInfo() {
    return this.serverInfo;
  }
  
  public boolean startRpcServer() {
    try {
      //socketListener.start();
      new NettyListener().start();
      //startRequestWorker(conf.getInt("rpcserver.request.workers", DEFAULT_REQUEST_WORKER));
      //startResponseWorker(conf.getInt("rpcserver.response.workers", DEFAULT_RESPONSE_WOKER));
    } catch(InterruptedException e) {
      e.printStackTrace(System.out);
      return false;
    }
    return true;
  }
  
  class SocketHandler implements CompletionHandler<AsynchronousSocketChannel, AsynchronousServerSocketChannel> {
    @Override
    public void completed(AsynchronousSocketChannel channel, AsynchronousServerSocketChannel serverChannel) {
      
      serverChannel.accept(serverChannel, this);
      try {
        LOG.info(String.format("SERVER[%s] accepted, remote host:[%s]\n", 
          getServerInfo(), channel.getRemoteAddress().toString()));
      } catch(IOException e) {
        LOG.error("accept exception", e);
      }
      //startTPSReport();

      while(true) {
        try {
          processRequest(channel);
        } catch(Exception e) {
          LOG.info("Exception: " + e.getClass().getName());
          if(e.getCause() != null) {
            LOG.info("Caused by: " + e.getCause().getClass().getName() + ":"  + e.getCause().getMessage());
          }
          try {
            channel.close();
          } catch(Exception e2) {
          }
          LOG.info("Socket connection closed.");
          break;
        } 
        callCounter.incrementAndGet();
        LOG.debug("request processed");
      }
    }
    @Override
    public void failed(Throwable throwable, AsynchronousServerSocketChannel attachment) {
      LOG.error("Exception", throwable);
    }
  }
  
  class NettyListener {
    public void start() throws InterruptedException {
      EventLoopGroup bossGroup = new NioEventLoopGroup(); 
      EventLoopGroup workerGroup = new NioEventLoopGroup();
      
      ServerBootstrap boot = new ServerBootstrap(); 
      boot.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) throws Exception {
               ch.pipeline().addLast("FrameDecoder", new LengthFieldBasedFrameDecoder(MAX_PACKET_SIZE,0,4,0,4)); 
               ch.pipeline().addLast("FrameEncoder", new LengthFieldPrepender(4));
               ch.pipeline().addLast("MessageDecoder", new RpcMessageDecoder() );
               ch.pipeline().addLast("MessageEncoder", new RpcMessageEncoder());
               ch.pipeline().addLast("RpcHandler", new RpcCallInboundHandler(getService()));
               LOG.info("initChannel");
             }
          })
         .option(ChannelOption.SO_BACKLOG, 128)          
         .childOption(ChannelOption.SO_KEEPALIVE, true); 

      // Bind and start to accept incoming connections.
      ChannelFuture f = boot.bind(getServerInfo().getPort()).sync(); 

      // Wait until the server socket is closed.
      System.out.println("server started");        
      //f.channel().closeFuture().sync();
    }
  }
  
  class SocketListener {
    public void start() throws IOException {
      
      AsynchronousChannelGroup group = AsynchronousChannelGroup.withFixedThreadPool(rpcListenThreads, 
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r);
              t.setName("Socket-Listener-" + (byte)System.currentTimeMillis());
              return t;
            }
          });
      /*
      ExecutorService executor = Executors.newFixedThreadPool(rpcListenThreads,
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("Socket-Listener-" + (byte)System.currentTimeMillis());
            return t;
          }
      });
      AsynchronousChannelGroup group = AsynchronousChannelGroup.withCachedThreadPool(executor, rpcListenThreads);
      */
      
      LOG.info("Server binding to:" + getServerInfo().getPort());
      final AsynchronousServerSocketChannel serverChannel = AsynchronousServerSocketChannel.open(group)
          .bind(new InetSocketAddress(getServerInfo().getPort()));
      
      serverChannel.accept(serverChannel, new SocketHandler());
      
      LOG.info("Server started, listen threads: " + rpcListenThreads );
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
      
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Exception", e);
      throw e;
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
  
  private final ConcurrentHashMap<AsynchronousSocketChannel, Lock> locks = new ConcurrentHashMap<>();
  class ResponseWorker implements Runnable {
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
      System.out.println("usage: RpcServer <listening port> <listen threads number>");
      return;
    }
    int port = Integer.parseInt(args[0]);
    
    Configuration conf = CmRaftConfiguration.create();
    conf.set(LOCAL_SERVER_ADDRESS_KEY, "localhost:" + port);
    conf.set(RPCSERVER_LISTEN_THREADS_KEY, args[1]);
    
    RpcServer server = new RpcServer(conf, RaftRpcService.create());
    LOG.info("starting server");
    server.startRpcServer();
    server.startTPSReport();
  }
}
