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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.core.RaftRpcService;
import com.google.protobuf.BlockingService;

/**
 * A RpcServer is a socket server listening on specified port, accepting client's connection
 * and handle RPC requests. In order to prevent slow RPC calls blocking fast ones, It uses 2 
 * queues to process RPC requests.
 * @author chicm
 *
 */
public class RpcServer {
  static final Log LOG = LogFactory.getLog(RpcServer.class);
  
  private static final String LOCAL_SERVER_ADDRESS_KEY = "raft.server.local";
  private static final int DEFAULT_CONNECTION_BACKLOG = 200;
  private RaftRpcService service = null;
  private PriorityBlockingQueue<RpcCall> requestQueue = new PriorityBlockingQueue<RpcCall>();
  private PriorityBlockingQueue<RpcCall> responseQueue = new PriorityBlockingQueue<RpcCall>();
  private final static AtomicLong callCounter = new AtomicLong(0);
  private boolean tpsReportStarted = false;
  private ServerInfo serverInfo;
  
  public RpcServer (Configuration conf, RaftRpcService service) {
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
      new NettyListener().start();
    } catch(InterruptedException e) {
      e.printStackTrace(System.out);
      return false;
    }
    return true;
  }

  class NettyListener {
    public void start() throws InterruptedException {
      EventLoopGroup bossGroup = new NioEventLoopGroup(); 
      EventLoopGroup workerGroup = new NioEventLoopGroup();
      ServerChannelHandler handler = new ServerChannelHandler(getService(), callCounter);
      ServerBootstrap boot = new ServerBootstrap(); 
      boot.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(handler)
          .option(ChannelOption.SO_BACKLOG, DEFAULT_CONNECTION_BACKLOG)    
          .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
          .childOption(ChannelOption.SO_KEEPALIVE, true); 

      // Bind and start to accept incoming connections.
      ChannelFuture f = boot.bind(getServerInfo().getPort()).sync(); 

      // Wait until the server socket is closed.
      LOG.info("server started");        
      //f.channel().closeFuture().sync();
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
  
  /**
   * For test purpose
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    
    if(args.length < 1) {
      System.out.println("usage: RpcServer <listening port>");
      return;
    }
    int port = Integer.parseInt(args[0]);
    
    Configuration conf = CmRaftConfiguration.create();
    conf.set(LOCAL_SERVER_ADDRESS_KEY, "localhost:" + port);
    
    RpcServer server = new RpcServer(conf, RaftRpcService.create());
    LOG.info("starting server");
    server.startRpcServer();
    server.startTPSReport();
  }
}
