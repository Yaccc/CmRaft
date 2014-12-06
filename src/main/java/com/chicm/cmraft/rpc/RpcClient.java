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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService.BlockingInterface;
import com.chicm.cmraft.protobuf.generated.RaftProtos.TestRpcRequest;
import com.chicm.cmraft.util.BlockingHashMap;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.google.protobuf.Descriptors.MethodDescriptor;

/**
 * RpcClient implements the BlockingRpcChannel interface with inner class. It translate RPC method calls to 
 * RPC request packets and send them to RPC server. Then translate RPC response packets from
 * RPC server to returned objects for RPC method calls.
 * 
 * @author chicm
 *
 */
public class RpcClient {
  static final Log LOG = LogFactory.getLog(RpcClient.class);
  private final static int DEFAULT_RPC_TIMEOUT = 2000;
  private final static int DEFAULT_RPC_RETRIES = 3;
  private static volatile AtomicInteger client_call_id = new AtomicInteger(0);
  private BlockingInterface stub = null;
  private ChannelHandlerContext ctx = null;
  private BlockingHashMap<Integer, RpcCall> responsesMap = new BlockingHashMap<>();
  private volatile boolean initialized = false;
  private int rpcTimeout;
  private int rpcRetries;
  private ServerInfo remoteServer = null;
  
  public RpcClient(Configuration conf, ServerInfo remoteServer) {
    rpcTimeout = conf.getInt("rpc.call.timeout", DEFAULT_RPC_TIMEOUT);
    rpcRetries = conf.getInt("rpc.call.retries", DEFAULT_RPC_RETRIES);
    this.remoteServer = remoteServer;
  }
  
  private boolean isInitialized() {
    return initialized;
  }
  
  private synchronized boolean init() 
      throws IOException, InterruptedException, ExecutionException {
    
    if(isInitialized())
      return true;
    try {
      ctx = connect();
    } catch(Exception e) {
      e.printStackTrace(System.out);
      return false;
    }
    
    BlockingRpcChannel c = createBlockingRpcChannel(responsesMap);
    stub =  RaftService.newBlockingStub(c);

    
    initialized = true;
    return initialized;
  }
  
  public ServerInfo getRemoteServer() {
    return remoteServer;
  }
  
  public void close() {
    try {
      ctx.close().sync();
    } catch(Exception e) {
      LOG.error("Closing failed", e);
    }
  }
  
  public BlockingInterface getStub() {
    if(!isInitialized()) {
      try { 
        init();
      } catch(Exception e) {
        LOG.error("RpcClient Initialization exception", e);
        return null;
      }
    }
    return stub;
  }
  
  public ChannelHandlerContext connect() throws Exception {
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    
    try {
      ClientChannelHandler channelHandler = new ClientChannelHandler(responsesMap);
      Bootstrap b = new Bootstrap(); 
      b.group(workerGroup); 
      b.channel(NioSocketChannel.class); 
      b.option(ChannelOption.SO_KEEPALIVE, true); 
      b.handler(channelHandler);

      ChannelFuture f = b.connect(getRemoteServer().getHost(), getRemoteServer().getPort()).sync(); 
      LOG.info("connected to: " + this.getRemoteServer() );
      return channelHandler.getCtx();
        // Wait until the connection is closed.
        //f.channel().closeFuture().sync();
    } finally {
        //workerGroup.shutdownGracefully();
    }
  }
  
  public static int generateCallId() {
    return client_call_id.incrementAndGet();
  }
  
  public static int getCallId() {
    return client_call_id.get();
  }
 
  private  BlockingRpcChannel createBlockingRpcChannel(BlockingHashMap<Integer, RpcCall> responseMap) {
    return new BlockingRpcChannelImplementation(responseMap);
  }

  class BlockingRpcChannelImplementation implements BlockingRpcChannel {
    private BlockingHashMap<Integer, RpcCall> responseMap = null;

    protected BlockingRpcChannelImplementation(BlockingHashMap<Integer, RpcCall> responseMap) {
      this.responseMap = responseMap;
    }

    @Override
    public Message callBlockingMethod(MethodDescriptor md, RpcController controller,
                                      Message request, Message returnType) throws ServiceException {

      Message response = null;
      try {
        RequestHeader.Builder builder = RequestHeader.newBuilder();
        int callId = generateCallId();
        builder.setId(callId); 
        builder.setRequestName(md.getName());
        RequestHeader header = builder.build();
        
        LOG.debug("SENDING RPC, CALLID:" + header.getId());
        RpcCall call = new RpcCall(callId, header, request, md);
        long tm = System.currentTimeMillis();
        
        ctx.writeAndFlush(call);
        
        RpcCall result = this.responseMap.take(callId, rpcTimeout, rpcRetries);
        
        response = result != null? result.getMessage() : null;
        if(response != null) {
          LOG.debug("response taken: " + callId);
          LOG.debug(String.format("RPC[%d] round trip takes %d ms", header.getId(), (System.currentTimeMillis() - tm)));
        }
       } catch(Exception e) {
        e.printStackTrace(System.out);
      }
      return response;
    }
  }
  
 
  /*
   * For testing purpose
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if(args.length < 3) {
      System.out.println("usage: RpcClient <server host> <server port> <clients number> <threads number> <packetsize>");
      return;
    }
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    int nclients = Integer.parseInt(args[2]);
    int nThreads = Integer.parseInt(args[3]);
    int nPacketSize = 1024;

    if(args.length >= 5) {
      nPacketSize = Integer.parseInt(args[4]);
    }
    
    for(int j =0; j < nclients; j++ ) {
      RpcClient client = new RpcClient(CmRaftConfiguration.create(), new ServerInfo(host, port));
      
      for(int i = 0; i < nThreads; i++) {
        new Thread(new TestRpcWorker(client, nPacketSize)).start();
      }
    }
  }
  
  static class TestRpcWorker implements Runnable{
    private RpcClient client;
    private int packetSize;
    
    public TestRpcWorker(RpcClient client, int size) {
      this.client = client;
      this.packetSize = size;
    }
    @Override
    public void run() {
      client.sendRequest(packetSize);
    }
  }
  
  public void testRpc(int packetSize) throws ServiceException {
    TestRpcRequest.Builder builder = TestRpcRequest.newBuilder();
    byte[] bytes = new byte[packetSize];
    builder.setData(ByteString.copyFrom(bytes));
    
    getStub().testRpc(null, builder.build());
  }
  
  private ThreadLocal<Long> startTime = new ThreadLocal<>();
  
  /*
   * For testing purpose
   */
  public void sendRequest(int packetSize) {
    
    if(!this.isInitialized()) {
      try {
        if(!init()) {
          LOG.error("INIT error");
          return;
        }
      } catch(Exception e) {
        LOG.error("RpcClient init exception", e);
        return;
      }
    }
    
    LOG.info("client thread started");
    long starttime = System.currentTimeMillis();
    try {
      for(int i = 0; i < 5000000 ;i++) {
        startTime.set(System.currentTimeMillis());
        testRpc(packetSize);
        if(i != 0 && i %1000 == 0 ) {
          long ms = System.currentTimeMillis() - starttime;
          LOG.debug("RPC CALL[ " + i + "] round trip time: " + ms);
          
          long curtm = System.currentTimeMillis();
          long elipsetm = (curtm - starttime) /1000;
          if(elipsetm == 0)
            elipsetm =1;
          long tps = i / elipsetm;
          
          LOG.info("response id: " + i + " time: " + elipsetm + " TPS: " + tps);
        }
      }
    } catch (Exception e) {
      e.printStackTrace(System.out);
    } 
  }
}
