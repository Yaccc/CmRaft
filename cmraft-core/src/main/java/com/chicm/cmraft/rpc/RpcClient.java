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
import java.util.Random;
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
import com.google.common.base.Preconditions;
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
  private final static String RPC_TIMEOUT_KEY = "raft.rpc.timeout";
  private final static int DEFAULT_RPC_TIMEOUT = 3000;
  private static volatile AtomicInteger client_call_id = new AtomicInteger(0);
  private BlockingInterface stub = null;
  private ChannelHandlerContext ctx = null;
  private BlockingHashMap<Integer, RpcCall> responsesMap = new BlockingHashMap<>();
  private RpcClientEventListener listener = new RpcClientEventListenerImpl();
  private volatile boolean connected = false;
  private int rpcTimeout;
  private ServerInfo remoteServer = null;
  
  public RpcClient(Configuration conf, ServerInfo remoteServer) {
    rpcTimeout = conf.getInt(RPC_TIMEOUT_KEY, DEFAULT_RPC_TIMEOUT);
    this.remoteServer = remoteServer;
    //todo: to change call id init value
    Random r = new Random();
    client_call_id.set(r.nextInt(1000) * 100);
  }
  
  public boolean isConnected() {
    return connected;
  }
  
  private synchronized boolean connect() 
      throws IOException, InterruptedException, ExecutionException {
    
    if(isConnected())
      return true;
    try {
      ctx = connectRemoteServer();
    } catch(Exception e) {
      LOG.error("Failed connecting to:" + getRemoteServer() + " : " + e.getMessage());
      try {
        if(ctx != null && ctx.channel().isOpen()) {
          ctx.close().sync();
        }
      } catch(Exception e2) {
        LOG.error("Failed closing ctx, " + e2.getMessage());
      }
      throw e;
    }
    
    BlockingRpcChannel c = createBlockingRpcChannel();
    stub =  RaftService.newBlockingStub(c);
    
    connected = true;
    return connected;
  }
  
  public ServerInfo getRemoteServer() {
    return remoteServer;
  }
  
  public synchronized void close() {
    try {
      ctx.close().sync();
      connected = false;
    } catch(Exception e) {
      LOG.error("Closing failed", e);
    }
  }
  
  public BlockingInterface getStub() throws Exception {
    if(!isConnected()) {
        if(!connect()) {
          return null;
        }
    }
    return stub;
  }
  
  private ChannelHandlerContext connectRemoteServer() throws InterruptedException  {
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    
    try {
      ClientChannelHandler channelHandler = new ClientChannelHandler(listener);
      Bootstrap b = new Bootstrap(); 
      b.group(workerGroup); 
      b.channel(NioSocketChannel.class); 
      b.option(ChannelOption.SO_KEEPALIVE, true); 
      b.handler(channelHandler);

      ChannelFuture f = b.connect(getRemoteServer().getHost(), getRemoteServer().getPort()).sync(); 
      LOG.debug("connected to: " + this.getRemoteServer() );
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
 
  private  BlockingRpcChannel createBlockingRpcChannel() {
    return new BlockingRpcChannelImplementation();
  }

  class BlockingRpcChannelImplementation implements BlockingRpcChannel {
    @Override
    public Message callBlockingMethod(MethodDescriptor md, RpcController controller,
                                      Message request, Message returnType) throws ServiceException {

      Message response = null;
      int callId = generateCallId();
      try {
        RequestHeader.Builder builder = RequestHeader.newBuilder();
        builder.setId(callId); 
        builder.setRequestName(md.getName());
        RequestHeader header = builder.build();
        
        LOG.debug("SENDING RPC, CALLID:" + header.getId());
        RpcCall call = new RpcCall(callId, header, request, md);
        long tm = System.currentTimeMillis();
        
        ctx.writeAndFlush(call);
        
        RpcCall result = responsesMap.take(callId, rpcTimeout);
        
        response = result != null? result.getMessage() : null;
        if(response != null) {
          LOG.debug("response taken: " + callId);
          LOG.debug(String.format("RPC[%d] round trip takes %d ms", header.getId(), (System.currentTimeMillis() - tm)));
        }
      } catch(RpcTimeoutException e) {
        LOG.error("Rpc Timeout, call ID:" + callId + ", remote server:" + getRemoteServer());
        LOG.error("Rpc Timeout, call:" + request);
        LOG.error("Rpc Timeout", e);
        ServiceException se = new ServiceException(e.getMessage(), e);
        throw se;
      } catch(Exception e) {
        LOG.error("ctx:" + ctx);
        LOG.error("callBlockingMethod exception", e);
        throw e;
      }
      return response;
    }
  }
  
 
  class RpcClientEventListenerImpl implements RpcClientEventListener {
    @Override
    public void channelClosed() {
      ctx.close();
      connected = false;
    }
    @Override
    public void onRpcResponse(RpcCall call) {
      Preconditions.checkNotNull(call);
      responsesMap.put(call.getCallId(), call);      
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
  
  public void testRpc(int packetSize) throws Exception {
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
    
    if(!this.isConnected()) {
      try {
        if(!connect()) {
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
