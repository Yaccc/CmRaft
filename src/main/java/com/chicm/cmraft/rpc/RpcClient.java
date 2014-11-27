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
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService.BlockingInterface;
import com.chicm.cmraft.util.BlockingHashMap;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.google.protobuf.Descriptors.MethodDescriptor;

/**
 * RpcClient implements the BlockingRpcChannel interface. It translate RPC method calls to 
 * RPC request packets and send them to RPC server. Then translate RPC response packets from
 * RPC server to returned objects for RPC method calls.
 * @author chicm
 *
 */
public class RpcClient {
  static final Log LOG = LogFactory.getLog(RpcClient.class);
  private static final int DEFAULT_SOCKET_READ_WORKS = 1;
  private static BlockingService service = null;
  private static volatile AtomicInteger client_call_id = new AtomicInteger(0);
  private RpcSendQueue sendQueue = null;
  private BlockingInterface stub = null;
  private AsynchronousSocketChannel socketChannel = null;
  private BlockingHashMap<Integer, RpcCall> responsesMap = new BlockingHashMap<>();
  private ExecutorService socketExecutor = null;
  
  
  public RpcClient(String host, int port) {
    
    InetSocketAddress isa = new InetSocketAddress(host, port);
    service = (new RaftRpcService()).getService();
    socketChannel = openConnection(isa);
    sendQueue = new RpcSendQueue(socketChannel); 
    
    BlockingRpcChannel c = RpcClient.createBlockingRpcChannel(sendQueue, responsesMap);
    stub =  RaftService.newBlockingStub(c);
    
    socketExecutor = Executors.newFixedThreadPool(DEFAULT_SOCKET_READ_WORKS);
    for(int i = 0; i < DEFAULT_SOCKET_READ_WORKS; i++ ) {
      Thread thread = new Thread(new SocketReader(socketChannel, service, responsesMap));
      thread.setDaemon(true);
      socketExecutor.execute(thread);
    }
  }
  
  private AsynchronousSocketChannel openConnection(InetSocketAddress isa) {
    AsynchronousSocketChannel channel = null;
    try  {
      LOG.info("opening connection to:" + isa);
      
      channel = AsynchronousSocketChannel.open();
      channel.connect(isa).get();
      
      LOG.info("client connected:" + isa);
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
    return channel;
  }
  
   
  public static int generateCallId() {
    return client_call_id.incrementAndGet();
  }
  
  public static int getCallId() {
    return client_call_id.get();
  }
 
  public static BlockingRpcChannel createBlockingRpcChannel(RpcSendQueue sendQueue, 
      BlockingHashMap<Integer, RpcCall> responseMap) {
    return new RpcClient.BlockingRpcChannelImplementation(sendQueue, responseMap);
  }

  public static class BlockingRpcChannelImplementation implements BlockingRpcChannel {
    private RpcSendQueue sendQueue = null;
    private BlockingHashMap<Integer, RpcCall> responseMap = null;

    protected BlockingRpcChannelImplementation(RpcSendQueue sendQueue, 
        BlockingHashMap<Integer, RpcCall> responseMap) {
      this.sendQueue = sendQueue;
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
        LOG.debug("sending, callid:" + header.getId());
        RpcCall call = new RpcCall(callId, header, request, md);
        long tm = System.currentTimeMillis();
        this.sendQueue.put(call);
        response = this.responseMap.take(callId).getMessage();
        LOG.debug("response taken: " + callId + " :" + response);
        LOG.debug(String.format("RPC[%d] round trip takes %d ms", header.getId(), (System.currentTimeMillis() - tm)));
      } catch(Exception e) {
        e.printStackTrace(System.out);
      }
      return response;
    }
  }
  
  class SocketReader implements Runnable {
    private AsynchronousSocketChannel channel;
    private BlockingService service;
    private BlockingHashMap<Integer, RpcCall> results;
    
    public SocketReader(AsynchronousSocketChannel channel, BlockingService service, BlockingHashMap<Integer, RpcCall> results) {
      this.channel = channel;
      this.service = service;
      this.results = results;
    }
    @Override
    public void run() {
      long starttime = System.currentTimeMillis();
      while(true) {
        try {
          RpcCall call = PacketUtils.parseRpcResponseFromChannel(channel, service);
          results.put(call.getCallId(), call);
          
          int id = call.getCallId();
          if(id % 1000 == 0) {
            long curtm = System.currentTimeMillis();
            long elipsetm = (curtm - starttime) /1000;
            if(elipsetm == 0)
              elipsetm =1;
            long tps = id / elipsetm;
            
            LOG.info("response id: " + id + " time: " + elipsetm + " TPS: " + tps);
          }
          LOG.debug("put response, call id: " + call.getCallId() + " result map size: " + results.size());
        } catch (InterruptedException | ExecutionException |IOException e) {
          LOG.error("exception", e);
          e.printStackTrace(System.out);
        }
      }
    }
  }
  
  /*
   * For testing purpose
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if(args.length < 3) {
      System.out.println("usage: RpcServer <server host> <server port> <clients number> <threads number> [padding length]");
      return;
    }
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    int nclients = Integer.parseInt(args[2]);
    int nThreads = Integer.parseInt(args[3]);
    

    if(args.length >= 5) {
      PacketUtils.TEST_PADDING_LEN = Integer.parseInt(args[4]);
    }
    for(int j =0; j < nclients; j++ ) {
      final RpcClient client = new RpcClient(host, port);
      
      for(int i = 0; i < nThreads; i++) {
        new Thread(new Runnable() {
          public void run() {
            client.sendRequest();
            
          }
        }).start();
      }
    }
  }
  
  private ThreadLocal<Long> startTime = new ThreadLocal<>();
  
  /*
   * For testing purpose
   */
  public void sendRequest() {
    ServerId.Builder sbuilder = ServerId.newBuilder();
    sbuilder.setHostName("localhost");
    sbuilder.setPort(11111);
    
    HeartBeatRequest.Builder builder = HeartBeatRequest.newBuilder();
    builder.setServer(sbuilder.build());
    
    LOG.info("client thread started");
    try {
      for(int i = 0; i < 5000000 ;i++) {
        startTime.set(System.currentTimeMillis());
        HeartBeatResponse r = stub.beatHeart(null, builder.build());
        
        if(i != 0 && i %1000 == 0 ) {
          long ms = System.currentTimeMillis() - startTime.get();
          LOG.info("RPC CALL[ " + i + "] round trip time: " + ms);
        }
      }
    } catch (Exception e) {
      e.printStackTrace(System.out);
    } 
  }
}
