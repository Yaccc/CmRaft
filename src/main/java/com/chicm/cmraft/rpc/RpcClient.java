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
import java.nio.channels.ReadPendingException;
import java.nio.channels.WritePendingException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.core.RaftRpcService;
import com.chicm.cmraft.log.LogEntry;
import com.chicm.cmraft.protobuf.generated.RaftProtos.AppendEntriesRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.AppendEntriesResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.CollectVoteResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService.BlockingInterface;
import com.chicm.cmraft.protobuf.generated.RaftProtos.TestRpcRequest;
import com.chicm.cmraft.util.BlockingHashMap;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ByteString;
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
  private final static int DEFAULT_RPC_TIMEOUT = 2000;
  private Configuration conf = null;
  private static final int DEFAULT_SOCKET_READ_WORKS = 1;
  private static BlockingService service = null;
  private static volatile AtomicInteger client_call_id = new AtomicInteger(0);
  private RpcSendQueue sendQueue = null;
  private BlockingInterface stub = null;
  private AsynchronousSocketChannel socketChannel = null;
  private BlockingHashMap<Integer, RpcCall> responsesMap = new BlockingHashMap<>();
  private ExecutorService socketExecutor = null;
  private InetSocketAddress serverIsa = null;
  private volatile boolean initialized = false;
  private int rpcTimeout;
  
  
  public RpcClient(Configuration conf, String serverHost, int serverPort) {
    this.conf = conf;
    rpcTimeout = conf.getInt("rpc.call.timeout", DEFAULT_RPC_TIMEOUT);
    this.serverIsa = new InetSocketAddress(serverHost, serverPort);
  }
  
  private boolean isInitialized() {
    return initialized;
  }
  
  private synchronized boolean init() {
    if(isInitialized())
      return true;
    
    service = (RaftRpcService.create()).getService();
    socketChannel = openConnection(serverIsa);
    sendQueue = new RpcSendQueue(socketChannel); 
    
    BlockingRpcChannel c = createBlockingRpcChannel(sendQueue, responsesMap);
    stub =  RaftService.newBlockingStub(c);
    
    socketExecutor = Executors.newFixedThreadPool(DEFAULT_SOCKET_READ_WORKS,
      new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("RPCClient-Socket-Worker-" + System.currentTimeMillis());
        return t;
      }
    });
    
    for(int i = 0; i < DEFAULT_SOCKET_READ_WORKS; i++ ) {
      Thread thread = new Thread(new SocketReader(socketChannel, service, responsesMap));
      thread.setDaemon(true);
      socketExecutor.execute(thread);
    }
    initialized = true;
    return initialized;
  }
  
  public BlockingInterface getStub() {
    if(!isInitialized())
      init();
    return stub;
  }
 
  public void testRpc() throws ServiceException {
    TestRpcRequest.Builder builder = TestRpcRequest.newBuilder();
    byte[] bytes = new byte[1024];
    builder.setData(ByteString.copyFrom(bytes));
    
    getStub().testRpc(null, builder.build());
  }
  
  public CollectVoteResponse collectVote(ServerInfo candidate, long term, long lastLogIndex,
      long lastLogTerm) throws ServiceException  {
    ServerId.Builder sbuilder = ServerId.newBuilder();
    sbuilder.setHostName(candidate.getHost());
    sbuilder.setPort(candidate.getPort());
    sbuilder.setStartCode(candidate.getStartCode());
    
    CollectVoteRequest.Builder builder = CollectVoteRequest.newBuilder();
    builder.setCandidateId(sbuilder.build());
    builder.setTerm(term);
    builder.setLastLogIndex(lastLogIndex);
    builder.setLastLogTerm(lastLogTerm);
    
    return (CollectVoteResponse)(getStub().collectVote(null, builder.build()));
  }
  
  public void testHeartBeat() {
    try {
      appendEntries(0, new ServerInfo("aaa", 111), 0, 0, 0, null);
    } catch(Exception e ) {
      LOG.error("testHeartBeat", e);
    }
  }
  
  public AppendEntriesResponse appendEntries(long term, ServerInfo leaderId, long leaderCommit,
      long prevLogIndex, long prevLogTerm, LogEntry[] entries) throws ServiceException {

    AppendEntriesRequest.Builder builder = AppendEntriesRequest.newBuilder();
    builder.setTerm(term);
    builder.setLeaderId(leaderId.toServerId());
    builder.setLeaderCommit(leaderCommit);
    builder.setPrevLogIndex(prevLogIndex);
    builder.setPrevLogTerm(prevLogTerm);
    if(entries != null) {
      for(int i = 0; i< entries.length; i++) {
        builder.setEntries(i, entries[i].toRaftEntry());
      }
    }
    LOG.info("RPCClient: making appendEntries call");
    AppendEntriesResponse response = getStub().appendEntries(null, builder.build());
    
    return response;
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
 
  private  BlockingRpcChannel createBlockingRpcChannel(RpcSendQueue sendQueue, 
      BlockingHashMap<Integer, RpcCall> responseMap) {
    return new BlockingRpcChannelImplementation(sendQueue, responseMap);
  }

  private  class BlockingRpcChannelImplementation implements BlockingRpcChannel {
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
        
        LOG.info("SENDING RPC, CALLID:" + header.getId());
        RpcCall call = new RpcCall(callId, header, request, md);
        long tm = System.currentTimeMillis();
        this.sendQueue.put(call);
        response = this.responseMap.take(callId, rpcTimeout).getMessage();
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
          break;
        } catch(ReadPendingException e) {
          LOG.error("retry", e);
          try {
            Thread.sleep(1);
          } catch(Exception e2) {
            
          }
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
      final RpcClient client = new RpcClient(CmRaftConfiguration.create(), host, port);
      
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
    
    if(!this.isInitialized()) {
      init();
    }
    
    LOG.info("client thread started");
    try {
      for(int i = 0; i < 5000000 ;i++) {
        startTime.set(System.currentTimeMillis());
        testRpc();
        //testHeartBeat();
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
