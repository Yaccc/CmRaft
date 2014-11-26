/*
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

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService.BlockingInterface;
import com.google.protobuf.BlockingRpcChannel;

public class ClientServicePool {
  static final Log LOG = LogFactory.getLog(ClientServicePool.class);
  private List<BlockingInterface> services = null;
  private int maxSize = 10;
  private int initialSize = 5;
  private int used = 0;
  private InetSocketAddress isa = null;
  
  private ClientServicePool(InetSocketAddress isa, int initialSize, int maxSize) {
    this.initialSize = initialSize;
    this.maxSize = maxSize; 
    this.isa = isa;
    services = new ArrayList<BlockingInterface>();
  }
  
  public static ClientServicePool createClientServicePool(InetSocketAddress isa, int initialSize, int maxSize) {
    ClientServicePool pool = new ClientServicePool(isa, initialSize, maxSize);
    for(int i = 0; i < initialSize; i++) {
      pool.createService();
    }
    return pool;
  }
  
  public synchronized BlockingInterface getService () {
    int size = services.size();
    if(size == 0 && used >= initialSize && used < maxSize) {
      createService();
    }
    
    if(services != null && size > 0) {
      used ++;
      return services.remove(size-1);
    }
    return null;
  }
  
  private void freeService(BlockingInterface service) {
    if(service != null) {
      services.add(service);
      used--;
    }
  }
  
  private BlockingInterface createService() {
    BlockingInterface service = null;
    try (SocketChannel channel = SocketChannel.open()) {
      
      if(channel.connect(isa)) {
        LOG.debug("client: connected");
      } else
        LOG.debug("client: connection failed");
      
      //BlockingRpcChannel c = RpcClient.createBlockingRpcChannel(channel);
      //service =  RaftService.newBlockingStub(c);
      
      //services.add(service);
    
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
    return service;
  }
  
  /*
  class ConnectionImpl implements Connection {
    private SocketChannel channel = null;
    private ClientServicePool pool = null;
    
    public ConnectionImpl(SocketChannel ch, ClientServicePool pool) {
      setChannel(ch);
      this.pool = pool;
    }
    
    @Override
    public SocketChannel getChannel() {
      return channel;
    }
    
    @Override
    public void setChannel(SocketChannel channel) {
      this.channel = channel;
    }
    
    @Override
    public void close() {
      pool.freeConnection(this);
    }
  }*/

}
