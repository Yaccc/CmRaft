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

package com.chicm.cmraft.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;

public class ServerInfo {
  static final Log LOG = LogFactory.getLog(ServerInfo.class);
  private final String host;
  private final int port;
  
  public ServerInfo(String host, int port) {
    this.host = host;
    this.port = port;
  }
  
  /**
   * @return the host
   */
  public String getHost() {
    return host;
  }

  /**
   * @return the port
   */
  public int getPort() {
    return port;
  }
  
  public static ServerInfo parseFromString(String hostAddress) {
    if(hostAddress == null)
      return null;
    String[] results = hostAddress.split(":");
    if(results == null || results.length != 2)
      return null;
    int port;
    try {
      port = Integer.parseInt(results[1]);
    } catch(Exception e) {
      LOG.error("parse port form configuration exception", e);
      return null;
    }
    return new ServerInfo(results[0], port);
  }
  /*
  public static List<ServerInfo> getRemoteServersFromConfiguration(Configuration conf) {
    List<ServerInfo> servers = new ArrayList<ServerInfo>();
    for (String key: conf.getKeys("raft.server.remote")) {
      ServerInfo server = ServerInfo.parseFromString(conf.getString(key));
      servers.add(server);
    }
    return servers;
  }*/
  
  public ServerId toServerId() {
    ServerId.Builder builder = ServerId.newBuilder();
    if(getHost() != null)
      builder.setHostName(getHost());
    builder.setPort(getPort());
    
    return builder.build();
  }
  
  public static ServerInfo copyFrom(ServerId serverId) {
    if(serverId == null)
      return null;
    ServerInfo server = new ServerInfo(serverId.getHostName(), serverId.getPort());
    return server;
  }
  
  @Override
  public String toString() {
    return String.format("[%s:%d]", getHost(), getPort());
  }

  
  private int memoizedHashCode = 0;
  
  @Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getPort();
    if(getHost()!=null) {
      hash = (53 * hash) + getHost().hashCode();
    }
    memoizedHashCode = hash;
    return hash;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if(obj == null) {
      LOG.debug("equals (null)");
      return false;
    }
    if (!(obj instanceof ServerInfo)) {
       return super.equals(obj);
    }
    ServerInfo other = (ServerInfo)obj;
    boolean result = true;
    result = result && ((getHost()!=null) == (other.getHost()!=null));
    if (getHost()!=null) {
      result = result && getHost()
          .equals(other.getHost());
    }
    
    result = result && (getPort()==other.getPort());
    
    return result;
  }
}
