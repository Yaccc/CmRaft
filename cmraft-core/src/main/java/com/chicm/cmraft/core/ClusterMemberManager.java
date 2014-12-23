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

package com.chicm.cmraft.core;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;
import com.google.common.base.Preconditions;

public class ClusterMemberManager {
  static final Log LOG = LogFactory.getLog(ClusterMemberManager.class);
  private static final String RAFT_SERVER_KEY_PREFIX = "raft.server";
  private static final String RAFT_LOCAL_SERVER_KEY = "raft.local.server";
  
  private Set<ServerInfo> remoteServers = new HashSet<ServerInfo>();
  private ServerInfo localServer;
  private Set<String> localAddresses = new HashSet<String>();
  
  public ClusterMemberManager(Configuration conf) {
    initLocalAddresses();
    LOG.debug("local addresses: " + localAddresses.toString());
    loadServerInfoFromConf(conf);
  }
  
  public ServerInfo getLocalServer() {
    return localServer;
  }
  
  public Set<ServerInfo> getRemoteServers() {
    return remoteServers;
  }
  
  // For Raft client to iterate all raft servers
  public static Set<ServerInfo> getRaftServers(Configuration conf) {
    Set<ServerInfo> servers = new HashSet<ServerInfo>();
    
    for (String key: conf.getKeys(RAFT_SERVER_KEY_PREFIX)) {
      ServerInfo server = ServerInfo.parseFromString(conf.getString(key));
      servers.add(server);
    }
    ServerInfo localServer = ServerInfo.parseFromString(conf.getString(RAFT_LOCAL_SERVER_KEY));
    if(localServer != null) {
      servers.add(localServer);
    }
    return servers;
  }
  
  private void loadServerInfoFromConf(Configuration conf) {
    List<ServerInfo> configuredServers = new ArrayList<ServerInfo>();
    int nLocalServers = 0;
    ServerInfo lastLocalServer = null;
    
    for (String key: conf.getKeys(RAFT_SERVER_KEY_PREFIX)) {
      ServerInfo server = ServerInfo.parseFromString(conf.getString(key));
      
      if(configuredServers.contains(server)) {
        String msg = "Duplicated server info in configuration:" + server;
        LOG.error(msg, new RuntimeException(msg));
      }
      
      configuredServers.add(server);
      if(isLocalAddress(server.getHost())) {
        nLocalServers ++;
        lastLocalServer = server;
      }
    }
    
    ServerInfo configuredLocalServer = ServerInfo.parseFromString(conf.getString(RAFT_LOCAL_SERVER_KEY));
    
    if(nLocalServers == 1) {
      localServer = configuredLocalServer == null ? lastLocalServer: configuredLocalServer;
    } else if(nLocalServers == 0 || nLocalServers > 1) {
      if(configuredLocalServer == null) {
        String msg = "Local host is not configured as a raft server, please check cmraft.properties configuration file";
        RuntimeException e = new RuntimeException(msg);
        LOG.error("Configuration error", e);
        throw e;
      } else {
        localServer = configuredLocalServer;
      }
    } else {
      // nLocalServers < 0, should never be here.
      LOG.error("SHOULD NOT BE HERE.");
      return;
    }
    remoteServers.addAll(configuredServers);
    remoteServers.remove(localServer);
  }

  public boolean isLocalAddress(String hostAddress) {
    Preconditions.checkNotNull(hostAddress);
    
    return localAddresses.contains(hostAddress);
  }
  
  private void initLocalAddresses() {
    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      while(interfaces.hasMoreElements()) {
          NetworkInterface inet = (NetworkInterface) interfaces.nextElement();
          Enumeration<InetAddress> addrs = inet.getInetAddresses();
          while (addrs.hasMoreElements()) {
              InetAddress addr = (InetAddress) addrs.nextElement();
              LOG.debug("local address:" + addr.getHostAddress());
              LOG.debug("local address:" + addr.getHostName());
              if(!addr.getHostAddress().isEmpty()) {
                localAddresses.add(addr.getHostAddress());
              }
              if(!addr.getHostName().isEmpty()) {
                localAddresses.add(addr.getHostName());
              }
          }
      }
      
      InetAddress inetAddress = InetAddress.getLocalHost();
      localAddresses.add(inetAddress.getHostAddress());
      localAddresses.add(inetAddress.getCanonicalHostName()); 
      localAddresses.add(inetAddress.getHostName()); 
      
      inetAddress = InetAddress.getLoopbackAddress();
      localAddresses.add(inetAddress.getHostAddress());
      localAddresses.add(inetAddress.getCanonicalHostName()); 
      localAddresses.add(inetAddress.getHostName()); 
    } catch(SocketException | UnknownHostException e ) {
      LOG.error("", e);
    }
  }
}
