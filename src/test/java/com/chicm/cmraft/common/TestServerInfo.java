package com.chicm.cmraft.common;

import org.junit.Test;

public class TestServerInfo {

  @Test
  public void testServerId() {
    Configuration conf = CmRaftConfiguration.create();
    conf.set("raft.server.local", "chicm:5555");
    conf.set("raft.server.remote.1", "chicm:1111");
    conf.set("raft.server.remote.2", "chicm:2222");
    conf.set("raft.server.remote.3", "chicm:3333");
    
    
    String thisServerId = conf.getString("raft.server");
    System.out.println(thisServerId);
    ServerInfo thisServer = ServerInfo.parseFromString(conf.getString("raft.server." + thisServerId));
    for (String key: conf.getKeys("raft.server.remote")) {
      System.out.println(key);
      ServerInfo server = ServerInfo.parseFromString(conf.getString(key));
      System.out.println(server);
      /*if(!server.equals(thisServer)) {
        knownServers.add(server);
      }*/
    }
  }
}
