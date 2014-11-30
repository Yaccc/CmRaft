package com.chicm.cmraft.common;

import org.junit.Test;

import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;

public class TestServerInfo {

  @Test
  public void testServerId() {
    Configuration conf = CmRaftConfiguration.create();
    conf.set("raft.server.local", "chicm:5555");
    conf.set("raft.server.remote.1", "chicm:1111");
    conf.set("raft.server.remote.2", "chicm:2222");
    conf.set("raft.server.remote.3", "chicm:3333");
   
    ServerInfo thisServer = ServerInfo.parseFromString(conf.getString("raft.server.local"));
    for (String key: conf.getKeys("raft.server.remote")) {
      System.out.println(key);
      ServerInfo server = ServerInfo.parseFromString(conf.getString(key));
      System.out.println(server);
      /*if(!server.equals(thisServer)) {
        knownServers.add(server);
      }*/
    }
    
    ServerId sid = thisServer.toServerId();
    System.out.println(sid);
  }
}
