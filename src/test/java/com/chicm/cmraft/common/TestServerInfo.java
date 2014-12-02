package com.chicm.cmraft.common;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;

public class TestServerInfo {
  private static Configuration conf = CmRaftConfiguration.create();
  
  @BeforeClass
  public static void init() {
    conf.set("raft.server.local", "chicm:5555");
    conf.set("raft.server.remote.1", "chicm:1111");
    conf.set("raft.server.remote.2", "chicm:2222");
    conf.set("raft.server.remote.3", "chicm:3333");
  }

  @Test
  public void testServerId() {
    ServerInfo thisServer = ServerInfo.parseFromString(conf.getString("raft.server.local"));
    for (String key: conf.getKeys("raft.server.remote")) {
      System.out.println(key);
      ServerInfo server = ServerInfo.parseFromString(conf.getString(key));
      System.out.println(server);
    }
    ServerId sid = thisServer.toServerId();
    System.out.println(sid);
  }
  
  @Test 
  public void testGetRemoteServersFromConfiguratoin() {
    List<ServerInfo> list = ServerInfo.getRemoteServersFromConfiguration(conf);
    System.out.println("testGetRemoteServersFromConfiguratoin");
    for(ServerInfo s:list) {
      System.out.println(s);
    }
    assertTrue(list.contains(new ServerInfo("chicm", 1111)));
    assertTrue(list.contains(new ServerInfo("chicm", 2222)));
    assertTrue(list.contains(new ServerInfo("chicm", 3333)));
    assertFalse(list.contains(new ServerInfo("chicm", 5555)));
    assertFalse(list.contains(new ServerInfo("chicm", 4444)));
  }
}
