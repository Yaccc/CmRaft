package com.chicm.cmraft.core;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
import com.chicm.cmraft.common.ServerInfo;

public class TestRpcClientManager {
  private static RpcClientManager mgr;
  
  @BeforeClass
  public static void init() {
    Configuration conf = CmRaftConfiguration.create();
    conf.set("raft.server.local", "chicm1:1111");
    conf.set("raft.server.remote.1", "chicm2:2222");
    conf.set("raft.server.remote.2", "chicm3:3333");
    
    mgr = new RpcClientManager(conf);
  }
  
  @Test
  public void testGetOtherServers() {
    assertFalse(mgr.getOtherServers().contains(new ServerInfo("chicm1", 1111)));
    assertTrue(mgr.getOtherServers().contains(new ServerInfo("chicm2", 2222)));
    assertTrue(mgr.getOtherServers().contains(new ServerInfo("chicm3", 3333)));
  }
  
  @Test
  public void testGetAllServers() {
    assertTrue(mgr.getAllServers().contains(new ServerInfo("chicm1", 1111)));
    assertTrue(mgr.getAllServers().contains(new ServerInfo("chicm2", 2222)));
    assertTrue(mgr.getAllServers().contains(new ServerInfo("chicm3", 3333)));
  }
}
