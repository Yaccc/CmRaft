package com.chicm.cmraft.core;

import static org.junit.Assert.*;

import java.lang.reflect.Method;

import org.junit.BeforeClass;
import org.junit.Test;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;

public class TestRaftNode {
  
  private static RaftNode node1;
  private static Configuration conf1;
  private static RaftNode node2;
  
  public static void main(String[] args) throws Exception {
    init();
    Thread.sleep(100000);
  }
  
  @BeforeClass
  public static void init() {
    conf1 = CmRaftConfiguration.create();
    conf1.clear();
    conf1.set("raft.server.local", "localhost:12888");
    conf1.set("raft.server.remote.1", "localhost:13888");
    conf1.set("raft.election.timeout", "500");
    conf1.set("raft.heartbeat.interval", "500");
    
    Configuration conf2 = CmRaftConfiguration.create();
    conf2.clear();
    conf2.set("raft.server.local", "localhost:13888");
    conf2.set("raft.server.remote.1", "localhost:12888");
    conf2.set("raft.election.timeout", "500");
    
    node1 = new RaftNode(conf1);
    node2 = new RaftNode(conf2);
  }
  
  @Test
  public void testRaftNode() {
    for(int i =0; i< 1000; i++) { 
      node1.testHearBeat();
      node2.testHearBeat();
    }
  }
  
  @Test
  public void testGetElectionTimeout() throws Exception {
    int confTimeout = conf1.getInt("raft.election.timeout");
    Method m = node1.getClass().getDeclaredMethod("getElectionTimeout", new Class[] {});
    m.setAccessible(true);
    
    for(int i = 0; i < 100; i++) {
      Object result = m.invoke(node1, null);
      
      int resultInt = ((Integer)result).intValue();
      System.out.println(result);
      assertTrue(resultInt >= confTimeout);
      assertTrue(resultInt <= confTimeout*2);
    }
  }

}
