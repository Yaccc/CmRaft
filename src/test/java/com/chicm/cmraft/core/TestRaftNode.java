package com.chicm.cmraft.core;

import org.junit.Test;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;

public class TestRaftNode {
  
  public static void main(String[] args) {
    TestRaftNode t = new TestRaftNode();
    t.testRaftNode();
  }
  
  @Test
  public void testRaftNode() {
    
    Configuration conf1 = CmRaftConfiguration.create();
    conf1.clear();
    conf1.set("raft.server.local", "localhost:12888");
    conf1.set("raft.server.remote.1", "localhost:13888");
    
    Configuration conf2 = CmRaftConfiguration.create();
    conf2.clear();
    conf2.set("raft.server.local", "localhost:13888");
    conf2.set("raft.server.remote.1", "localhost:12888");
    
    RaftNode node1 = new RaftNode(conf1);
    RaftNode node2 = new RaftNode(conf2);
    
    for(int i =0; i< 1000; i++) { 
    node1.testHearBeat();
    node2.testHearBeat();
    }
  }

}
