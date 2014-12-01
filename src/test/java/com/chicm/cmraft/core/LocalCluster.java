package com.chicm.cmraft.core;

import org.junit.Test;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;

import static org.junit.Assert.*;

public class LocalCluster {
  
  private static final int NODE_NUMBER = 3;
  private static final int START_PORT = 12888;
  
  private int nodeNumber = NODE_NUMBER;
  private int startPort = START_PORT;
  
  private Configuration[] confs;

  public LocalCluster() {
  }
  
  public Configuration getConf(int index) {
    return confs[index];
  }
  
  public void checkNodesState(RaftNode[] nodes) {
    int nLeader = 0;
    int nFollower = 0;
    
    for (int i =0; i < nodeNumber; i++) {
      if(nodes[i].getState() == State.LEADER) {
        nLeader++;
      } else if(nodes[i].getState() == State.FOLLOWER) {
        nFollower++;
      }
    }
    assertTrue(nLeader == 1);
    assertTrue(nFollower == (nodeNumber -1));
  }
  
  public void checkGetCurrentLeader(RaftNode[] nodes) {

    for (int i =0; i < nodeNumber; i++) {
      if(i != 0) {
        assertTrue(nodes[i].getCurrentLeader().equals(nodes[i-1].getCurrentLeader()));
      }
    }
  }
  
  @Test
  public void testCluster() throws Exception {
    RaftNode[] nodes = createCluster(8, 12888);
    Thread.sleep(10000);
    
    checkNodesState(nodes);
    
    for(RaftNode node: nodes) {
      System.out.println(node.getName() + ":" + node.getState() + ":" + node.getCurrentTerm());
      if(node.isLeader()) {
        System.out.println(node.getServerInfo() + " is leader, killing it");
        node.kill();
      }
    }
    for(int i=0; i < 5; i++) {
      Thread.sleep(8000);
      checkNodesState(nodes);
      checkGetCurrentLeader(nodes);
    }
  }
  
  public RaftNode[] createCluster(int n, int startPort) {
    this.nodeNumber = n;
    this.startPort = startPort;
    
    createConfiguration();
    
    RaftNode[] nodes = new RaftNode[nodeNumber];
    for(int i = 0; i < nodeNumber; i++) {
      nodes[i] = new RaftNode(confs[i]);
    }
    return nodes;
  }
  
  private void createConfiguration() {
    confs = new Configuration[nodeNumber];
    for(int i = 0; i < nodeNumber; i++) {
      confs[i] = CmRaftConfiguration.create();
      confs[i].useResource("cmraft_cluster_test.properties");
      for(int j=0; j < nodeNumber;j++) {
        confs[i].set("raft.server.remote." + j, "localhost:" + (startPort+j));
      }
      confs[i].remove("raft.server.remote." + i);
      confs[i].set("raft.server.local", "localhost:" + (startPort+i));
      
      System.out.println("confs[" + i + "]:\n" + confs[i].toString());
    }
  }
  
  public static void main(String[] args) throws Exception {
    LocalCluster clu = new LocalCluster();
    
    RaftNode[] nodes = clu.createCluster(8, 12888);
    
    Thread.sleep(10000);
    for(RaftNode node: nodes) {
      System.out.println(node.getName() + ":" + node.getState() + ":" + node.getCurrentTerm());
      if(node.isLeader()) {
        System.out.println(node.getServerInfo() + " is leader, killing it");
        //node.kill();
      }
    }
  }
}
