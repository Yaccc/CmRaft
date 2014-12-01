package com.chicm.cmraft.core;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;

public class LocalCluster {
  
  private static final int NODE_NUMBER = 3;
  private static final int START_PORT = 12888;
  
  private int nodeNumber = NODE_NUMBER;
  private int startPort = START_PORT;

  public LocalCluster(int n, int startPort) {
    this.nodeNumber = n;
    this.startPort = startPort;
  }
  
  public static void main(String[] args) throws Exception {
    LocalCluster clu = new LocalCluster(8, 12888);
    
    RaftNode[] nodes = clu.createCluster();
    
    Thread.sleep(10000);
    for(RaftNode node: nodes) {
      System.out.println(node.getName() + ":" + node.getState());
      if(node.isLeader()) {
        System.out.println(node.getServerInfo() + " is leader, killing it");
        node.kill();
      }
    }
    /*
    while(true) {
      Thread.sleep(5000);
      System.out.println("**************************************");
      for(RaftNode node: nodes) {
        System.out.println(node.getName() + ":" + node.getState());
        if(node.isLeader()) {
          System.out.println(node.getServerInfo() + " is leader, killing it");
          node.kill();
        }
      }
    }*/
  }
  
  public RaftNode[] createCluster() {
    Configuration[] confs = createConfiguration();
    
    RaftNode[] nodes = new RaftNode[nodeNumber];
    for(int i = 0; i < nodeNumber; i++) {
      nodes[i] = new RaftNode(confs[i]);
    }
    return nodes;
  }
  
  public Configuration[] createConfiguration() {
    Configuration[] confs = new Configuration[nodeNumber];
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
    return confs;
  }

}
