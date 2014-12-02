package com.chicm.cmraft;

import org.apache.log4j.Level;
import org.junit.Test;

import com.chicm.cmraft.core.LocalCluster;
import com.chicm.cmraft.core.RaftNode;

public class TestConnectionManager {
  
  public static void main(String[] args) throws Exception {
    TestConnectionManager t = new TestConnectionManager();
    t.testGetConnection();
  }

  @Test
  public void testGetConnection() throws Exception {
    org.apache.log4j.LogManager.getRootLogger().setLevel(Level.WARN);
    LocalCluster cluster = new LocalCluster();
    RaftNode[] nodes = cluster.createCluster(3, 12888);
    
    Thread.sleep(10000);
    
    Connection conn = ConnectionManager.getConnection(cluster.getConf(0));
    conn.set("key1", "value1");
    conn.set("key2", "value2");
    conn.set("key3", "value3");
    
    Result r = conn.list("");
    for(byte[] b: r.keySet()) {
      System.out.println(new String(b));
    }
    
    for(int i = 0; i < nodes.length; i++) {
      if(nodes[i].isLeader()) {
        System.out.println("KILLING leader");
        nodes[i].kill();
      }
    }
    
    Thread.sleep(10000);
    
    Connection conn2 = ConnectionManager.getConnection(cluster.getConf(0));
    Result r2 = conn.list("");
    for(byte[] b: r2.keySet()) {
      System.out.println(new String(b));
    }
  }
}
