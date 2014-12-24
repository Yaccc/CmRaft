package com.chicm.cmraft.log;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.junit.Test;

import com.chicm.cmraft.core.LocalCluster;
import com.chicm.cmraft.core.RaftNode;


public class TestLogManager {
  static final Log LOG = LogFactory.getLog(TestLogManager.class);
  

  @Test
  public void test() throws Exception {
    org.apache.log4j.LogManager.getRootLogger().setLevel(Level.FATAL);
    RaftNode[] nodes =LocalCluster.create(3,3, 15888).getNodes();
    /*
    Thread.sleep(10000);
    for(int i = 0; i < nodes.length; i++) {
      if(nodes[i].isLeader()) {
        DefaultRaftLog mgr = nodes[i].getLogManager();
        mgr.set("11111".getBytes(), "VVVVV1".getBytes());
        mgr.set("11112".getBytes(), "VVVVV2".getBytes());
        mgr.set("11113".getBytes(), "VVVVV3".getBytes());
        
        Collection<LogEntry> col = mgr.list("".getBytes());
        for(LogEntry e:col) {
          System.out.println("key:" + new String(e.getKey()));
          System.out.println("value:" + new String(e.getValue()));
        }
       }
    }*/
  }
}
