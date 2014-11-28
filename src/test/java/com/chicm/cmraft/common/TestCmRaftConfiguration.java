package com.chicm.cmraft.common;

import org.junit.Test;

public class TestCmRaftConfiguration {

  @Test
  public void testCmRaftConfiguration() {
    Configuration conf = CmRaftConfiguration.create();
    conf.set("aaa", "123a");
    conf.set("aaa", "123");
    for(String key: conf.getKeys()) {
      System.out.println(key +": " +  conf.getString(key));
    }
    System.out.println("**************");
    for(String key: conf.getKeys("raft.server")) {
      System.out.println(key +": " +  conf.getString(key));
    }
  }
}
