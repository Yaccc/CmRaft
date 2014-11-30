package com.chicm.cmraft.core;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestNodeTimeoutThread {
  private static NodeTimeoutThread threads[] = null;
  
  @BeforeClass
  public static void init() {
    threads = new NodeTimeoutThread[10];
    for(int i = 0; i < 10; i++) {
      threads[i] = new NodeTimeoutThread();
    }
  }
  
  @Test
  public void test() throws Exception {
    for(int i = 0; i < 5; i++) {
       threads[0].start("start0" + i, 1000, null);
       threads[1].start("start1"+ i, 1000, null);
       
       //Thread.sleep(5000);
       
       threads[0].stop();
       threads[1].stop();
    }
  }
}
