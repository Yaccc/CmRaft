package com.chicm.cmraft.core;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestTimeoutWorker {
  private static TimeoutWorker threads[] = null;
  
  @BeforeClass
  public static void init() {
    threads = new TimeoutWorker[10];
    for(int i = 0; i < 10; i++) {
      threads[i] = new TimeoutWorker();
    }
  }
  
  @Test
  public void test() throws Exception {
    for(int i = 0; i < 5; i++) {
       threads[0].start("start0" + i, 1000, null);
       threads[1].start("start1"+ i, 1000, null);
       
       Thread.sleep(3000);
       
       threads[0].stop();
       threads[1].stop();
    }
  }
  
  @Test
  public void test2() throws Exception {
    TimeoutWorker p = new TimeoutWorker();
    p.start("test", 5000, null);
    Thread.sleep(6000);
    
    for(int i = 0; i<7 ; i++) {
      
      p.reset();
      Thread.sleep(3000);
      if(i == 5) {
        p.stop();
        break;
      }
    }
    
    System.exit(0);
  }
}
