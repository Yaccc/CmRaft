package com.chicm.cmraft.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

public class TestBlockingHashMap2 {
  private static BlockingHashMap2<Integer, String> map = new BlockingHashMap2<>();
  public static void main(String[] args) throws InterruptedException {
    // TODO Auto-generated method stub
    //ExecutorService es1 = Executors.newFixedThreadPool(10);
    
    long tm = System.currentTimeMillis();
    //for(int i = 0; i < 10; i++) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
         for(int j = 0; j < 1000000; j++) {
           try {
           map.put(j, String.format("VALUE%04d", j));
           } catch(Exception e) {
             e.printStackTrace(System.out);
           }
         }
        }
      });
      t.setDaemon(true);
      t.start();
    //}
    
    //ExecutorService es1 = Executors.newFixedThreadPool(10);
    for(int i = 0; i < 1000000; i++) {
      String s = map.take(i);
      
      System.out.println("get:" + s);
    }
    
    System.out.println("done: " + (System.currentTimeMillis()-tm)/1000);
  }
  
  @Test 
  public void test() {
    
  }

}
