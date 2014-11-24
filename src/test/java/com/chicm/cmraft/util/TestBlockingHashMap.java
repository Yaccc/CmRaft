package com.chicm.cmraft.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TestBlockingHashMap {
  public final static int testnumber = 100000;
  private static BlockingHashMap<Integer, String> map = new BlockingHashMap<>();
  private static ConcurrentHashMap<Integer, String> result = new ConcurrentHashMap<>();
  
  static class PutThread implements Runnable {
    private int index = 0;
    public PutThread(int index) {
      this.index = index;
    }
    public void run() {
      for(int j = testnumber*index; j < testnumber*index+testnumber; j++) {
        map.put(j, String.format("VALUE%04d", j));
      }
    }
  }
  
  static class TakeThread implements Runnable {
    private int index = 0;
    public TakeThread(int index) {
      this.index = index;
    }
    public void run() {
      for(int j = testnumber*index; j < testnumber*index+testnumber; j++) {
        result.put(j, map.take(j));
      }
    }
  }
  
  public static void validateResult() {
    for (int i =0; i < 10*testnumber; i++) {
      String expected = String.format("VALUE%04d", i);
      String str = result.get(i);
      if(!expected.equals(str)) {
        System.out.printf("ERROR %d: expected: %s: resut: %s\n", i, expected, str);
      }
     }
    System.out.println("validate done");
  }
  
  public static void main(String[] args) {
    // TODO Auto-generated method stub
    //ExecutorService es1 = Executors.newFixedThreadPool(10);
    
    long tm = System.currentTimeMillis();
    
    ExecutorService es1 = Executors.newFixedThreadPool(10);
    for(int i = 0; i < 10; i++) {
      Thread t = new Thread(new PutThread(i));
      t.setDaemon(true);
      es1.execute(t);
    }
    ExecutorService es2 = Executors.newFixedThreadPool(10);
    for(int i = 0; i < 10; i++) {
      Thread t = new Thread(new TakeThread(i));
      
      es2.execute(t);
    }
    es2.shutdown();
    try {
    es2.awaitTermination(100, TimeUnit.DAYS);
    } catch(Exception e) {
      
    }
    System.out.println("done: " + (System.currentTimeMillis()-tm)/1000);
    
    validateResult();
    /*
    //for(int i = 0; i < 10; i++) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
         for(int j = 0; j < 1000000; j++) {
           map.put(j, String.format("VALUE%04d", j));
         }
        }
      });
      t.setDaemon(true);
      t.start();
    //}
    
    //ExecutorService es1 = Executors.newFixedThreadPool(10);
    for(int i = 0; i < 1000000; i++) {
      String s = map.take(i);
      
     // System.out.println("get:" + s);
    }*/
    
    //System.out.println("done: " + (System.currentTimeMillis()-tm)/1000);
  }
  
  @Test 
  public void test() {
    
  }

}
