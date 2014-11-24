package com.chicm.cmraft.util;

public class TestBlockingHashMap {
  private static BlockingHashMap<Integer, String> map = new BlockingHashMap<>();
  public static void main(String[] args) {
    // TODO Auto-generated method stub
    
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
       for(int i = 0; i < 1000; i++) {
         map.put(i, String.format("VALUE%04d", i));
       }
      }
    });
    t.setDaemon(true);
    t.start();
    
    for(int i = 0; i < 1000; i++) {
      String s = map.get(i);
      System.out.println("get:" + s);
    }
  }

}
