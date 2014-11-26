/**
* Copyright 2014 The Apache Software Foundation
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at:
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations
* under the License.
*/

package com.chicm.cmraft.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

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
      assertTrue(expected.equals(str));
    }
  }
  
  @Test
  public void testMultiThreaded() {
    
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
  }
  
  @Test
  public void testSingleThread() {
    
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
         for(int j = 0; j < 100000; j++) {
           map.put(j, String.format("VALUE%04d", j));
         }
        }
      });
      t.setDaemon(true);
      t.start();
    
    for(int i = 0; i < 100000; i++) {
      String s = map.take(i);
      assertTrue(s.equals(String.format("VALUE%04d", i)));
    }
    
  }
  
}
