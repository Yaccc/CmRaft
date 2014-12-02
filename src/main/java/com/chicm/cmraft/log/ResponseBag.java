package com.chicm.cmraft.log;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResponseBag<T> {
  private Map<T, Integer> map = new ConcurrentHashMap<T, Integer>();
  
  public synchronized void add(T key, int number) {
    int currentNumber = map.get(key) == null? 0: map.get(key);
    map.put(key, number+currentNumber);
  }
  
  public int get(T key) {
    return map.get(key) == null ? 0 : map.get(key);
  }
}
