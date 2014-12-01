package com.chicm.cmraft;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class Result {
  private Map<byte[], byte[]> map = new HashMap<>();
  
  public void put(byte[] key, byte[] value) {
    map.put(key, value);
  }
  
  public byte[] get(byte[] key) {
    return map.get(key); 
  }
  
  public Set<byte[]> keySet() {
    return map.keySet();
  }

}
