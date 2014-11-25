package com.chicm.cmraft.util;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class BlockingHashMap2 <K, V> {
  static final Log LOG = LogFactory.getLog(BlockingHashMap2.class);
  private static int QUEUE_SIZE = 5;  
  private ConcurrentHashMap<K, BlockingQueue<V>> map = new ConcurrentHashMap<>();
  
  //private Object lock = new Object();

  public V put(K key, V value) throws InterruptedException {
    BlockingQueue<V> q = map.get(key);
    if (q == null) {
      q = new ArrayBlockingQueue<V>(QUEUE_SIZE);
      map.put(key, q);
    }
    q.put(value);
    return value;
  }
  
  public void remove(K key) {
    map.remove(key);
  }
  
  public V take(K key) throws InterruptedException {
    BlockingQueue<V> q = map.get(key);
    if (q == null) {
      q = new ArrayBlockingQueue<V>(QUEUE_SIZE);
      map.put(key, q);
    }
    return q.take();
  }
  
  public int size() {
    return map.size();
  }
  
  public boolean isEmpty() {
    return map.isEmpty();
  }
  
  public Set<K> keySet() {
    return map.keySet();
  }
  
  
}
