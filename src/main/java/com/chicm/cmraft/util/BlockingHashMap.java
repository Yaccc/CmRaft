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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BlockingHashMap <K,V> {
  static final Log LOG = LogFactory.getLog(BlockingHashMap.class);
  
  private ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
  private ConcurrentHashMap<K, KeyLock> locks = new ConcurrentHashMap<>();
  
  private void signalKeyArrive(K key) {
    final KeyLock lock = locks.get(key);
    if(lock == null)
      return;
    
    lock.lock();
    try {
      LOG.debug("singal key:[" + key + "] arrive");
      lock.signal();
    } finally {
      lock.unlock();
    }
  }
  
  public V put(K key, V value) {
    V ret = map.put(key, value);
    signalKeyArrive(key);
    return ret;
  }
  
  private V remove(K key) {
    locks.remove(key);
    return map.remove(key);
  }
  
  public V take(K key) {
    return take(key, 0);
  }
  
  public V take(K key, int timeout) {
    V ret = null;
    KeyLock lock = locks.get(key);
    if(lock == null) {
      lock = new KeyLock();
      locks.put(key, lock);
    }
    try {
      lock.lock();
      ret = map.get(key);
      if(ret != null) {
        remove(key);
        return ret;
      }
      
      while(ret == null) {
        if(timeout==0) {
          lock.await();
        } else {
          lock.await(timeout);
        }
        ret = map.get(key);
        if(ret == null) {
          LOG.error("RPC TIMEOUT! KEY=" + key);
        }
      }
      LOG.debug("wait done: " + key + ": " + ret);
    } catch (InterruptedException ex) {
      LOG.error("InterruptedException", ex);
      return ret;
    }
    finally {
        lock.unlock();
    }
    if(key != null) {
      remove(key);
    }
    return ret;
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
  
  public Collection<V> values() {
    return map.values();
  }
  
  class KeyLock {
    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();
    
    public ReentrantLock getLock() {
      return lock;
    }
    
    public Condition getCondition () {
      return condition;
    }
    
    public void lock() {
      lock.lock();
    }
    
    public void unlock() {
      lock.unlock();
    }
    
    public void signal() {
      condition.signal();
    }
    
    public void await() throws InterruptedException {
      condition.await();
    }
    public void await(int timeout) throws InterruptedException {
      condition.await(timeout, TimeUnit.MILLISECONDS);
    }
  }

}
