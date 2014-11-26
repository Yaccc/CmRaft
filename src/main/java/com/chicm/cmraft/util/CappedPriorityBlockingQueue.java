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

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * CappedPriorityBlockingQueue is a bounded priority blocking queue, while the size reach it's maximum
 * capacity, the put operation is blocked until there is space in the queue to place the element.
 * The PriorityBlockingQueue is an unbounded priority queue, for which the put operation never blocked.
 * @author chicm
 *
 * @param <E>
 */
public class CappedPriorityBlockingQueue<E> extends PriorityBlockingQueue<E>{
  static final Log LOG = LogFactory.getLog(CappedPriorityBlockingQueue.class);
  
  private  int maxCapacity = 1024;
  /** Lock held by put, offer, etc */
  private final ReentrantLock putLock = new ReentrantLock();
  /** Wait queue for waiting puts */
  private final Condition notFull = putLock.newCondition();
  
  public CappedPriorityBlockingQueue(int maxCapacity) {
    this.maxCapacity = maxCapacity;
  }
  
  /**
   * Signals a waiting put. Called only from take/poll.
   */
  private void signalNotFull() {
      final ReentrantLock putLock = this.putLock;
      putLock.lock();
      try {
        LOG.debug("singal not full");
        notFull.signal();
      } finally {
        putLock.unlock();
      }
  }
  @Override
  public void put(E e) {
    offer(e); 
  }
  
  @Override
  public boolean add(E e) {
    return offer(e);
  }
  
  @Override
  public boolean offer(E e) {
    boolean ret = false;
    final ReentrantLock putLock = this.putLock;
    try {
      putLock.lockInterruptibly();
        while (size() >= this.maxCapacity) {
          LOG.debug("Thread:" + Thread.currentThread().getName() + ": queue is full, waiting...");
          notFull.await();
          LOG.debug("Thread:" + Thread.currentThread().getName() + ": waiting done");
        }
        
        ret = super.offer(e);
        if (size() < this.maxCapacity)
            notFull.signal();
    } catch (InterruptedException ex) {
      LOG.error("InterruptedException", ex);
      return false;
    }
    finally {
        putLock.unlock();
    }
    return ret;
  }
  
  public E poll(long timeout, TimeUnit unit) throws InterruptedException {
    InterruptedException e = new InterruptedException("poll is not supported, please use take");
    LOG.error("poll is not supported, please use take", e);
    throw e;
  }
  
  public E take() throws InterruptedException {
    E e = super.take();
    if(size() < this.maxCapacity) {
      signalNotFull();
    }
    return e;
  }
}
