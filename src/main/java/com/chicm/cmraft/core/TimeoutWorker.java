/**
* Copyright 2014 The CmRaft Project
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

package com.chicm.cmraft.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A thread provides time out service, it calls listener's timeout() method on every timeout.
 * If the reset method is called before timeout, the timer will be restart.
 * @author chicm
 *
 */
public class TimeoutWorker implements Runnable {
  static final Log LOG = LogFactory.getLog(TimeoutWorker.class);
  private TimeoutListener listener = null;
  private int timeout = 0;
  private final Object sleepLock = new Object();
  private volatile boolean reset = false;
  private volatile boolean isStopped = false;
  private Thread thread = null;
  
  // It is not allowed to create new instance with new operator, can only be created
  // from create method.
  private TimeoutWorker() {
  }
  
  public static TimeoutWorker create(String name, int timeout, TimeoutListener listener) {
    TimeoutWorker worker = new TimeoutWorker();
    worker.setTimeout(timeout);
    worker.setListener(listener);
    
    worker.setThread(new Thread(worker));
    worker.getThread().setName(name);
    worker.getThread().start();
    return worker;
  }
  
  /**
   * @return the listener
   */
  private TimeoutListener getListener() {
    return listener;
  }

  /**
   * @param listener the listener to set
   */
  private void setListener(TimeoutListener listener) {
    this.listener = listener;
  }

  /**
   * @return the timeout
   */
  private int getTimeout() {
    return timeout;
  }

  /**
   * @param timeout the timeout to set
   */
  private void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  /**
   * @return the thread
   */
  private Thread getThread() {
    return thread;
  }

  /**
   * @param thread the thread to set
   */
  private void setThread(Thread thread) {
    this.thread = thread;
  }
  
  public void reset() {
    LOG.info(thread.getName() + " RESET");
    synchronized (sleepLock) {
      reset = true;
      sleepLock.notifyAll();
    }   
  }
  
  public void reset(int timeout) {
    setTimeout(timeout);
    reset();
  }
  
  public boolean isStopped() {
    return isStopped;
  }

  public void stop() {
    LOG.info(thread.getName() + " STOP");
    this.isStopped = true;
    reset();
    try {
      this.thread.join();
    } catch(Exception e) {
      LOG.error("exception", e);
    }
  }

  private void sleep() {
    LOG.debug("entering sleep");
    try {
      while(!isStopped()) {
        reset = false;
        synchronized (sleepLock) {
          sleepLock.wait(getTimeout());
        }
        if(reset)
          continue;
        break;
      }
    } catch(InterruptedException iex) {
      LOG.debug("interrupted");
    }
  }

  @Override
  public void run() {
    LOG.info(thread.getName() + " started, timeout=" + this.timeout);
    try {
      while (!isStopped()) {
        sleep();
        if(isStopped())
          break;
        try {
           doTimeOut();
        } catch (Exception e) {
          if (isStopped()) {
            break;
          }
        }        
      }
    } catch (Throwable t) {
      LOG.error("TimeoutWorker exception", t);
    } finally {
      LOG.info(thread.getName() + " STOPPED");
    }
  }

  private void doTimeOut() {
    LOG.info(thread.getName() + " TIMEOUT");
    if(listener != null) {
      getListener().timeout();
      LOG.info(thread.getName() + " listener timeout is called");
    } else {
      LOG.info(thread.getName() + " listener is null");
    }
  }
}
