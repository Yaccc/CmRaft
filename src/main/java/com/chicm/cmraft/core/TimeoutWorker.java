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

public class TimeoutWorker implements Runnable {
  static final Log LOG = LogFactory.getLog(TimeoutWorker.class);
  private RaftTimeoutListener listener = null;
  private int timeout = 0;
  private final Object sleepLock = new Object();
  private volatile boolean reset = false;
  private volatile boolean isStopped = false;
  private Thread thread = null;
  
  public void start(String name, int timeout, RaftTimeoutListener listener) {
    this.timeout = timeout;
    this.listener = listener;
    
    //Thread object can not be reused, need to create new object every time
    this.thread = new Thread(this);
    this.thread.setName(name);
    isStopped = false;
    this.thread.start();
  }
    
  public void reset() {
    LOG.info(thread.getName() + " RESET");
    synchronized (sleepLock) {
      reset = true;
      sleepLock.notifyAll();
    }   
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
          sleepLock.wait(this.timeout);
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
      listener.timeout();
      LOG.info(thread.getName() + " listener timeout is called");
    } else {
      LOG.info(thread.getName() + " listener is null");
    }
  }


 
}
