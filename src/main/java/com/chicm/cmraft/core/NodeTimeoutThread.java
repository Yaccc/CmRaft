/*
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

package com.chicm.cmraft.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NodeTimeoutThread implements Runnable {
  static final Log LOG = LogFactory.getLog(NodeTimeoutThread.class);
  private RaftEventListener listener = null;
  private int period = 0;
  private long starttime = 0;
  private final Object sleepLock = new Object();
  private boolean reset = false;
  private volatile boolean isStopped = false;
  private Thread thread = null;
  
  public NodeTimeoutThread() {
  }
  
  public void start(int timeout, RaftEventListener listener) {
    this.period = timeout;
    this.listener = listener;
    
    //Thread object can not be reused, need to create new object every time
    this.thread = new Thread(this);
    this.thread.setName("RaftNode-TimeOut-" + System.currentTimeMillis());
    isStopped = false;
    this.thread.start();
  }
    
  public void reset() {
    synchronized (sleepLock) {
      reset = true;
      LOG.debug("reset:" + (System.currentTimeMillis()-starttime));
      sleepLock.notifyAll();
      LOG.debug("reset done:" + (System.currentTimeMillis()-starttime));
    }   
  }

  private void sleep() {
    LOG.debug("entering sleep");
    try {
      while(!isStopped()) {
        reset = false;
        synchronized (sleepLock) {
          LOG.debug("sleep:" + (System.currentTimeMillis() - starttime));
          sleepLock.wait(this.period);
          LOG.debug("sleep done:"  + (System.currentTimeMillis() -starttime) );
        }
        if(reset)
          continue;
        break;
      }
    } catch(InterruptedException iex) {
      LOG.debug("interrupted");
    }
  }

  public final int getPeriod() {
    return period;
  }
  
  @Override
  public void run() {
    LOG.info(Thread.currentThread().getName() + " started, timeout=" + this.period);
    try {
      init();
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
      //LOG.info(Thread.currentThread().getName() + " stopped");
    } catch (Throwable t) {
    } finally {
      LOG.info(Thread.currentThread().getName() + " stopped");
      cleanup();
    }
  }
  protected void cleanup () {
    LOG.info("***cleanup");
  }
  protected boolean init() {
    LOG.debug("init");
    starttime = System.currentTimeMillis();
    return true;
  }
  protected void doTimeOut() {
    LOG.debug(Thread.currentThread().getName() + " timeout");
    if(listener != null) {
      listener.timeout();
      LOG.debug(Thread.currentThread().getName() + " listener timeout is called");
    } else {
      LOG.debug(Thread.currentThread().getName() + " listener is null");
    }
  }

  public boolean isStopped() {
    return isStopped;
  }

  public void stop() {
    LOG.debug("stop");
    this.isStopped = true;
    reset();
    //join();
  }
  
  public void join() {
    try {
      this.thread.join();
    } catch(Exception e) {
      LOG.error("Join exception!", e);
    }
  }
  
  
  public static void main(String[] args) throws Exception {
    NodeTimeoutThread p = new NodeTimeoutThread();
    p.start(5000, null);
    Thread.sleep(6000);
    
    for(int i = 0; i<7 ; i++) {
      
      p.reset();
      Thread.sleep(3000);
      if(i == 5) {
        p.stop();
        break;
      }
    }
    //p.join();
    
    System.exit(0);
    
  }
}
