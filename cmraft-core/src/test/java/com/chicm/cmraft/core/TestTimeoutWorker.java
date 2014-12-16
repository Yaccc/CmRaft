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
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTimeoutWorker {
  private static RaftNodeTimer worker;
  
  static class MyListener implements TimeoutListener {
    @Override
    public void timeout() {
      System.out.println("timeout called");
      assertTrue(false);
    }
    
  }
  
  @BeforeClass
  public static void init() {
    worker = RaftNodeTimer.create("test", 1000, new MyListener());
  }
  
  @Test
  public void testReset() throws Exception {
    for(int i = 0; i < 5; i++) {
       worker.reset();
       Thread.sleep(300);
    }
  }

}
