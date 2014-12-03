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

package com.chicm.cmraft.rpc;

import java.util.Random;

import org.junit.Test;

import static com.chicm.cmraft.rpc.PacketUtils.*;
import static org.junit.Assert.*;

public class TestPacketUtil {
   
  @Test
  public void testInt2Bytes() {
    
    Random r = new Random((int)System.currentTimeMillis());
    
    for(int i = 0; i <10000; i++) {
      
      int n  = r.nextInt();
      byte[] b = int2Bytes(n);
      int n2 = bytes2Int(b);
      
      assertTrue(n2 == n);
      
    }
  }
}
