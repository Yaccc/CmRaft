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

package com.chicm.cmraft;

import java.util.List;
/**
 * Key value store interface for clients.
 * 
 * example:
 * KeyValueStore kvs = ConnectoinManager.getConnection().getKeyValueStore();
 * kvs.set("mykey", "myvalue");
 * 
 * @author chicm
 *
 */
public interface KeyValueStore {
  boolean set(byte[] key, byte[] value);
  boolean set(String key, String value);
  
  byte[] get(byte[] key);
  String get(String key);
  
  boolean delete(byte[] key);
  boolean delete(String key);
  
  List<KeyValue> list(byte[] pattern);
  List<KeyValue> list(String pattern);
}
