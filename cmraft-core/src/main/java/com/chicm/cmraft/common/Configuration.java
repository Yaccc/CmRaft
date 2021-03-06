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

package com.chicm.cmraft.common;

public interface Configuration {
  
  void useResource(String resourceName);
  
  String getString(String key);
  
  String getString(String key, String defaultValue);
  
  int getInt(String key);
  
  int getInt(String key, int defaultValue);
  
  void set(String key, String value);
  
  void clear();
  
  void remove(String key);
  
  Iterable<String> getKeys();
  
  Iterable<String> getKeys(String prefix);
}
