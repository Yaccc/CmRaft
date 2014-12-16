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

import com.chicm.cmraft.protobuf.generated.RaftProtos.KeyValuePair;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class KeyValue {
  private byte[] key;
  private byte[] value;
  
  public KeyValue(byte[] k, byte[] v) {
    Preconditions.checkNotNull(k);
    key = k;
    value = v;
  }
  
  /**
   * @return the key
   */
  public byte[] getKey() {
    return key;
  }
  
  /**
   * @param key the key to set
   */
  public void setKey(byte[] key) {
    this.key = key;
  }
  /**
   * @return the value
   */
  public byte[] getValue() {
    return value;
  }
  /**
   * @param value the value to set
   */
  public void setValue(byte[] value) {
    this.value = value;
  }
  
  @Override
  public String toString() {
    if(value != null) {
      return String.format("[key:[%s], value:[%s]]", new String(key),
        new String(value));
    } else {
      return String.format("[key:[%s], value:[null]]", new String(key));
    }
  }
  
  public KeyValuePair toKeyValuePair() {
    KeyValuePair.Builder builder = KeyValuePair.newBuilder();
    if(getKey() != null)
      builder.setKey(ByteString.copyFrom(getKey()));
    if(getValue() != null)
      builder.setValue(ByteString.copyFrom(getValue()));
    
    return builder.build();
  }
  
  public static KeyValue copyFrom(KeyValuePair kvp) {
    Preconditions.checkNotNull(kvp);
    Preconditions.checkNotNull(kvp.getKey());
    KeyValue kv;
    if(kvp.getValue() != null) {
      kv = new KeyValue(kvp.getKey().toByteArray(), kvp.getValue().toByteArray());
    } else {
      kv = new KeyValue(kvp.getKey().toByteArray(), null);
    }
    return kv;
  }
  
  @Override
  public int hashCode() {
    int hash = 0;
    if(getKey() != null) {
      hash = (19 * hash) + getKey().hashCode();
    }
    if(getValue()!=null) {
      hash = (37 * hash) + getValue().hashCode();
    }
    return hash;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if(obj == null) {
      return false;
    }
    if (!(obj instanceof KeyValue)) {
       return super.equals(obj);
    }
   
    KeyValue other = (KeyValue)obj;
    return Objects.equal(getKey(), other.getKey()) && Objects.equal(getValue(), other.getValue());
  }
}
