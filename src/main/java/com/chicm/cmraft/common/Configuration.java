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
