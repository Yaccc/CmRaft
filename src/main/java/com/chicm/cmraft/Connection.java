/**
 * 
 */
package com.chicm.cmraft;

/**
 * @author chicm
 *
 */
public interface Connection {
  boolean set(byte[] key, byte[] value);
  boolean set(String key, String value);
  
  byte[] get(byte[] key);
  String get(String key);
  
  boolean delete(byte[] key);
  boolean delete(String key);
  
  Result list(byte[] pattern);
  Result list(String pattern);
  
  void close();
}
