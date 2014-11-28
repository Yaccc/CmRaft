package com.chicm.cmraft.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ServerInfo {
  static final Log LOG = LogFactory.getLog(ServerInfo.class);
  private String host;
  private int port;
  private long startCode;
  
  public ServerInfo(String host, int port) {
    this.host = host;
    this.port = port;
  }
  
  /**
   * @return the host
   */
  public String getHost() {
    return host;
  }
  /**
   * @param host the host to set
   */
  public void setHost(String host) {
    this.host = host;
  }
  /**
   * @return the port
   */
  public int getPort() {
    return port;
  }
  /**
   * @param port the port to set
   */
  public void setPort(int port) {
    this.port = port;
  }
  /**
   * @return the startCode
   */
  public long getStartCode() {
    return startCode;
  }
  /**
   * @param startCode the startCode to set
   */
  public void setStartCode(long startCode) {
    this.startCode = startCode;
  }
  
  public static ServerInfo parseFromString(String hostAddress) {
    if(hostAddress == null)
      return null;
    String[] results = hostAddress.split(":");
    if(results == null || results.length != 2)
      return null;
    int port;
    try {
      port = Integer.parseInt(results[1]);
    } catch(Exception e) {
      LOG.error("parse port form configuration exception", e);
      return null;
    }
    return new ServerInfo(results[0], port);
  }
  
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("host: " + getHost());
    s.append("\nport: " + getPort());
    s.append("\nstartCode: " + getStartCode());
    
    return s.toString();
  }

  
  private int memoizedHashCode = 0;
  
  @Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getPort();
    hash = (37 * hash) + (int)getStartCode();
    if(getHost()!=null) {
      hash = (53 * hash) + getHost().hashCode();
    }
    memoizedHashCode = hash;
    return hash;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if(obj == null) {
      LOG.error("equals (null)");
      return false;
    }
    if (!(obj instanceof ServerInfo)) {
       return super.equals(obj);
    }
    ServerInfo other = (ServerInfo)obj;
    boolean result = true;
    result = result && ((getHost()!=null) == (other.getHost()!=null));
    result = result && (getPort()==other.getPort());
    result = result && (getStartCode() == other.getStartCode());
    
    return result;
  }
}
