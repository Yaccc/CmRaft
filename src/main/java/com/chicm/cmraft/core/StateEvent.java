package com.chicm.cmraft.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.common.ServerInfo;

public class StateEvent {
  static final Log LOG = LogFactory.getLog(StateEvent.class);
  private StateEventType eventType;
  private ServerInfo fromServer;
  private long term;
  
  /**
   * @return the term
   */
  public long getTerm() {
    return term;
  }

  /**
   * @param term the term to set
   */
  public void setTerm(long term) {
    this.term = term;
  }

  public StateEvent(StateEventType eventType, ServerInfo fromServer, long term) {
    this.eventType = eventType;
    this.fromServer = fromServer;
    this.term = term;
  }
  
  private int memoizedHashCode = 0;
  
  @Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    if(getEventType() != null)
      hash = (19 * hash) + getEventType().hashCode();
    if(getFromServer() != null)
      hash = (37 * hash) + getFromServer().hashCode();
    hash = (51*hash) + new Long(term).hashCode();
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
    if (!(obj instanceof StateEvent)) {
       return super.equals(obj);
    }
    StateEvent other = (StateEvent)obj;
    boolean result = true;
    result = result && ((getEventType()!=null) == (other.getEventType()!=null));
    result = result && (getEventType()==other.getEventType());
    result = result && ((getFromServer()!=null) == (other.getFromServer()!=null));
    
    if (getFromServer()!=null) {
      result = result && getFromServer()
          .equals(other.getFromServer());
    }
    result = result && (getTerm() == other.getTerm());
    
    return result;
  }

  /**
   * @return the eventType
   */
  public StateEventType getEventType() {
    return eventType;
  }

  /**
   * @param eventType the eventType to set
   */
  public void setEventType(StateEventType eventType) {
    this.eventType = eventType;
  }

  /**
   * @return the fromServer
   */
  public ServerInfo getFromServer() {
    return fromServer;
  }

  /**
   * @param fromServer the fromServer to set
   */
  public void setFromServer(ServerInfo fromServer) {
    this.fromServer = fromServer;
  }
}
