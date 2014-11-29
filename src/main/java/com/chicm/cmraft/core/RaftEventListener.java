/**
 * 
 */
package com.chicm.cmraft.core;

import com.chicm.cmraft.common.ServerInfo;

/**
 * @author chicm
 *
 */
public interface RaftEventListener {
  
  void timeout();
  void voteReceived();
  void voteReceived(ServerInfo server, long term);
  void discoverLeader();
  void discoverHigherTerm();
}
