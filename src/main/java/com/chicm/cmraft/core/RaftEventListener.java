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
  void voteReceived(ServerInfo server);
  void discoverLeader();
  void discoverHigherTerm();
}
