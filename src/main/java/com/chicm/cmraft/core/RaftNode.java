/**
* Copyright 2014 The Apache Software Foundation
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

package com.chicm.cmraft.core;

import java.util.Map;

import com.chicm.cmraft.rpc.RpcServer;
import com.chicm.cmraft.rpc.RpcClient;

/**
 * This class represents a Raft node in a cluster.
 * Rule of Raft servers:
 * All Servers:
 * If commitIndex > lastApplied: increment lastApplied, apply
 * log[lastApplied] to state machine 
 * If RPC request or response contains term T > currentTerm:
 * set currentTerm = T, convert to follower 
 * 
 * Followers :
 * Respond to RPCs from candidates and leaders
 * If election timeout elapses without receiving AppendEntries
 * RPC from current leader or granting vote to candidate:
 * convert to candidate

 * Candidates :
 * On conversion to candidate, start election:
 * Increment currentTerm
 * Vote for self
 * Reset election timer
 * Send RequestVote RPCs to all other servers
 * If votes received from majority of servers: become leader
 * If AppendEntries RPC received from new leader: convert to
 * follower
 * If election timeout elapses: start new election

 * Leaders:
 * Upon election: send initial empty AppendEntries RPCs
 * (heartbeat) to each server; repeat during idle periods to
 * prevent election timeouts 
 * If command received from client: append entry to local log,
 * respond after entry applied to state machine
 * If last log index >= nextIndex for a follower: send
 * AppendEntries RPC with log entries starting at nextIndex
 * If successful: update nextIndex and matchIndex for
 * follower 
 * If AppendEntries fails because of log inconsistency:
 * decrement nextIndex and retry 
 * If there exists an N such that N > commitIndex, a majority
 * of matchIndex[i] >= N, and log[N].term == currentTerm:
 * set commitIndex = N 
 * 
 * Persistent state on all servers:
 * (Updated on stable storage before responding to RPCs)
 * currentTerm latest term server has seen (initialized to 0
 * on first boot, increases monotonically)
 * votedFor candidateId that received vote in current
 * term (or null if none)
 * log[] log entries; each entry contains command
 * for state machine, and term when entry
 * was received by leader (first index is 1)
 * 
 * Volatile state on all servers:
 * commitIndex index of highest log entry known to be
 * committed (initialized to 0, increases
 * monotonically)
 * lastApplied index of highest log entry applied to state
 * machine (initialized to 0, increases
 * monotonically)
 * 
 * Volatile state on leaders:
 * (Reinitialized after election)
 * nextIndex[] for each server, index of the next log entry
 * to send to that server (initialized to leader
 * last log index + 1)
 * matchIndex[] for each server, index of highest log entry
 * known to be replicated on server
 * (initialized to 0, increases monotonically)
 * 
 * @author chicm
 *
 */
public class RaftNode {
  
  private StateMachine fsm = new StateMachine();
  private RpcServer rpcServer = new RpcServer(10);
  private Map<String, RpcClient> rpcCients;
  
  private long currentTerm;

  public RaftNode() {
  }

}
