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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class represents the states transition of a Raft Node.
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
 * 
 * @author chicm
 *
 */

public class StateMachine {
  static final Log LOG = LogFactory.getLog(StateMachine.class);
  
  private volatile State state;
  private static Map<State, Map<StateEventType, State>> transitionMap = new HashMap<>();
  private RaftStateChangeListener listener;
  
  StateMachine(RaftStateChangeListener listener) {
    this.listener = listener;
    this.state = State.FOLLOWER;
    buildTransitionMap();
  }
  
  public boolean accepting(StateEventType sym) {
    return transitionMap.get(state).get(sym) != null;
  }
  
  public State getState() {
    return this.state;
  }
  
  
  private void notifyIfStateChange(State oldState, State newState) {
    if(oldState != state) {
      LOG.debug(String.format("State change: %s=>%s", oldState, newState));
      if(listener != null) {
        listener.stateChange(oldState, state);
      }
    }
  }
  
  public State electionTimeout() {
    State oldState = getState();
    if(accepting(StateEventType.ELECTION_TIMEOUT)) {
        state = transitionMap.get(state).get(StateEventType.ELECTION_TIMEOUT);
    }
    notifyIfStateChange(oldState, state);

    return oldState;
  }
  
  public State voteReceived() {
    State oldState = getState();
    if(accepting(StateEventType.VOTE_RECEIVED_MAJORITY)) {
        state = transitionMap.get(state).get(StateEventType.VOTE_RECEIVED_MAJORITY);
    }
    notifyIfStateChange(oldState, state);
    return oldState;
  }
  
  public State discoverLeader() {
    State oldState = getState();
    if(accepting(StateEventType.DISCOVERD_LEADER)) {
      state = transitionMap.get(state).get(StateEventType.DISCOVERD_LEADER);
    }
    notifyIfStateChange(oldState, state);
    return oldState;
  }
  
  public State discoverHigherTerm() {
    State oldState = getState();
    if(accepting(StateEventType.DISCOVERD_HIGHER_TERM)) {
      state = transitionMap.get(state).get(StateEventType.DISCOVERD_HIGHER_TERM);
    }
    notifyIfStateChange(oldState, state);
    return oldState;
  }
    
  private void buildTransitionMap() {
    HashMap<StateEventType, State> followerMap = new HashMap<>(); 
    HashMap<StateEventType, State> candidateMap = new HashMap<>(); 
    HashMap<StateEventType, State> leaderMap = new HashMap<>(); 
    
    followerMap.put(StateEventType.ELECTION_TIMEOUT, State.CANDIDATE);
    
    candidateMap.put(StateEventType.VOTE_RECEIVED_MAJORITY, State.LEADER);
    candidateMap.put(StateEventType.DISCOVERD_LEADER, State.FOLLOWER);
    candidateMap.put(StateEventType.DISCOVERD_HIGHER_TERM, State.FOLLOWER);
    
    leaderMap.put(StateEventType.DISCOVERD_HIGHER_TERM, State.FOLLOWER);
    
    transitionMap.put(State.FOLLOWER, followerMap);
    transitionMap.put(State.CANDIDATE, candidateMap);
    transitionMap.put(State.LEADER, leaderMap);
    
  }

}
