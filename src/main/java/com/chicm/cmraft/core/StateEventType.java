package com.chicm.cmraft.core;

public enum StateEventType {
  ELECTION_TIMEOUT,
  VOTE_RECEIVED_ONE,
  VOTE_RECEIVED_MAJORITY,
  DISCOVERD_LEADER,
  DISCOVERD_HIGHER_TERM;
}
