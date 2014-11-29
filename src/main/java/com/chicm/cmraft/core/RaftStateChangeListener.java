package com.chicm.cmraft.core;

public interface RaftStateChangeListener {
  void stateChange (State oldState, State newState);
}
