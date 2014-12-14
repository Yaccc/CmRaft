package com.chicm.cmraft.log;

import java.util.Collection;
import java.util.List;

import com.chicm.cmraft.common.ServerInfo;
import com.chicm.cmraft.core.State;
import com.chicm.cmraft.protobuf.generated.RaftProtos.KeyValuePair;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftLogEntry;

public interface RaftLog {
  void stateChange(State oldState, State newState);
  long getLogTerm(long index);
  long getCommitIndex();
  long getLastApplied();
  long getLastLogTerm();
  long getFlushedIndex();
  List<RaftLogEntry> getLogEntries(long startIndex, long endIndex);
  long getFollowerMatchIndex(ServerInfo follower);
  
  boolean appendEntries(long term, ServerInfo leaderId, long leaderCommit,
      long prevLogIndex, long prevLogTerm, List<RaftLogEntry> leaderEntries);
  void onAppendEntriesResponse(ServerInfo follower, long followerTerm, boolean success, 
      long followerLastApplied);
  
  boolean set(KeyValuePair kv);
  void delete(byte[] key);
  Collection<RaftLogEntry> list(byte[] pattern);
}
