package com.chicm.cmraft.log;

import org.junit.Test;

import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftEntry;

public class TestLogEntry {
  
  @Test
  public void test() {
    LogEntry le = new LogEntry(1, 2, "key1", "value1");
    
    RaftEntry re = le.toRaftEntry();
    
    System.out.println(re);
    
    Exception e = new Exception("aaa");
    e.printStackTrace(System.out);
  }

}
