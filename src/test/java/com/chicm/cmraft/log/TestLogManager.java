package com.chicm.cmraft.log;

import org.junit.Test;

public class TestLogManager {

  @Test
  public void test() {
    LogManager lmgr = new LogManager();
    System.out.println(lmgr.getCommitIndex());
    System.out.println(lmgr.getCurrentIndex());
    System.out.println(lmgr.getCurrentTerm());
  }
}
