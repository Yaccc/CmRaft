package com.chicm.cmraft.core;

import static org.junit.Assert.*;

import org.junit.Test;

import com.chicm.cmraft.common.ServerInfo;

public class TestStateEvent {

  @Test
  public void test() {
    ServerInfo s1 = new ServerInfo("abc", 111);
    ServerInfo s2 = new ServerInfo("abc", 111);
    ServerInfo s3 = new ServerInfo("abcd", 111);
    
    System.out.println(s1);
    
    StateEvent e1 = new StateEvent(StateEventType.DISCOVERD_HIGHER_TERM, s1, 0);
    StateEvent e2 = new StateEvent(StateEventType.DISCOVERD_HIGHER_TERM, s1, 0);
    StateEvent e3 = new StateEvent(StateEventType.DISCOVERD_HIGHER_TERM, s2, 0);
    
    StateEvent e4 = new StateEvent(StateEventType.DISCOVERD_HIGHER_TERM, s3, 0);
    StateEvent e5 = new StateEvent(StateEventType.ELECTION_TIMEOUT, s3, 0);
    
    assertFalse(s1.equals(s3));
    assertTrue(s1.equals(s2));
    assertTrue(e1.equals(e2));
    assertTrue(e1.equals(e3));
    assertFalse(e1.equals(e4));
    assertFalse(e4.equals(e5));
  }
}
