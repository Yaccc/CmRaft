package com.chicm.cmraft.log;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestResponseBag {
  
  @Test
  public void test() {
    ResponseBag<Long> bag = new ResponseBag<>();
    bag.add((long)15, 1);
    bag.add((long)15, 1);
    bag.add((long)15, 1);
    bag.add((long)15, 1);
    bag.add((long)15, 100);
    
    assertTrue(bag.get((long)15) == 104);
    assertTrue(bag.get((long)150) == 0);
  }
}
