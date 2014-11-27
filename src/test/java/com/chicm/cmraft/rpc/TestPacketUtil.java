package com.chicm.cmraft.rpc;

import java.util.Random;

import org.junit.Test;

import static com.chicm.cmraft.rpc.PacketUtils.*;
import static org.junit.Assert.*;

public class TestPacketUtil {
   
  @Test
  public void testInt2Bytes() {
    
    Random r = new Random((int)System.currentTimeMillis());
    
    for(int i = 0; i <10000; i++) {
      
      int n  = r.nextInt();
      byte[] b = int2Bytes(n);
      int n2 = bytes2Int(b);
      
      assertTrue(n2 == n);
      
    }
  }
}
