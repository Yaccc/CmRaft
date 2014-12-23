package com.chicm.cmraft.core;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.junit.Test;

import com.chicm.cmraft.common.CmRaftConfiguration;
import com.chicm.cmraft.common.Configuration;
public class TestClusterMemberManager {
  
  public Configuration createConf1() {
    Configuration conf1 = CmRaftConfiguration.create();
    conf1.clear();
    conf1.set("raft.server.server1", "chicm:2222");
    conf1.set("raft.server.server2", "chicm1:2222");
    conf1.set("raft.server.server3", "chicm2:2222");
    conf1.set("raft.server.server4", "chicm3:2222");
    return conf1;
  }
  
  public Configuration createConf2() {
    Configuration conf1 = CmRaftConfiguration.create();
    conf1.clear();
    conf1.set("raft.server.server1", "chicm:2222");
    conf1.set("raft.server.server2", "chicm:2223");
    conf1.set("raft.server.server3", "chicm:2224");
    conf1.set("raft.server.server4", "chicm:2224");
    
    conf1.set("raft.local.server", "chicm:2222");
    return conf1;
  }

  @Test
  public void testClusterMember() {
    Configuration conf = CmRaftConfiguration.create();
    
    ClusterMemberManager mgr1 = new ClusterMemberManager(createConf1());
    System.out.println("================================");
    System.out.println("local:" + mgr1.getLocalServer());
    System.out.println("remote:" + mgr1.getRemoteServers());
    
    ClusterMemberManager mgr2 = new ClusterMemberManager(createConf2());
    System.out.println("================================");
    System.out.println("local:" + mgr2.getLocalServer());
    System.out.println("remote:" + mgr2.getRemoteServers());
    
    System.out.println("raftserver:" + ClusterMemberManager.getRaftServers(createConf2()));
  }
  
  /*
  @Test
  public void testLocalHost() {
    try {
      InetAddress inetAddress = InetAddress.getLocalHost();
      String ip = inetAddress.getHostAddress();
      String canonical =inetAddress.getCanonicalHostName();
      String host =inetAddress.getHostName();
      System.out.println("ip:"+ip);
      System.out.println("host name:"+canonical);
      System.out.println("hostname:"+host);
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    try {
      InetAddress inetAddress = InetAddress.getLoopbackAddress();
      String ip = inetAddress.getHostAddress();
      String canonical =inetAddress.getCanonicalHostName();
      String host =inetAddress.getHostName(); 
      System.out.println("ip:"+ip);
      System.out.println("host name:"+canonical);
      System.out.println("hostname:"+host);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @Test
  public void testNetworkInterfaces() throws SocketException {
    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    System.out.println("**************");
    while(interfaces.hasMoreElements())
    {
        NetworkInterface inet = (NetworkInterface) interfaces.nextElement();
        Enumeration<InetAddress> addrs = inet.getInetAddresses();
        while (addrs.hasMoreElements())
        {
            InetAddress addr = (InetAddress) addrs.nextElement();
            System.out.print(addr.getHostAddress());
            System.out.println(" ==> " + addr.getHostName());
        }
    }
  }
  */
  
}
