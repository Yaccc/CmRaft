package com.chicm.cmraft.rpc;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestRaftRpcServer {

  public static void main(String[] args) {
    RaftRpcServer server = new RaftRpcServer(20);
    server.startRpcServer();
    final RaftRpcClient client = new RaftRpcClient("localhost", RaftRpcServer.SERVER_PORT);
    
    for(int i = 0; i < 10; i++) {
    new Thread(new Runnable() {
      public void run() {
        client.sendRequest();
        
      }
    }).start();
    }
  }

}

class TestClient implements Runnable {
  private static final int NTHREADS = 100;
  
  public static void sendData() {
    ExecutorService es = Executors.newFixedThreadPool(NTHREADS);
    Thread[] threads = new Thread[NTHREADS];
    for(int i = 0; i < NTHREADS; i++) {
      threads[i] = new Thread(new TestClient());
      threads[i].setName(String.format("CLIENT%02d", i));
      es.submit(threads[i]);
      
    }
    try {
      es.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      
    }
  }
  
  public void run() {
    try (SocketChannel channel = SocketChannel.open()) {
      SocketAddress adr = new InetSocketAddress("localhost", RaftRpcServer.SERVER_PORT);
      channel.connect(adr);
      
      for(int i =0; i< 1; i++) {
      //Thread.currentThread().setName(String.format("CLIENT%02d", i));
      String str = Thread.currentThread().getName() + " Hello";
      byte[] bytes = str.getBytes();
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      channel.write(buf);
      /*
      String str2 = Thread.currentThread().getName() + " World";  
      byte[] bytes2 = str2.getBytes();
      ByteBuffer buf2 = ByteBuffer.wrap(bytes2);
      channel.write(buf2);*/
      
      channel.close();
      }
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
  }
}