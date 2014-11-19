package com.chicm.cmraft.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Executors;

public class RaftRpcServer {
  public final static int SERVER_PORT = 12888;
  private final static int DEFAULT_RPC_LISTEN_THREADS = 5;
  private SocketListener socketListener = null;
  private int rpcListenThreads = DEFAULT_RPC_LISTEN_THREADS;
  
  RaftRpcServer (int nListenThreads) {
    socketListener = new SocketListener();
    rpcListenThreads = nListenThreads;
  }
  
  public boolean startRpcServer() {
    try {
      socketListener.start();
    } catch(IOException e) {
      e.printStackTrace(System.out);
      return false;
    }
    return true;
  }
  
  class SocketHandler implements CompletionHandler<AsynchronousSocketChannel, AsynchronousServerSocketChannel> {
    @Override
    public void completed(AsynchronousSocketChannel channel, AsynchronousServerSocketChannel serverChannel) {
      serverChannel.accept(serverChannel, this);
      
      processRequest(channel);
      /*
      System.out.println("SERVER THREAD:" + Thread.currentThread().getId());
      try {
      Thread.sleep(500);
      } catch (Exception e) {
        
      }
      System.out.println("SLEEP DONE:" + Thread.currentThread().getId());*/
    }
    @Override
    public void failed(Throwable throwable, AsynchronousServerSocketChannel attachment) {
    }
  }
  
  class SocketListener {
    public void start() throws IOException {
      AsynchronousChannelGroup group = AsynchronousChannelGroup.withFixedThreadPool(rpcListenThreads, 
          Executors.defaultThreadFactory());
      final AsynchronousServerSocketChannel serverChannel = AsynchronousServerSocketChannel.open(group)
          .bind(new InetSocketAddress(SERVER_PORT));
      serverChannel.accept(serverChannel, new SocketHandler());
      
      System.out.println("Server started");
    }
  }
  
  private void processRequest(AsynchronousSocketChannel channel) {
    
  }
}
