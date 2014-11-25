package com.chicm.cmraft.rpc;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.util.CappedPriorityBlockingQueue;

public class RpcSendQueue {
  static final Log LOG = LogFactory.getLog(RpcSendQueue.class);
  private static int DEFAULT_WORKERS = 1;
  private AsynchronousSocketChannel channel;
  private CappedPriorityBlockingQueue<RpcCall> callQueue; 
  private ExecutorService executor;
  
  private static int MAX_SEND_CALL_QUEUE_SIZE = 100*1024;
   
  private static volatile RpcSendQueue instance = null;
  
  private RpcSendQueue(AsynchronousSocketChannel channel) {
    this.channel = channel;
    callQueue = new CappedPriorityBlockingQueue<RpcCall>(MAX_SEND_CALL_QUEUE_SIZE);
    executor = Executors.newFixedThreadPool(DEFAULT_WORKERS,
      new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setName("RPC-sending-worker-" + System.currentTimeMillis());
          return t;
        }
    });
    for(int i = 0; i < DEFAULT_WORKERS; i++) {
      Thread t = new Thread(new WorkerThread());
      t.setDaemon(true);
      executor.execute(t);
    }
  }
  
  public static RpcSendQueue getInstance (AsynchronousSocketChannel channel) {
    if(instance == null) {
      synchronized(RpcSendQueue.class) {
        instance = new RpcSendQueue(channel);
      }
    }
    return instance;
  }
  
  
  public void put(RpcCall call) {
    callQueue.put(call);
  }
  
  public int size() {
    return callQueue.size();
  }
  
  class WorkerThread implements Runnable {
    @Override
    public void run() {
      String name = Thread.currentThread().getName();
      while(true) {
        try {
          RpcCall call = callQueue.take();
          if(call == null) {
            LOG.error("Thread: [" + name + "] call is null");
            return;
          }
          LOG.debug(String.format("Thread[%s], take: call id: %d", name, call.getCallId()));
          LOG.debug("send queue size:" + callQueue.size());
          try {
            RpcUtils.writeRpc(channel, call.getHeader(), call.getMessage());
          } catch (Exception e) {
            LOG.error("Thread: " + name, e);
          }
          
        } catch (InterruptedException e) {
          e.printStackTrace(System.out);
        }
      }
    }
  }
}
