package com.chicm.cmraft.rpc;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RpcSendQueue {
  static final Log LOG = LogFactory.getLog(RpcSendQueue.class);
  private static int DEFAULT_WORKERS = 1;
  private AsynchronousSocketChannel channel;
  private PriorityBlockingQueue<RpcCall> callQueue; 
  private ExecutorService executor;
  
  private static int MAX_SEND_CALL_QUEUE_SIZE = 50;
  /** Lock held by put, offer, etc */
  private final ReentrantLock putLock = new ReentrantLock();
  /** Wait queue for waiting puts */
  private final Condition notFull = putLock.newCondition();
  
  private static volatile RpcSendQueue instance = null;
  
  private RpcSendQueue(AsynchronousSocketChannel channel) {
    this.channel = channel;
    callQueue = new PriorityBlockingQueue<RpcCall>();
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
  
  /**
   * Signals a waiting put. Called only from take/poll.
   */
  private void signalNotFull() {
      final ReentrantLock putLock = this.putLock;
      putLock.lock();
      try {
        LOG.debug("singal not full");
        notFull.signal();
      } finally {
        putLock.unlock();
      }
  }
  
  public void put(RpcCall call) throws InterruptedException {
    if (call == null) throw new NullPointerException();
    // Note: convention in all put/take/etc is to preset local var
    // holding count negative to indicate failure unless set.
 
    final ReentrantLock putLock = this.putLock;
    putLock.lockInterruptibly();
    try {
        while (callQueue.size() >= MAX_SEND_CALL_QUEUE_SIZE) {
          LOG.debug("Thread:" + Thread.currentThread().getName() + ": queue is full, waiting...");
          notFull.await();
          LOG.debug("Thread:" + Thread.currentThread().getName() + ": waiting done");
        }
        
        callQueue.put(call);
        if (callQueue.size() < MAX_SEND_CALL_QUEUE_SIZE)
            notFull.signal();
    } finally {
        putLock.unlock();
    }
  }
  
  public RpcCall take() throws InterruptedException {
    RpcCall call = callQueue.take();
    if(callQueue.size() < MAX_SEND_CALL_QUEUE_SIZE) {
      signalNotFull();
    }
    return call;
  }
  
  class WorkerThread implements Runnable {
    @Override
    public void run() {
      String name = Thread.currentThread().getName();
      while(true) {
        try {
          RpcCall call = take();
          if(call == null) {
            LOG.error("Thread: [" + name + "] call is null");
            return;
          }
          LOG.debug(String.format("Thread[%s], take: call id: %d", name, call.getCallId()));
          try {
            //RpcUtils.writeRpc(channel, call.getHeader(), call.getMessage());
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
