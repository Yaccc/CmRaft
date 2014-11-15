package com.chicm.cmraft.common;

public class HeartBeat extends Thread{
  private int period = 0;
  private long starttime = 0;
  private final Object sleepLock = new Object();
  private boolean reset = false;
  private volatile boolean isStopped = false;
  
  HeartBeat(String name, int period) {
    this.period = period;
  }
    
  public void beat() {
    reset();
  }
  
  private void reset() {
    synchronized (sleepLock) {
      reset = true;
      System.out.println("reset:" + (System.currentTimeMillis()-starttime));
      sleepLock.notifyAll();
      System.out.println("reset done:" + (System.currentTimeMillis()-starttime));
    }   
  }

  private void sleep() {
    try {
      while(!isStopped()) {
        reset = false;
        synchronized (sleepLock) {
          System.out.println("sleep:" + (System.currentTimeMillis() - starttime));
          sleepLock.wait(this.period);
          System.out.println("sleep done:"  + (System.currentTimeMillis() -starttime) );
        }
        if(reset)
          continue;
        break;
      }
    } catch(InterruptedException iex) {
      System.out.println("interrupted");
    }
  }

  public final int getPeriod() {
    return period;
  }
  
  @Override
  public void run() {
    try {
      init();
      while (!isStopped()) {
        sleep();
        if(isStopped())
          break;
        try {
           doTimeOut();
        } catch (Exception e) {
          if (isStopped()) {
            break;
          }
        }        
      }
      System.out.println("stopped");
    } catch (Throwable t) {
    } finally {
      cleanup();
    }
  }
  protected void cleanup () {
  }
  protected boolean init() {
    starttime = System.currentTimeMillis();
    return true;
  }
  protected void doTimeOut() {
    System.out.println("doTimeOut");
  }

  public boolean isStopped() {
    return isStopped;
  }

  public void cancel() {
    this.isStopped = true;
    reset();
  }
  
  public static void main(String[] args) throws Exception {
    HeartBeat p = new HeartBeat("P1", 5000);
    p.start();
    Thread.sleep(1000);
    for(int i = 0; ; i++) {
      
      p.beat();
      Thread.sleep(3000);
      if(i == 5) {
        p.cancel();
        break;
      }
    }
    
  }
}
