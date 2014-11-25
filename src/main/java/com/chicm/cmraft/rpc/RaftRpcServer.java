package com.chicm.cmraft.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.ResponseHeader;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;

public class RaftRpcServer {
  static final Log LOG = LogFactory.getLog(RaftRpcServer.class);
  public static int SERVER_PORT = 12888;
  private final static int DEFAULT_RPC_LISTEN_THREADS = 1;
  private SocketListener socketListener = null;
  private int rpcListenThreads = DEFAULT_RPC_LISTEN_THREADS;
  private RaftRpcService service = null;
  private static PriorityBlockingQueue<RpcCall> requestQueue = new PriorityBlockingQueue<RpcCall>();
  private static PriorityBlockingQueue<RpcCall> responseQueue = new PriorityBlockingQueue<RpcCall>();
  private ExecutorService requestExecutor = null;
  private ExecutorService responseExecutor = null;
  private final static AtomicLong callCounter = new AtomicLong(0);
  private static boolean tpsReportStarted = false;
  
  public static void main(String[] args) throws Exception {
    
    if(args.length < 2) {
      System.out.println("usage: RaftRpcServer <listening port> <listen threads number> [padding length]");
      return;
    }
    SERVER_PORT = Integer.parseInt(args[0]);
    int nListenThreads = Integer.parseInt(args[1]);
    
    if(args.length == 3) {
      RpcUtils.TEST_PADDING_LEN = Integer.parseInt(args[2]);
    }
    
    RaftRpcServer server = new RaftRpcServer(nListenThreads);
    LOG.info("starting server");
    server.startRpcServer();
    
    /*
    final RaftRpcClient client = new RaftRpcClient();
    
    for(int i = 0; i < 5; i++) {
      new Thread(new Runnable() {
        public void run() {
          client.sendRequest();
        }
      }).start();
     }*/
  }
  
  RaftRpcServer (int nListenThreads) {
    socketListener = new SocketListener();
    rpcListenThreads = nListenThreads;
    service = new RaftRpcService();
  }
  
  public BlockingService getService() {
    return service.getService();
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
      LOG.info(String.format("SERVER[%d] accepted\n", Thread.currentThread().getId()));
      startTPSReport();

      for(;;) {
        try {
          processRequest(channel);
        } catch(Exception e) {
          e.printStackTrace(System.out);
          try {
            channel.close();
          } catch(Exception e2) {
          }
          LOG.info("BREAK");
          break;
        } 
        callCounter.incrementAndGet();
        LOG.debug("request processed");
      }
      LOG.info("BREAK OUT");
    }
    @Override
    public void failed(Throwable throwable, AsynchronousServerSocketChannel attachment) {
      //throwable.printStackTrace(System.out);
      LOG.error("Exception");
    }
  }
  
  class SocketListener {
    public void start() throws IOException {
      AsynchronousChannelGroup group = AsynchronousChannelGroup.withFixedThreadPool(rpcListenThreads, 
          Executors.defaultThreadFactory());
      final AsynchronousServerSocketChannel serverChannel = AsynchronousServerSocketChannel.open(group)
          .bind(new InetSocketAddress(SERVER_PORT));
      serverChannel.accept(serverChannel, new SocketHandler());
      
      LOG.info("Server started");
    }
  }
  
  private void processRequest(AsynchronousSocketChannel channel) 
      throws InterruptedException, ExecutionException, IOException, ServiceException {
    try {
      long curtime1 = System.currentTimeMillis();
      RpcCall call = RpcUtils.parseRpcRequestFromChannel(channel, getService());
      long curtime2 = System.currentTimeMillis();
      LOG.debug("Parsing request takes: " + (curtime2-curtime1) + " ms");
      if(call == null)
        return;
      LOG.debug("server recieved: call id: " + call.getCallId());

      Message response = getService().callBlockingMethod(call.getMd(), null, call.getMessage());
      
      sendResponse(channel, call, response);
      
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace(System.out);
      throw e;
    } catch(ServiceException e2) {
      e2.printStackTrace(System.out);
      LOG.info("EXCEPTION");
      throw e2;
    } 
  }
  
  private void sendResponse(AsynchronousSocketChannel channel, RpcCall call, Message response) {
    ResponseHeader.Builder builder = ResponseHeader.newBuilder();
    builder.setId(call.getCallId()); 
    builder.setResponseName(call.getMd().getName());
    ResponseHeader header = builder.build();
    
    try {
        RpcUtils.writeRpc(channel, header, response);
        
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
  }
  
  public void startTPSReport() {
    if (tpsReportStarted )
      return;
    
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while(true) {
          long calls = callCounter.get();
          long starttm = System.currentTimeMillis();
          try {
            Thread.sleep(5000);
          } catch(Exception e) {
            LOG.error("exception", e);
          }
          long sec = (System.currentTimeMillis() - starttm)/1000;
          if(sec == 0)
            sec =1;
          long n = callCounter.get() - calls;
          LOG.info("TPS: " + (n/sec));
        }
      }
    });
    thread.setDaemon(true);
    thread.setName("TPS report");
    thread.start();
    tpsReportStarted = true;
    LOG.info("TPS report started");
  }
  
  class RequestWorker implements Runnable {
    @Override
    public void run() {
      while(true) {
        try {
          RpcCall call = requestQueue.take();
          if(call != null) {
            Message response = getService().callBlockingMethod(call.getMd(), null, call.getMessage());
            if(response != null) {
              ResponseHeader.Builder builder = ResponseHeader.newBuilder();
              builder.setId(call.getCallId()); 
              builder.setResponseName(call.getMd().getName());
              ResponseHeader header = builder.build();
              call.setHeader(header);
              call.setMessage(response);
              
              responseQueue.put(call);
            }
          }
        } catch(Exception e) {
          e.printStackTrace(System.out);
          LOG.error("exception", e);
        }
      }
    }
  }
  
  class ResponseWorker implements Runnable {
    private AsynchronousSocketChannel channel;
    
    public ResponseWorker(AsynchronousSocketChannel channel) {
      this.channel = channel;
    }
    @Override
    public void run() {
      
      while(true) {
        try {
          RpcCall call = responseQueue.take();
          RpcUtils.writeRpc(channel, call.getHeader(), call.getMessage());
        } catch(Exception e) {
          e.printStackTrace(System.out);
          LOG.error("exception", e);
        }
      }
      
    }
  }
  
}
