package com.chicm.cmraft.rpc;

import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService.BlockingInterface;
import com.chicm.cmraft.util.BlockingHashMap;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.google.protobuf.Descriptors.MethodDescriptor;

public class RaftRpcClient {
  static final Log LOG = LogFactory.getLog(RaftRpcClient.class);
  //private ConnectionPool connections = null;
  private static final int DEFAULT_SOCKET_READ_WORKS = 1;
  private static BlockingService service = null;
  private static volatile AtomicInteger client_call_id = new AtomicInteger(0);
  private static RpcSendQueue sendQueue = null;
  private BlockingInterface stub = null;
  private AsynchronousSocketChannel socketChannel = null;
  private static BlockingHashMap<Integer, RpcCall> responsesMap = new BlockingHashMap<>();
  private ExecutorService socketExecutor = null;
  
  public static void main(String[] args) throws Exception {
    //RaftRpcServer server = new RaftRpcServer(20);
    //server.startRpcServer();
    if(args.length < 3) {
      System.out.println("usage: RaftRpcServer <server host> <server port> <threads number> [padding length]");
      return;
    }
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    int nThreads = Integer.parseInt(args[2]);
    

    if(args.length >= 4) {
      RpcUtils.TEST_PADDING_LEN = Integer.parseInt(args[3]);
    }
    
    final RaftRpcClient client = new RaftRpcClient(host, port);
    
    for(int i = 0; i < nThreads; i++) {
    new Thread(new Runnable() {
      public void run() {
        client.sendRequest();
        
      }
    }).start();
    }
    
    //System.out.println("client: after call service");
  }
  
  //public re
  
  public void sendRequest() {
    ServerId.Builder sbuilder = ServerId.newBuilder();
    sbuilder.setHostName("localhost");
    sbuilder.setPort(11111);
    
    HeartBeatRequest.Builder builder = HeartBeatRequest.newBuilder();
    builder.setServer(sbuilder.build());
    
    //Connection con = getConnection();
    
    LOG.info("client thread started");
    try {
      long tm = System.currentTimeMillis();
      for(int i = 0; i < 5000000 ;i++) {
        HeartBeatResponse r = stub.beatHeart(null, builder.build());
        int n = getCallId();
        if(n %100 == 0 ) {
          long ms = System.currentTimeMillis() - tm;
          LOG.info("RPC CALLS FINISHED: " + n + ", TIME: " + ms/1000 + " s, TPS: " + (n*1000/ms));
        }
      }
      //System.out.println("LL");
      //con.getService().beatHeart(null, builder.build());
    } catch (Exception e) {
      e.printStackTrace(System.out);
    } 
  }
  
  
  public RaftRpcClient(String host, int port) {
    
    InetSocketAddress isa = new InetSocketAddress(host, port);
    //connections = ConnectionPool.createConnectionPool(isa, 1, 500);
    service = (new RaftRpcService()).getService();
    socketChannel = openConnection(isa);
    sendQueue = RpcSendQueue.getInstance(socketChannel);
    
    socketExecutor = Executors.newFixedThreadPool(DEFAULT_SOCKET_READ_WORKS);
    for(int i = 0; i < DEFAULT_SOCKET_READ_WORKS; i++ ) {
      Thread thread = new Thread(new SocketReader(socketChannel, service, responsesMap));
      thread.setDaemon(true);
      socketExecutor.execute(thread);
    }
    
  }
  
  private AsynchronousSocketChannel openConnection(InetSocketAddress isa) {
    AsynchronousSocketChannel channel = null;
    try  {
      LOG.debug("opening connection to:" + isa);
      
      channel = AsynchronousSocketChannel.open();
      channel.connect(isa).get();
      
      LOG.debug("client connected:" + isa);
      
      BlockingRpcChannel c = RaftRpcClient.createBlockingRpcChannel(channel);
      stub =  RaftService.newBlockingStub(c);
      
    
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
    return channel;
  }
  
   
  public static int generateCallId() {
    return client_call_id.incrementAndGet();
  }
  
  public static int getCallId() {
    return client_call_id.get();
  }
 
  
  public static BlockingRpcChannel createBlockingRpcChannel(AsynchronousSocketChannel channel) {
    return new RaftRpcClient.BlockingRpcChannelImplementation(channel);
  }

  public static class BlockingRpcChannelImplementation implements BlockingRpcChannel {
    private AsynchronousSocketChannel channel = null;

    protected BlockingRpcChannelImplementation(AsynchronousSocketChannel channel) {
      this.channel = channel;
    }

    @Override
    public Message callBlockingMethod(MethodDescriptor md, RpcController controller,
                                      Message request, Message returnType) throws ServiceException {

      Message response = null;
      try {
        RequestHeader.Builder builder = RequestHeader.newBuilder();
        int callId = generateCallId();
        builder.setId(callId); 
        builder.setRequestName(md.getName());
        RequestHeader header = builder.build();
        LOG.debug("sending, callid:" + header.getId());
        RpcCall call = new RpcCall(callId, header, request, md);
        long tm = System.currentTimeMillis();
        sendQueue.put(call);
        response = responsesMap.take(callId).getMessage();
        LOG.debug("response taken: " + callId + " :" + response);
        //RpcUtils.writeRpc(channel, header, request);
        //response = RpcUtils.parseRpcResponseFromChannel(channel, service).getMessage();

        LOG.debug(String.format("RPC[%d] round trip takes %d ms", header.getId(), (System.currentTimeMillis() - tm)));
        
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
    
    return response;
    }
  }
  
  class SocketReader implements Runnable {
    private AsynchronousSocketChannel channel;
    private BlockingService service;
    private BlockingHashMap<Integer, RpcCall> results;
    
    public SocketReader(AsynchronousSocketChannel channel, BlockingService service, BlockingHashMap<Integer, RpcCall> results) {
      this.channel = channel;
      this.service = service;
      this.results = results;
    }
    @Override
    public void run() {
      while(true) {
        try {
          RpcCall call = RpcUtils.parseRpcResponseFromChannel(channel, service);
          results.put(call.getCallId(), call);
          LOG.debug("put response, call id: " + call.getCallId() + " result map size: " + results.size());
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("exception", e);
          e.printStackTrace(System.out);
        }
      }
    }
  }
}
