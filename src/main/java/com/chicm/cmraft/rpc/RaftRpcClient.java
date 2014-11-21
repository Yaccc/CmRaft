package com.chicm.cmraft.rpc;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerId;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.google.protobuf.Descriptors.MethodDescriptor;

public class RaftRpcClient {
  static final Log LOG = LogFactory.getLog(RaftRpcClient.class);
  private ConnectionPool connections = null;
  private static volatile AtomicInteger client_call_id = new AtomicInteger(1);
  
  public static void main(String[] args) throws Exception {
    RaftRpcServer server = new RaftRpcServer(15);
    server.startRpcServer();
    final RaftRpcClient client = new RaftRpcClient();
    
    for(int i = 0; i < 1; i++) {
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
    
    Connection con = getConnection();
    
    //System.out.println("client: before call service");
    try {
      long tm = System.currentTimeMillis();
      for(;;) {
        con.getService().beatHeart(null, builder.build());
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
    } finally {
      con.close();
    }
  }
  
  
  public RaftRpcClient() {
    
    InetSocketAddress isa = new InetSocketAddress("localhost", 12888);
    connections = ConnectionPool.createConnectionPool(isa, 1, 500);
    
  }
  
  public Connection getConnection() {
    return connections.getConnection();
  }
  
  public static int generateCallId() {
    return client_call_id.incrementAndGet();
  }
  
  public static int getCallId() {
    return client_call_id.get();
  }
 
  
  public static BlockingRpcChannel createBlockingRpcChannel(SocketChannel channel) {
    return new RaftRpcClient.BlockingRpcChannelImplementation(channel);
  }
  /*
  public Message callBlockingMethod(MethodDescriptor md, RpcController controller,
      Message request, Message returnType, SocketChannel channel) {
    
      Message respond = null;
      try {
        RequestHeader.Builder builder = RequestHeader.newBuilder();
        builder.setId(getCallId()); 
        builder.setRequestName(md.getName());
        
        RequestHeader head = builder.build();
        
        ByteBuffer buf = ByteBuffer.allocate(1000);
        
        byte[] headersize = new byte[10];
        CodedOutputStream cos = CodedOutputStream.newInstance(headersize);
        cos.writeRawVarint32(head.getSerializedSize()); 
        
        buf.put(headersize, 0, cos.computeRawVarint32Size(head.getSerializedSize()));
        buf.put(head.toByteArray());
        buf.put(request.toByteArray());
        
        buf.flip();
        channel.write(buf);
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
    
    return respond;
  }
*/
  public static class BlockingRpcChannelImplementation implements BlockingRpcChannel {
    private SocketChannel channel = null;

    protected BlockingRpcChannelImplementation(SocketChannel channel) {
      this.channel = channel;
    }

    @Override
    public Message callBlockingMethod(MethodDescriptor md, RpcController controller,
                                      Message request, Message returnType) throws ServiceException {

      Message respond = null;
      try {
        RequestHeader.Builder builder = RequestHeader.newBuilder();
        builder.setId(generateCallId()); 
        builder.setRequestName(md.getName());
        
        RequestHeader header = builder.build();
        int len = RpcUtils.writeRpc(channel, header, request);
        
        /*
        ByteBuffer buf = ByteBuffer.allocate(1000);
        
        byte[] headersize = new byte[10];
        CodedOutputStream cos = CodedOutputStream.newInstance(headersize);
        cos.writeRawVarint32(header.getSerializedSize()); 
        
        buf.put(headersize, 0, cos.computeRawVarint32Size(header.getSerializedSize()));
        buf.put(header.toByteArray());
        buf.put(request.toByteArray());
        
        buf.flip();
        int len = channel.write(buf);*/
        LOG.debug("client write: " + len);
        LOG.debug("client channel: " + channel);
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
    
    return respond;
    }
  }
}
