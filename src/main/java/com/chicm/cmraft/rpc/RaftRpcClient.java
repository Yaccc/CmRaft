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
  //private BlockingInterface service = null;
  //public static SocketChannel ch = null;
  
  public static void main(String[] args) throws Exception {
    RaftRpcServer server = new RaftRpcServer(150);
    server.startRpcServer();
    final RaftRpcClient client = new RaftRpcClient();
    
    for(int i = 0; i < 100; i++) {
    new Thread(new Runnable() {
      public void run() {
        client.sendRequest();
        
      }
    }).start();
    }
    
    //System.out.println("client: after call service");
  }
  
  public void sendRequest() {
    ServerId.Builder sbuilder = ServerId.newBuilder();
    sbuilder.setHostName("localhost");
    sbuilder.setPort(11111);
    
    HeartBeatRequest.Builder builder = HeartBeatRequest.newBuilder();
    builder.setServer(sbuilder.build());
    
    Connection con = getConnection();
    
    //System.out.println("client: before call service");
    try {
      con.getService().beatHeart(null, builder.build());
      
      System.out.println("LL");
      //con.getService().beatHeart(null, builder.build());
    } catch (Exception e) {
      e.printStackTrace(System.out);
    } finally {
      con.close();
    }
  }
  
  
  public RaftRpcClient() {
    
    InetSocketAddress isa = new InetSocketAddress("localhost", 12888);
    connections = ConnectionPool.createConnectionPool(isa, 5, 500);
    //services = ClientServicePool.createClientServicePool(isa, 1, 100);
    
    
   // BlockingRpcChannel c = new BlockingRpcChannelImplementation(this, );
   // BlockingRpcChannel c = RaftRpcClient.createBlockingRpcChannel();
   // System.out.println("client: get service1");
   // service =  RaftService.newBlockingStub(c);
  }
  
  public Connection getConnection() {
    return connections.getConnection();
  }
  
  public static int getCallId() {
    return client_call_id.incrementAndGet();
  }
  
  /*
  public BlockingInterface getService() {
    InetSocketAddress isa = new InetSocketAddress("localhost", 12888);
   // BlockingInterface service = null;
    //return services.getService();
    try  {
      SocketChannel channel = SocketChannel.open();
      channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
      if(channel.connect(isa)) {
        System.out.println("client: connected");
      } else
        System.out.println("client: connection failed");
      
      System.out.println("client: get service2");
      //services.add(service);
      RaftRpcClient.ch = channel;
      
      ServerId.Builder sbuilder = ServerId.newBuilder();
      sbuilder.setHostName("localhost");
      sbuilder.setPort(11111);
      
      HeartBeatRequest.Builder builder = HeartBeatRequest.newBuilder();
      builder.setServer(sbuilder.build());
      //Thread.sleep(1000);
      //service.beatHeart(null, builder.build());
      
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
System.out.println("client: get service done");
    return null;
  }*/
  /*
  public void run() {
    try (SocketChannel channel = SocketChannel.open()) {
      SocketAddress adr = new InetSocketAddress("localhost", 9999);
      channel.connect(adr);
      BlockingRpcChannel c = new BlockingRpcChannelImplementation(this, channel);
      BlockingInterface service =  PersonService.newBlockingStub(c);
      Person p = AddPerson.newPerson(id,  name, String.format("%s@aaa.com", name), "11111111");
      service.add(null, p);
    
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
  }*/
  
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
        int len = channel.write(buf);
        //channel.
        System.out.println("client write: " + len);
        System.out.println("client channel: " + channel);
        //RaftRpcClient.ch.write(buf);
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
    
    return respond;
    }
  }
}
