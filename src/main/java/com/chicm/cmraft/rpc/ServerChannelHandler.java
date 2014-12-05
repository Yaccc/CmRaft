package com.chicm.cmraft.rpc;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.core.RaftRpcService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ResponseHeader;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message.Builder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class ServerChannelHandler extends ChannelInitializer<Channel> {
  static final Log LOG = LogFactory.getLog(ServerChannelHandler.class);
  private static final int MAX_PACKET_SIZE = 1024*1024*100;
  private static final int RPC_WORKER_THREADS = 10;
  static final EventExecutorGroup rpcGroup = new DefaultEventExecutorGroup(RPC_WORKER_THREADS);
  private BlockingService service;
  private AtomicLong callCounter;
  
  public ServerChannelHandler(BlockingService service, AtomicLong counter) {
    this.service = service;
    this.callCounter = counter;
  }
  
  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().addLast("FrameDecoder", new LengthFieldBasedFrameDecoder(MAX_PACKET_SIZE,0,4,0,4)); 
    ch.pipeline().addLast("FrameEncoder", new LengthFieldPrepender(4));
    ch.pipeline().addLast("MessageDecoder", new RpcRequestDecoder() );
    ch.pipeline().addLast("MessageEncoder", new RpcResponseEncoder());
    ch.pipeline().addLast(rpcGroup, "RpcHandler", new RpcRequestHandler(service, callCounter));
    LOG.info("initChannel");
  }
  
  class RpcRequestHandler extends ChannelInboundHandlerAdapter {
    private BlockingService service;
    private AtomicLong callCounter;
    
    RpcRequestHandler(BlockingService service, AtomicLong counter) {
      this.service = service;
      this.callCounter = counter;
    }
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) { 
      //System.out.println("channelRead");
      RpcCall call = (RpcCall)msg;
      if(call == null) {
        return;
      }
      try {
      Message response = service.callBlockingMethod(call.getMd(), null, call.getMessage());
        if(response != null) {
          ResponseHeader.Builder builder = ResponseHeader.newBuilder();
          builder.setId(call.getCallId()); 
          builder.setResponseName(call.getMd().getName());
          ResponseHeader header = builder.build();
          call.setHeader(header);
          call.setMessage(response);
          ctx.writeAndFlush(call);
          callCounter.getAndIncrement();
        }
      } catch(ServiceException e) {
        e.printStackTrace(System.out);
      }
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        // Close the connection when an exception is raised.
      System.out.println("catched:" + cause.getMessage());
        //cause.printStackTrace(System.out);
        ctx.close();
    }
  }
  
  class RpcRequestDecoder extends MessageToMessageDecoder<ByteBuf> {
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)       
        throws Exception {
      ByteBufInputStream in = new ByteBufInputStream(msg);

      RequestHeader.Builder hbuilder = RequestHeader.newBuilder();
      hbuilder.mergeDelimitedFrom(in);
      RequestHeader header = hbuilder.build();

      BlockingService service = RaftRpcService.create().getService();
      
      MethodDescriptor md = service.getDescriptorForType().findMethodByName(header.getRequestName());
      Builder builder = service.getRequestPrototype(md).newBuilderForType();
      Message body = null;
      if (builder != null) {
        if(builder.mergeDelimitedFrom(in)) {
          body = builder.build();
        } else {
          LOG.error("Parsing packet failed!");
        }
      }
      RpcCall call = new RpcCall(header.getId(), header, body, md);
      out.add(call);
    }
  }
  
  class RpcResponseEncoder extends MessageToMessageEncoder<RpcCall> {
    @Override
    protected  void encode(ChannelHandlerContext ctx,  RpcCall call, List<Object> out) throws Exception {
      int totalSize = PacketUtils.getTotalSizeofMessages(call.getHeader(), call.getMessage());
      ByteBuf encoded = ctx.alloc().buffer(totalSize);
      ByteBufOutputStream os = new ByteBufOutputStream(encoded);
      try {
        call.getHeader().writeDelimitedTo(os);
        if (call.getMessage() != null)  {
          call.getMessage().writeDelimitedTo(os);
        }
        out.add(encoded);
      } catch(Exception e) {
        e.printStackTrace(System.out);
      }
    }
  }

}
