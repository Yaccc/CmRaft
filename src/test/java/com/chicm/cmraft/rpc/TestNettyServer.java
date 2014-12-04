package com.chicm.cmraft.rpc;

import java.nio.ByteBuffer;
import java.util.List;

import com.chicm.cmraft.core.RaftRpcService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.google.protobuf.BlockingService;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message.Builder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.protobuf.ProtobufDecoder;

public class TestNettyServer {

  private int port;

  public TestNettyServer(int port) {
      this.port = port;
  }

  public void run() throws Exception {
      EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
      EventLoopGroup workerGroup = new NioEventLoopGroup();
      try {
          ServerBootstrap b = new ServerBootstrap(); // (2)
          b.group(bossGroup, workerGroup)
           .channel(NioServerSocketChannel.class) // (3)
           .childHandler(new ChannelInitializer<SocketChannel>() { // (4)
               @Override
               public void initChannel(SocketChannel ch) throws Exception {
                   
                   ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(100000000,0,4,0,4)); 
                   ch.pipeline().addLast("encoder", new LengthFieldPrepender(4));  
                   ch.pipeline().addLast("msgDecoder", new MyProtobufDecoder() );
                   ch.pipeline().addLast("handler", new MyRcpCallHandler());
                   
                   /*
                    * static final EventExecutorGroup group = new DefaultEventExecutorGroup(16);
                       ...
                      
                       ChannelPipeline pipeline = ch.pipeline();
                      
                       pipeline.addLast("decoder", new MyProtocolDecoder());
                       pipeline.addLast("encoder", new MyProtocolEncoder());
                      
                       // Tell the pipeline to run MyBusinessLogicHandler's event handler methods
                       // in a different thread than an I/O thread so that the I/O thread is not blocked by
                       // a time-consuming task.
                       // If your business logic is fully asynchronous or finished very quickly, you don't
                       // need to specify a group.
                       pipeline.addLast(group, "handler", new MyBusinessLogicHandler());
                    */
               }
           })
           .option(ChannelOption.SO_BACKLOG, 128)          // (5)
           .childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

          // Bind and start to accept incoming connections.
          ChannelFuture f = b.bind(port).sync(); // (7)

          // Wait until the server socket is closed.
          // In this example, this does not happen, but you can do that to gracefully
          // shut down your server.
          System.out.println("server started");
          //f.channel().closeFuture().sync();
      } finally {
         // workerGroup.shutdownGracefully();
         // bossGroup.shutdownGracefully();
      }
  }

  public static void main(String[] args) throws Exception {
      int port;
      if (args.length > 0) {
          port = Integer.parseInt(args[0]);
      } else {
          port = 18080;
      }
      new TestNettyServer(port).run();
  }
  
  public class TestServerHandler extends ChannelInboundHandlerAdapter { // (1)

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
        // Discard the received data silently.
     System.out.println("msg:" +  ((ByteBuf) msg).capacity());
     System.out.println("isdirect:" +  ((ByteBuf) msg).isDirect());
        ((ByteBuf) msg).release(); // (3)
      System.out.println("channel read");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        // Close the connection when an exception is raised.
        cause.printStackTrace(System.out);
        ctx.close();
    }
  }
  
  public class MyRcpCallHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
        // Discard the received data silently.
      RpcCall call = (RpcCall)msg;
      System.out.println("rpccall parsed:" + call);
    
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        // Close the connection when an exception is raised.
        cause.printStackTrace(System.out);
        ctx.close();
    }
  }
  
  public class MyProtobufDecoder extends MessageToMessageDecoder<ByteBuf> {

   
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)       
        throws Exception {
      System.out.println("size:" + msg.capacity());
      long t = System.currentTimeMillis();
      
      ByteBufInputStream in = new ByteBufInputStream(msg);

      RequestHeader.Builder hbuilder = RequestHeader.newBuilder();
      hbuilder.mergeDelimitedFrom(in);
      RequestHeader header = hbuilder.build();
      System.out.println("header:" + header);

      BlockingService service = RaftRpcService.create().getService();
      
      MethodDescriptor md = service.getDescriptorForType().findMethodByName(header.getRequestName());
      Builder builder = service.getRequestPrototype(md).newBuilderForType();
      Message body = null;
      if (builder != null) {
        if(builder.mergeDelimitedFrom(in)) {
          body = builder.build();
          System.out.println("body parsed:" + body);
          
        } else {
          System.out.println("parse failed");
        }
      }
      RpcCall call = new RpcCall(header.getId(), header, body, md);
        System.out.println("Parse Rpc request from socket: " + call.getCallId() 
          + ", takes" + (System.currentTimeMillis() -t) + " ms");

      out.add(call);
    }
  }
}