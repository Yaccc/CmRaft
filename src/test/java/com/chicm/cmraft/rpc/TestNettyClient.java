package com.chicm.cmraft.rpc;

import java.util.Date;

import com.google.protobuf.Message;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;


public class TestNettyClient {
  
  public static void main(String[] args) throws Exception {
    new TestNettyServer(13888).run();
    for(int i=0; i< 10; i++) {
      TestNettyClient client = new TestNettyClient();
      new Thread(new ConnectWorker(client)).start();
    }
  }
  static class ConnectWorker implements Runnable {
    TestNettyClient c ;
    public ConnectWorker (TestNettyClient cl) {
      this.c = cl;
    }
    @Override
    public void run() {
      try {
      c.connect();
      } catch(Exception e) {
        e.printStackTrace(System.out);
      }
    }
  }
  
  public void connect() throws Exception {
    
    
      String host = "localhost";
      int port = 13888;
      EventLoopGroup workerGroup = new NioEventLoopGroup();

      try {
          Bootstrap b = new Bootstrap(); // (1)
          b.group(workerGroup); // (2)
          b.channel(NioSocketChannel.class); // (3)
          b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
          b.handler(new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(SocketChannel ch) throws Exception {
                  ch.pipeline().addLast("decoder", new LengthFieldBasedFrameDecoder(100000000,0,4,0,4)); 
                  ch.pipeline().addLast("encoder", new LengthFieldPrepender(4));  
                  ch.pipeline().addLast(new TestNettyClientHandler());
              }
          });

          // Start the client.
          ChannelFuture f = b.connect(host, port).sync(); // (5)
System.out.println("connected");
          // Wait until the connection is closed.
          f.channel().closeFuture().sync();
      } finally {
          workerGroup.shutdownGracefully();
      }
  }
  
  static class TestNettyClientHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      System.out.println("client channel read");
        ByteBuf m = (ByteBuf) msg; // (1)
        try {
            long currentTimeMillis = (m.readUnsignedInt() - 2208988800L) * 1000L;
            System.out.println(new Date(currentTimeMillis));
            ctx.close();
        } finally {
            m.release();
        }
    }
    
    @Override
    public void channelActive(final ChannelHandlerContext ctx) { // (1)
      
      System.out.println("Channel Active");
      
      for(int i= 0; i < 1; i++) {
        final ByteBuf buf = ctx.alloc().directBuffer(1024*1024*10+i);
        buf.writeBytes(new byte[1024*1024*10+i]);
        ctx.writeAndFlush(buf);
      }
      //Message a; a.getDefaultInstanceForType()
      /*
        final ByteBuf time = ctx.alloc().buffer(4); // (2)
        time.writeInt((int) (System.currentTimeMillis() / 1000L + 2208988800L));

        final ChannelFuture f = ctx.writeAndFlush(time); // (3)
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                assert f == future;
                ctx.close();
            }
        }); // (4)  */
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
}