package com.chicm.cmraft.rpc;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.core.RaftRpcService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ResponseHeader;
import com.chicm.cmraft.util.BlockingHashMap;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
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

public class ClientChannelHandler extends ChannelInitializer<Channel>  {
  static final Log LOG = LogFactory.getLog(ClientChannelHandler.class);
  private static final int MAX_PACKET_SIZE = 1024*1024*100;
  private ChannelHandlerContext activeCtx;
  private RpcClientEventListener listener;
  //private long startTime = System.currentTimeMillis();
  
  public ClientChannelHandler(RpcClientEventListener listener) {
    this.listener = listener;
  }
  
  @Override
  protected void initChannel(Channel ch) throws Exception {
    ch.pipeline().addLast("FrameDecoder", new LengthFieldBasedFrameDecoder(MAX_PACKET_SIZE,0,4,0,4)); 
    ch.pipeline().addLast("FrameEncoder", new LengthFieldPrepender(4));
    ch.pipeline().addLast("MessageDecoder", new RpcResponseDecoder() );
    ch.pipeline().addLast("MessageEncoder", new RpcRequestEncoder());
    ch.pipeline().addLast("ClientHandler", new RpcResponseHandler());
    LOG.debug("initChannel");
  }
  
  public ChannelHandlerContext getCtx() {
    return activeCtx;
  }
  
  class RpcResponseHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      
      RpcCall call = (RpcCall) msg;
      LOG.info("client channel read, callid: " + call.getCallId());
      
      listener.onRpcResponse(call);
    }
    
    /* (non-Javadoc)
     * @see io.netty.channel.ChannelStateHandlerAdapter#channelActive(io.netty.channel.ChannelHandlerContext)
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) { // (1)
      activeCtx = ctx;
      LOG.debug("Client Channel Active");
    }
    
    /* (non-Javadoc)
     * @see io.netty.channel.ChannelStateHandlerAdapter#channelInactive(io.netty.channel.ChannelHandlerContext)
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      super.channelInactive(ctx);
    }
    
    /* (non-Javadoc)
     * @see io.netty.channel.ChannelStateHandlerAdapter#exceptionCaught(io.netty.channel.ChannelHandlerContext)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Socket Exception: " + cause.getMessage(), cause);
        listener.channelClosed();
    }
  }
  
  class RpcResponseDecoder extends MessageToMessageDecoder<ByteBuf> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)       
        throws Exception {
      
      ByteBufInputStream in = new ByteBufInputStream(msg);

      ResponseHeader.Builder hbuilder = ResponseHeader.newBuilder();
      hbuilder.mergeDelimitedFrom(in);
      ResponseHeader header = hbuilder.build();

      BlockingService service = RaftRpcService.create().getService();
      
      MethodDescriptor md = service.getDescriptorForType().findMethodByName(header.getResponseName());
      Builder builder = service.getResponsePrototype(md).newBuilderForType();
      Message body = null;
      if (builder != null) {
        if(builder.mergeDelimitedFrom(in)) {
          body = builder.build();
        } else {
          LOG.error("Parse packet failed!!");
        }
      }
      RpcCall call = new RpcCall(header.getId(), header, body, md);

      out.add(call);
    }
  }
  
  class RpcRequestEncoder extends MessageToMessageEncoder<RpcCall> {
    @Override
    protected  void encode(ChannelHandlerContext ctx,  RpcCall call, List<Object> out) throws Exception {
      //System.out.println("RpcMessageEncoder");
      int totalSize = PacketUtils.getTotalSizeofMessages(call.getHeader(), call.getMessage());
      ByteBuf encoded = ctx.alloc().buffer(totalSize);
      ByteBufOutputStream os = new ByteBufOutputStream(encoded);
      try {
        call.getHeader().writeDelimitedTo(os);
        if (call.getMessage() != null)  {
          call.getMessage().writeDelimitedTo(os);
        }
        out.add(encoded);
        LOG.info("Rpc encode: " + call.getCallId());
      } catch(Exception e) {
        LOG.error("Rpc Encoder exception:" + e.getMessage(), e);
      }
    }
  }
}
