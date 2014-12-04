package com.chicm.cmraft.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class RpcMessageEncoder extends MessageToMessageEncoder<RpcCall> {
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
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
  }
}