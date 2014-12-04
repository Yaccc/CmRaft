package com.chicm.cmraft.rpc;

import com.chicm.cmraft.protobuf.generated.RaftProtos.ResponseHeader;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class RpcCallInboundHandler extends ChannelInboundHandlerAdapter {
  private BlockingService service;
  
  RpcCallInboundHandler(BlockingService service) {
    this.service = service;
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
      }
    } catch(ServiceException e) {
      e.printStackTrace(System.out);
    }
  }
}
