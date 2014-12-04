package com.chicm.cmraft.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import com.chicm.cmraft.core.RaftRpcService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message.Builder;

public class RpcMessageDecoder extends MessageToMessageDecoder<ByteBuf> {
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out)       
      throws Exception {
    //System.out.println("size:" + msg.capacity());
    long t = System.currentTimeMillis();
    
    ByteBufInputStream in = new ByteBufInputStream(msg);

    RequestHeader.Builder hbuilder = RequestHeader.newBuilder();
    hbuilder.mergeDelimitedFrom(in);
    RequestHeader header = hbuilder.build();
    //System.out.println("header:" + header);

    BlockingService service = RaftRpcService.create().getService();
    
    MethodDescriptor md = service.getDescriptorForType().findMethodByName(header.getRequestName());
    Builder builder = service.getRequestPrototype(md).newBuilderForType();
    Message body = null;
    if (builder != null) {
      if(builder.mergeDelimitedFrom(in)) {
        body = builder.build();
        //System.out.println("body parsed:" + body);
        
      } else {
        //System.out.println("parse failed");
      }
    }
    RpcCall call = new RpcCall(header.getId(), header, body, md);
      //System.out.println("Parse Rpc request from socket: " + call.getCallId() 
      //  + ", takes" + (System.currentTimeMillis() -t) + " ms");

    out.add(call);
  }
}
