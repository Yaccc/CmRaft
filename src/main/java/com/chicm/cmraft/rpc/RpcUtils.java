package com.chicm.cmraft.rpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

public class RpcUtils {
  static final Log LOG = LogFactory.getLog(RaftRpcService.class);
  
  public static int writeRpc(SocketChannel channel, Message header, Message body)
    throws IOException {
    
    if(header == null) {
      LOG.error("Message head is null!");
      return -1;
    }
    
    int totalsize = header.getSerializedSize();
    if(body != null)
      totalsize += body.getSerializedSize();
    
    int sizesize = CodedOutputStream.computeRawVarint32Size(totalsize);
    
    ByteBuffer buf = ByteBuffer.allocate(totalsize+sizesize);
    
    byte[] sizebuf = new byte[sizesize];
    CodedOutputStream cos = CodedOutputStream.newInstance(sizebuf);
    cos.writeRawVarint32(totalsize); 
    
    buf.put(sizebuf);
    buf.put(header.toByteArray());
    
    if(body != null)
      buf.put(body.toByteArray());
    
    buf.flip();
    return channel.write(buf);
  }
}
