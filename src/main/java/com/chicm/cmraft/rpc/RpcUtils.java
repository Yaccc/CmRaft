package com.chicm.cmraft.rpc;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

public class RpcUtils {
  static final Log LOG = LogFactory.getLog(RaftRpcService.class);
  
  public static int getTotalSizeofMessages(Message ... messages) {
    int totalSize = 0;
    for (Message m: messages) {
      if (m == null) continue;
      totalSize += m.getSerializedSize();
      totalSize += CodedOutputStream.computeRawVarint32Size(m.getSerializedSize());
    }
    return totalSize;
  }
  
  public static int writeRpc(SocketChannel channel, Message header, Message body) 
    throws IOException {
    int totalSize = getTotalSizeofMessages(header, body);
    return writeRpc(channel, header, body, totalSize);
  }
  
  private static int writeRpc(SocketChannel channel, Message header, Message body, 
      int totalSize) throws IOException {
    OutputStream os = Channels.newOutputStream(channel);
    CodedOutputStream cos = CodedOutputStream.newInstance(os);
    cos.writeRawVarint32(totalSize); 
    //Integer.t
    //os.write(Bytes.toBytes(totalSize));
    // This allocates a buffer that is the size of the message internally.
    header.writeDelimitedTo(os);
    if (body != null) 
      body.writeDelimitedTo(os);
    
    os.flush();
    return totalSize;
  }
  /*
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
  }*/
}
