package com.chicm.cmraft.rpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;
import java.nio.channels.ReadPendingException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.google.protobuf.BlockingService;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message.Builder;

public class RpcUtils {
  static final Log LOG = LogFactory.getLog(RpcUtils.class);
  static final int DEFAULT_BYTEBUFFER_SIZE = 1000;
  static final int MESSAGE_LENGHT_FIELD_SIZE = 4;
  static final int DEFAULT_CHANNEL_READ_RETRIES = 5;
  
  public static byte[] int2Bytes(int n) {
    byte[] bytes = new byte[4];
    for (int i = 0; i < 4; i++) {
        bytes[i] = (byte)(n >>> (i * 8));
    }
    return bytes;
  }
  
  public static int bytes2Int(byte[] b) {
    int n = 0;
    for (int i = 0; i < 4; i++) {
      n |= ((int)b[i]) << (i*8);
    }
    return n;
  }
  
  public static void writeIntToStream(int n, OutputStream os)
    throws IOException {
    byte[] b = int2Bytes(n);
    os.write(b);
  }
  
  //public static int readInt()
  
  public static void main (String[] args) {
    int n = 12345;
    byte[] b = int2Bytes(n);
    int n2 = bytes2Int(b);
    System.out.println(""+ n2);
  }
  
  
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
    // writing total size so that server can read all request data in one read
    LOG.debug("total size:" + totalSize);
    OutputStream os = Channels.newOutputStream(channel);
    /*
    CodedOutputStream cos = CodedOutputStream.newInstance(os);
    cos.writeRawVarint32(totalSize); 
    */
    writeIntToStream(totalSize, os);

    header.writeDelimitedTo(os);
    if (body != null) 
      body.writeDelimitedTo(os);
    
    os.flush();
    LOG.debug("flushed:" + totalSize);
    return totalSize;
  }

  public static RpcCall parseRpcFromChannel (AsynchronousSocketChannel channel, BlockingService service) 
    throws InterruptedException, ExecutionException {
    RpcCall call = null;
    try {  
      
      InputStream in = Channels.newInputStream(channel);
      byte[] datasize = new byte[MESSAGE_LENGHT_FIELD_SIZE];
      in.read(datasize);
      int nDataSize = bytes2Int(datasize);
      
      LOG.debug("message size: " + nDataSize);
      /*
      ByteBuffer buf4Size = ByteBuffer.allocate(1000);
      
      int len = channel.read(buf4Size).get();
      if(len != MESSAGE_LENGHT_FIELD_SIZE) {
        LOG.error("server: connection closed by client, len:" + len);
        //return call;
      }   
      LOG.debug("SERVER READ LEN:" + len);
      buf4Size.flip();
      byte[] data4Size = new byte[len];
      buf4Size.get(data4Size);
      int messageLen = bytes2Int(data4Size);
      LOG.debug("message size: " + messageLen);
      */
      int len = 0;
      ByteBuffer buf = ByteBuffer.allocateDirect(nDataSize);
      for ( int i = 0; i < DEFAULT_CHANNEL_READ_RETRIES; i++) {
        len += channel.read(buf).get(3, TimeUnit.SECONDS);
      }
      LOG.debug("len:" + len);
      if(len < nDataSize) {
        LOG.error("SOCKET READ FAILED, len:" + len);
        return call;
      }
      //byte[] data = buf.array();
      byte[] data = new byte[nDataSize];
      buf.flip();
      buf.get(data);
      
      int offset = 0;
      CodedInputStream cis = CodedInputStream.newInstance(data, offset, nDataSize - offset);
      int headerSize =  cis.readRawVarint32();
      offset += cis.getTotalBytesRead();
      
      RequestHeader header = RequestHeader.newBuilder().mergeFrom(data, offset, headerSize ).build();
      
      offset += headerSize;
      cis.skipRawBytes(headerSize);
      cis.resetSizeCounter();
      int bodySize = cis.readRawVarint32();
      offset += cis.getTotalBytesRead();
      
      LOG.debug("header parsed:" + header.toString());
      
      MethodDescriptor md = service.getDescriptorForType().findMethodByName(header.getRequestName());
      Builder builder = service.getRequestPrototype(md).newBuilderForType();
      Message body = null;
      if (builder != null) {
        body = builder.mergeFrom(data, offset, bodySize).build();
        LOG.debug("server : request parsed:" + body.toString());
        //Message response = getService().callBlockingMethod(md, null, request);
        LOG.debug("server method called:" + header.getRequestName());
        //System.out.println("Map:" + handle.getMap());
      }
      
      call = new RpcCall(header, body);
    
  } catch (InterruptedException | ExecutionException e) {
    throw e;
  } /*catch (ReadPendingException e) {
    System.out.println(e);
  } */catch(Exception e) {
    e.printStackTrace(System.out);
  } 
    return call;
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
