package com.chicm.cmraft.rpc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
  static final int TEST_PADDING_LEN = 0;
  
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
  
  public static int writeRpc(AsynchronousSocketChannel channel, Message header, Message body) 
    throws IOException, InterruptedException, ExecutionException {
    int totalSize = getTotalSizeofMessages(header, body);
    return writeRpc(channel, header, body, totalSize);
  }
  
  private static int writeRpc(AsynchronousSocketChannel channel, Message header, Message body, 
      int totalSize) throws IOException, InterruptedException, ExecutionException {
    // writing total size so that server can read all request data in one read
    LOG.debug("total size:" + totalSize);
    long t = System.currentTimeMillis();
    byte btest[];
    if(TEST_PADDING_LEN > 0)  {
      btest = new byte[TEST_PADDING_LEN];
      totalSize += btest.length;
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    writeIntToStream(totalSize, bos);

    header.writeDelimitedTo(bos);
    if (body != null) 
      body.writeDelimitedTo(bos);
    
    bos.flush();
    byte[] b = bos.toByteArray();
    ByteBuffer buf = ByteBuffer.allocateDirect(totalSize + 4);
    buf.put(b);
    
    if(TEST_PADDING_LEN > 0)  {
        buf.put(btest);
    }
    buf.flip();
    channel.write(buf).get();
    LOG.debug("2222: " + (System.currentTimeMillis() -t) + " ms");
    LOG.debug("flushed:" + totalSize);
    return totalSize;
  }
  
  private static int writeRpc_backup(SocketChannel channel, Message header, Message body, 
      int totalSize) throws IOException {
    // writing total size so that server can read all request data in one read
    LOG.debug("total size:" + totalSize);
    long t = System.currentTimeMillis();
    OutputStream os = Channels.newOutputStream(channel);
    writeIntToStream(totalSize, os);
    header.writeDelimitedTo(os);
    if (body != null) 
      body.writeDelimitedTo(os);
    os.flush();
    LOG.debug("2222: " + (System.currentTimeMillis() -t) + " ms");
    LOG.debug("flushed:" + totalSize);
    return totalSize;
  }

  public static RpcCall parseRpcFromChannel (AsynchronousSocketChannel channel, BlockingService service) 
    throws InterruptedException, ExecutionException {
    RpcCall call = null;
    try {  
      long t = System.currentTimeMillis();
      InputStream in = Channels.newInputStream(channel);
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
      byte[] datasize = new byte[MESSAGE_LENGHT_FIELD_SIZE];
      in.read(datasize);
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
      int nDataSize = bytes2Int(datasize);
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
      
      LOG.debug("message size: " + nDataSize);
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
      
      int len = 0;
      ByteBuffer buf = ByteBuffer.allocateDirect(nDataSize);
      for ( ;len < nDataSize; ) {
        len += channel.read(buf).get();
      }
      LOG.debug("len:" + len);
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
      if(len < nDataSize) {
        LOG.error("SOCKET READ FAILED, len:" + len);
        return call;
      }
      //byte[] data = buf.array();
      byte[] data = new byte[nDataSize];
      buf.flip();
      buf.get(data);
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
      int offset = 0;
      CodedInputStream cis = CodedInputStream.newInstance(data, offset, nDataSize - offset);
      int headerSize =  cis.readRawVarint32();
      offset += cis.getTotalBytesRead();
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
      RequestHeader header = RequestHeader.newBuilder().mergeFrom(data, offset, headerSize ).build();
      
      offset += headerSize;
      cis.skipRawBytes(headerSize);
      cis.resetSizeCounter();
      int bodySize = cis.readRawVarint32();
      offset += cis.getTotalBytesRead();
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
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
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
      call = new RpcCall(header.getId(), header, body, md);
      LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
    
  } catch (InterruptedException | ExecutionException e) {
    throw e;
  } /*catch (ReadPendingException e) {
    System.out.println(e);
  } */catch(Exception e) {
    e.printStackTrace(System.out);
  } 
    return call;
  }
  
  public static RpcCall parseRpcFromChannel2 (AsynchronousSocketChannel channel, BlockingService service) 
      throws InterruptedException, ExecutionException {
      RpcCall call = null;
      try {  
        long t = System.currentTimeMillis();
        /*
        InputStream in = Channels.newInputStream(channel);
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
        byte[] datasize = new byte[MESSAGE_LENGHT_FIELD_SIZE];
        in.read(datasize);
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
        int nDataSize = bytes2Int(datasize);
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
        
        LOG.debug("message size: " + nDataSize);
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");*/
        
        int len = 0;
        ByteBuffer buf = ByteBuffer.allocateDirect(DEFAULT_BYTEBUFFER_SIZE);
        
        len = channel.read(buf).get();
        buf.flip();
        byte[] datasize = new byte[MESSAGE_LENGHT_FIELD_SIZE];
        buf.get(datasize);
        int nDataSize = bytes2Int(datasize);
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
        LOG.debug("message size: " + nDataSize);
        return call;
        /*
        buf = ByteBuffer.allocateDirect(nDataSize);
        for ( int i = 0; i < DEFAULT_CHANNEL_READ_RETRIES; i++) {
          len += channel.read(buf).get(3, TimeUnit.SECONDS);
        }
        LOG.debug("len:" + len);
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
        if(len < nDataSize) {
          LOG.error("SOCKET READ FAILED, len:" + len);
          return call;
        }
        //byte[] data = buf.array();
        byte[] data = new byte[nDataSize];
        buf.flip();
        buf.get(data);
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
        int offset = 0;
        CodedInputStream cis = CodedInputStream.newInstance(data, offset, nDataSize - offset);
        int headerSize =  cis.readRawVarint32();
        offset += cis.getTotalBytesRead();
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
        RequestHeader header = RequestHeader.newBuilder().mergeFrom(data, offset, headerSize ).build();
        
        offset += headerSize;
        cis.skipRawBytes(headerSize);
        cis.resetSizeCounter();
        int bodySize = cis.readRawVarint32();
        offset += cis.getTotalBytesRead();
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
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
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
        call = new RpcCall(header, body);
        LOG.debug("1111: " + (System.currentTimeMillis() -t) + " ms");
      */
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
