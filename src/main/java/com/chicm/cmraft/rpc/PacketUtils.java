/**
* Copyright 2014 The CmRaft Project
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at:
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations
* under the License.
*/

package com.chicm.cmraft.rpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.RequestHeader;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ResponseHeader;
import com.google.protobuf.BlockingService;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message.Builder;

/**
 * Static utility methods dealing with protobuf RPC packets. 
 * @author chicm
 *
 */
public class PacketUtils {
  static final Log LOG = LogFactory.getLog(PacketUtils.class);
  static final int DEFAULT_BYTEBUFFER_SIZE = 1000;
  static final int MESSAGE_LENGHT_FIELD_SIZE = 4;
  static final int DEFAULT_CHANNEL_READ_RETRIES = 5;
  public static int TEST_PADDING_LEN = 0;
  
    
  public static byte[] int2Bytes(int val) {
    byte [] b = new byte[4];
    for(int i = 3; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }
   
  public static int bytes2Int(byte[] bytes) {
    int n = 0;
    for(int i = 0; i < 4; i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }
  
  public static void writeIntToStream(int n, OutputStream os)
    throws IOException {
    byte[] b = int2Bytes(n);
    os.write(b);
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
    //LOG.debug("total size:" + totalSize);
    long t = System.currentTimeMillis();
    byte btest[] = null;
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
    
    if(LOG.isTraceEnabled()) {
      LOG.trace("Write Rpc message to socket, takes " + (System.currentTimeMillis() -t) + " ms, size " + totalSize);
      LOG.trace("message:" + body);
    }
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
    LOG.debug("" + (System.currentTimeMillis() -t) + " ms");
    LOG.debug("flushed:" + totalSize);
    return totalSize;
  }

  public static RpcCall parseRpcRequestFromChannel (AsynchronousSocketChannel channel, BlockingService service) 
    throws InterruptedException, ExecutionException, IOException {
    
    RpcCall call = null;
      long t = System.currentTimeMillis();
      InputStream in = Channels.newInputStream(channel);
      byte[] datasize = new byte[MESSAGE_LENGHT_FIELD_SIZE];
      in.read(datasize);
      int nDataSize = bytes2Int(datasize);
      
      int len = 0;
      ByteBuffer buf = ByteBuffer.allocateDirect(nDataSize);
      for ( ;len < nDataSize; ) {
        len += channel.read(buf).get();
      }
      if(len < nDataSize) {
        LOG.error("SOCKET READ FAILED, len:" + len);
        return call;
      }
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
      //LOG.debug("header parsed:" + header.toString());
      
      MethodDescriptor md = service.getDescriptorForType().findMethodByName(header.getRequestName());
      Builder builder = service.getRequestPrototype(md).newBuilderForType();
      Message body = null;
      if (builder != null) {
        body = builder.mergeFrom(data, offset, bodySize).build();
        //LOG.debug("server : request parsed:" + body.toString());
      }
      call = new RpcCall(header.getId(), header, body, md);
      if(LOG.isTraceEnabled()) {
        LOG.trace("Parse Rpc request from socket: " + call.getCallId() 
          + ", takes" + (System.currentTimeMillis() -t) + " ms");
      }

    return call;
  }
  
  public static RpcCall parseRpcResponseFromChannel (AsynchronousSocketChannel channel, BlockingService service) 
      throws InterruptedException, ExecutionException, IOException {
      RpcCall call = null;
        long t = System.currentTimeMillis();
        InputStream in = Channels.newInputStream(channel);
        byte[] datasize = new byte[MESSAGE_LENGHT_FIELD_SIZE];
        in.read(datasize);
        int nDataSize = bytes2Int(datasize);
        
        LOG.debug("message size: " + nDataSize);
        
        int len = 0;
        ByteBuffer buf = ByteBuffer.allocateDirect(nDataSize);
        for ( ;len < nDataSize; ) {
          len += channel.read(buf).get();
        }
        if(len < nDataSize) {
          LOG.error("SOCKET READ FAILED, len:" + len);
          return call;
        }
        byte[] data = new byte[nDataSize];
        buf.flip();
        buf.get(data);
        int offset = 0;
        CodedInputStream cis = CodedInputStream.newInstance(data, offset, nDataSize - offset);
        int headerSize =  cis.readRawVarint32();
        offset += cis.getTotalBytesRead();
        ResponseHeader header = ResponseHeader.newBuilder().mergeFrom(data, offset, headerSize ).build();
        
        offset += headerSize;
        cis.skipRawBytes(headerSize);
        cis.resetSizeCounter();
        int bodySize = cis.readRawVarint32();
        offset += cis.getTotalBytesRead();
        
        MethodDescriptor md = service.getDescriptorForType().findMethodByName(header.getResponseName());
        Builder builder = service.getResponsePrototype(md).newBuilderForType();
        Message body = null;
        if (builder != null) {
          body = builder.mergeFrom(data, offset, bodySize).build();
        }
        call = new RpcCall(header.getId(), header, body, md);
        if(LOG.isTraceEnabled()) {
          LOG.trace("Parse Rpc response from socket: " + call.getCallId() 
            + ", takes" + (System.currentTimeMillis() -t) + " ms");
        }

      return call;
    }
   
}
