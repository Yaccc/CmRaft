/**
 * 
 */
package com.chicm.cmraft.rpc;

import java.nio.channels.AsynchronousSocketChannel;

import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService.BlockingInterface;

/**
 * @author chicm
 *
 */
public interface Connection {
  BlockingInterface getService();
  AsynchronousSocketChannel getChannel();
  void close();
}
