/**
 * 
 */
package com.chicm.cmraft.rpc;

import java.nio.channels.SocketChannel;

import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService.BlockingInterface;

/**
 * @author chicm
 *
 */
public interface Connection {
  BlockingInterface getService();
  SocketChannel getChannel();
  void close();
}
