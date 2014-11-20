package com.chicm.cmraft.rpc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerListRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerListResponse;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RaftRpcService implements RaftService.BlockingInterface{
  static final Log LOG = LogFactory.getLog(RaftRpcService.class);
  
  public HeartBeatResponse beatHeart(RpcController controller, HeartBeatRequest request)
      throws ServiceException {
    return null;
  }

  public ServerListResponse listServer(RpcController controller, ServerListRequest request)
      throws ServiceException {
    return null;
  }

  public BlockingService getService() {
    return RaftService.newReflectiveBlockingService(this);
  }
}
