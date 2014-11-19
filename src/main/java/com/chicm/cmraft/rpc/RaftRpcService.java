package com.chicm.cmraft.rpc;

import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.HeartBeatResponse;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerListRequest;
import com.chicm.cmraft.protobuf.generated.RaftProtos.ServerListResponse;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class RaftRpcService implements RaftService.BlockingInterface{
  
  public HeartBeatResponse beatHeart(RpcController controller, HeartBeatRequest request)
      throws ServiceException {
    return null;
  }

  public ServerListResponse listServer(RpcController controller, ServerListRequest request)
      throws ServiceException {
    return null;
  }

}
