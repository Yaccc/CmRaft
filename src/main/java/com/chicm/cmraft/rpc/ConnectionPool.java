package com.chicm.cmraft.rpc;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService;
import com.chicm.cmraft.protobuf.generated.RaftProtos.RaftService.BlockingInterface;
import com.google.protobuf.BlockingRpcChannel;

public class ConnectionPool {
  static final Log LOG = LogFactory.getLog(ConnectionPool.class);
  private volatile List<Connection> connections = null;
  private int maxSize = 10;
  private int initialSize = 5;
  private int used = 0;
  private InetSocketAddress isa = null;
  
  private ConnectionPool(InetSocketAddress isa, int initialSize, int maxSize) {
    this.initialSize = initialSize;
    this.maxSize = maxSize; 
    this.isa = isa;
    connections = new ArrayList<Connection>();
  }
  
  public static ConnectionPool createConnectionPool(InetSocketAddress isa, int initialSize, int maxSize) {
    final ConnectionPool pool = new ConnectionPool(isa, initialSize, maxSize);
    for(int i = 0; i < initialSize; i++) {
      pool.createConnection();
    }
    return pool;
  }
  
  public synchronized Connection getConnection () {
    LOG.debug("getConnection1: used:" + used 
      + " size:" + connections.size() + " maxsize:" +maxSize  + " initialSize:" + initialSize);
    
    if(connections.size() == 0 && used >= initialSize && used < maxSize) {
          if(createConnection() == null) {
            LOG.error("create connection failed");
            return null;
          }
    }
    if(connections != null && connections.size() > 0) {
      used ++;
      return connections.remove(0);
    }
    
    LOG.debug("getConnection2: return null: used:" + used 
      + " size:" + connections.size() + " maxsize:" +maxSize  + " initialSize:" + initialSize);
    return null;
  }
  
  public synchronized void freeConnection(Connection connection) {
    
    LOG.debug("freeConnection: used:" + used 
      + " size:" + connections.size() + " maxsize:" +maxSize  + " initialSize:" + initialSize);
    if(connection != null) {
      connections.add(connection);
      used--;
    }
  }
  
  private Connection createConnection() {
    Connection conn = null;
    try  {
      LOG.debug("creating conn:");
      SocketChannel channel = SocketChannel.open();
      boolean connected = channel.connect(isa);
      LOG.debug("client connected:" + connected + " " + channel);
      
      BlockingRpcChannel c = RaftRpcClient.createBlockingRpcChannel(channel);
      BlockingInterface service =  RaftService.newBlockingStub(c);
      
      conn = new ConnectionImpl(channel, service, this);
      connections.add(conn);
    
    } catch(Exception e) {
      e.printStackTrace(System.out);
    }
    return conn;
  }
  
  class ConnectionImpl implements Connection {
    private SocketChannel channel = null;
    private BlockingInterface service = null;
    private ConnectionPool pool = null;
    
    public ConnectionImpl(SocketChannel channel, BlockingInterface service, ConnectionPool pool) {
      this.channel = channel;
      this.service = service;
      this.pool = pool;
    }
    
    @Override
    public BlockingInterface getService() {
      return service;
    }
    
    @Override
    public SocketChannel getChannel() {
      return channel;
    }
    
    @Override
    public void close() {
      pool.freeConnection(this);
    }
  }

}
