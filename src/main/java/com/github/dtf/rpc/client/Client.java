package com.github.dtf.rpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.common.http.NetUtils;
import com.github.dtf.conf.Configuration;
import com.github.dtf.rpc.RpcType;
import com.github.dtf.rpc.Writable;
import com.github.dtf.rpc.server.Server;
import com.github.dtf.security.UserGroupInformation;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
public class Client {
  
  public static final Log LOG = LogFactory.getLog(Client.class);

  private Hashtable<ConnectionId, Connection> connections = new Hashtable<ConnectionId, Connection>();

  private AtomicBoolean running = new AtomicBoolean(true); // if client runs

  private SocketFactory socketFactory;           // how to create sockets
  
  private int refCount = 1;

  private Configuration conf;

  private Class<? extends Writable> valueClass;
  
  public final static int PING_CALL_ID = -1;
  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  final public static void setPingInterval(Configuration conf, int pingInterval) {
//    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  final static int getPingInterval(Configuration conf) {
	  return 3000;
//    return conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
//        CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
  }

  /**
   * The time after which a RPC will timeout.
   * If ping is not enabled (via ipc.client.ping), then the timeout value is the 
   * same as the pingInterval.
   * If ping is enabled, then there is no timeout value.
   * 
   * @param conf Configuration
   * @return the timeout period in milliseconds. -1 if no timeout value is set
   */
  final public static int getTimeout(Configuration conf) {
//    if (!conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true)) {
//      return getPingInterval(conf);
//    }
    return -1;
  }
  
  /**
   * Increment this client's reference count
   *
   */
  synchronized void incCount() {
    refCount++;
  }
  
  /**
   * Decrement this client's reference count
   *
   */
  synchronized void decCount() {
    refCount--;
  }
  
  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized boolean isZeroReference() {
    return refCount==0;
  }

  

  /** Construct an IPC client whose values are of the given {@link Writable}
   * class. */
  public Client(Class<? extends Writable> valueClass, Configuration conf, 
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.conf = conf;
    this.socketFactory = factory;
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass
   * @param conf
   */
  public Client(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory());
  }
 
  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }
    
    // wake up all connections
    synchronized (connections) {
      for (Connection conn : connections.values()) {
        conn.interrupt();
      }
    }
    
    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Same as {@link #call(RpcType, Writable, ConnectionId)}
   *  for RPC_BUILTIN
   */
//  public Writable call(Writable param, InetSocketAddress address)
//  throws InterruptedException, IOException {
//    return call(RpcType.RPC_BUILTIN, param, address);
//  }
  /**
   * Same as {@link #call(RpcType, Writable, InetSocketAddress, 
   * Class, UserGroupInformation, int, Configuration)}
   * except that rpcKind is writable.
   */
  public Writable call(Writable param, InetSocketAddress addr, 
      Class<?> protocol, UserGroupInformation ticket,
      int rpcTimeout, Configuration conf)  
      throws InterruptedException, IOException {
//        ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
//        ticket, rpcTimeout, conf);
      ConnectionId remoteId = new ConnectionId(addr, protocol);
    return call(RpcType.RPC_BUILTIN, param, remoteId);
  }
  
  /**
   * Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, <code>rpcTimeout</code> as
   * timeout and <code>conf</code> as conf for this connection, returning the
   * value. Throws exceptions if there are network problems or if the remote
   * code threw an exception.
   */
  public Writable call(RpcType rpcKind, Writable param, InetSocketAddress addr, 
                       Class<?> protocol, UserGroupInformation ticket,
                       int rpcTimeout, Configuration conf)  
                       throws InterruptedException, IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        rpcTimeout, conf);
    return call(rpcKind, param, remoteId);
  }
  
  /**
   * Same as {link {@link #call(RpcType, Writable, ConnectionId)}
   * except the rpcKind is RPC_BUILTIN
   */
  public Writable call(Writable param, ConnectionId remoteId)  
      throws InterruptedException, IOException {
     return call(RpcType.RPC_BUILTIN, param, remoteId);
  }
  
  /** 
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   * 
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   */
  public Writable call(RpcType rpcKind, Writable rpcRequest,
      ConnectionId remoteId) throws InterruptedException, IOException {
    Call call = new Call(rpcKind, rpcRequest, this);
    Connection connection = getConnection(remoteId, call);
    connection.sendParam(call);                 // send the parameter
    boolean interrupted = false;
    synchronized (call) {
      while (!call.done) {
        try {
          call.wait();                           // wait for the result
        } catch (InterruptedException ie) {
          // save the fact that we were interrupted
          interrupted = true;
        }
      }

      if (interrupted) {
        // set the interrupt flag now that we are done waiting
        Thread.currentThread().interrupt();
      }

      if (call.error != null) {
        /*if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception
          InetSocketAddress address = connection.getRemoteAddress();
          throw NetUtils.wrapException(address.getHostName(),
                  address.getPort(),
                  NetUtils.getHostname(),
                  0,
                  call.error);
        }*/
    	  return null;
      } else {
        return call.rpcResponse;
      }
    }
  }

  // for unit testing only
  Set<ConnectionId> getConnectionIds() {
    synchronized (connections) {
      return connections.keySet();
    }
  }
  
  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given ConnectionId are reused. */
  private Connection getConnection(ConnectionId remoteId,
                                   Call call)
                                   throws IOException, InterruptedException {
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    do {
      synchronized (connections) {
        connection = connections.get(remoteId);
        if (connection == null) {
          connection = new Connection(this, remoteId);
          connections.put(remoteId, connection);
        }
      }
    } while (!connection.addCall(call));
    
    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams();
    return connection;
  }
  
  int counter = 0;
  synchronized int getCounter(){
	return counter ++;  
  }

	public Hashtable getConnections() {
		return connections;
	}
}
