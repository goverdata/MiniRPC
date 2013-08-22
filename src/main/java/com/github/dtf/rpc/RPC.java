package com.github.dtf.rpc;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.*;

//import org.apache.hadoop.HadoopIllegalArgumentException;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.io.retry.RetryPolicy;
//import org.apache.hadoop.ipc.Client.ConnectionId;
//import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolInfoService;
//import org.apache.hadoop.net.NetUtils;
//import org.apache.hadoop.security.SaslRpcServer;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.hadoop.security.token.SecretManager;
//import org.apache.hadoop.security.token.TokenIdentifier;
//import org.apache.hadoop.conf.*;
//import org.apache.hadoop.util.ReflectionUtils;
//import org.apache.hadoop.util.Time;

import com.github.dtf.conf.Configuration;
import com.github.dtf.exception.HadoopIllegalArgumentException;
import com.github.dtf.protocol.ProtocolInfo;
import com.github.dtf.protocol.VersionedProtocol;
import com.github.dtf.rpc.server.Server;
import com.github.dtf.utils.ReflectionUtils;
import com.google.protobuf.BlockingService;

/** A simple RPC mechanism.
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
public class RPC {
  public enum Type {
    RPC_BUILTIN ((short) 1),         // Used for built in calls by tests
    RPC_WRITABLE ((short) 2),        // Use WritableRpcEngine 
    RPC_PROTOCOL_BUFFER ((short) 3); // Use ProtobufRpcEngine
    final static short MAX_INDEX = RPC_PROTOCOL_BUFFER.value; // used for array size
    private static final short FIRST_INDEX = RPC_BUILTIN.value;    
    public final short value; //TODO make it private

    Type(short val) {
      this.value = val;
    } 
  }

  
  static final Log LOG = LogFactory.getLog(RPC.class);
  
  /**
   * Get all superInterfaces that extend VersionedProtocol
   * @param childInterfaces
   * @return the super interfaces that extend VersionedProtocol
   */
  static Class<?>[] getSuperInterfaces(Class<?>[] childInterfaces) {
    List<Class<?>> allInterfaces = new ArrayList<Class<?>>();

    for (Class<?> childInterface : childInterfaces) {
      if (VersionedProtocol.class.isAssignableFrom(childInterface)) {
          allInterfaces.add(childInterface);
          allInterfaces.addAll(
              Arrays.asList(
                  getSuperInterfaces(childInterface.getInterfaces())));
      } else {
        LOG.warn("Interface " + childInterface +
              " ignored because it does not extend VersionedProtocol");
      }
    }
    return allInterfaces.toArray(new Class[allInterfaces.size()]);
  }
  
  /**
   * Get all interfaces that the given protocol implements or extends
   * which are assignable from VersionedProtocol.
   */
  static Class<?>[] getProtocolInterfaces(Class<?> protocol) {
    Class<?>[] interfaces  = protocol.getInterfaces();
    return getSuperInterfaces(interfaces);
  }
  
  /**
   * Get the protocol name.
   *  If the protocol class has a ProtocolAnnotation, then get the protocol
   *  name from the annotation; otherwise the class name is the protocol name.
   */
  static public String getProtocolName(Class<?> protocol) {
    if (protocol == null) {
      return null;
    }
    ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
    return  (anno == null) ? protocol.getName() : anno.protocolName();
  }
  
  /**
   * Get the protocol version from protocol class.
   * If the protocol class has a ProtocolAnnotation, then get the protocol
   * name from the annotation; otherwise the class name is the protocol name.
   */
  static public long getProtocolVersion(Class<?> protocol) {
    if (protocol == null) {
      throw new IllegalArgumentException("Null protocol");
    }
    long version;
    ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
    if (anno != null) {
      version = anno.protocolVersion();
      if (version != -1)
        return version;
    }
    try {
      Field versionField = protocol.getField("versionID");
      versionField.setAccessible(true);
      return versionField.getLong(protocol);
    } catch (NoSuchFieldException ex) {
      throw new RuntimeException(ex);
    } catch (IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
  }

  private RPC() {}                                  // no public ctor

  // cache of RpcEngines by protocol
  private static final Map<Class<?>,RpcEngine> PROTOCOL_ENGINES
    = new HashMap<Class<?>,RpcEngine>();

  private static final String ENGINE_PROP = "rpc.engine";

  /**
   * Set a protocol to use a non-default RpcEngine.
   * @param conf configuration to use
   * @param protocol the protocol interface
   * @param engine the RpcEngine impl
   */
  public static void setProtocolEngine(Configuration conf,
                                Class<?> protocol, Class<?> engine) {
    conf.setClass(ENGINE_PROP+"."+protocol.getName(), engine, RpcEngine.class);
  }

  // return the RpcEngine configured to handle a protocol
  static synchronized RpcEngine getProtocolEngine(Class<?> protocol,
      Configuration conf) {
    RpcEngine engine = PROTOCOL_ENGINES.get(protocol);
    if (engine == null) {
      Class<?> impl = conf.getClass(ENGINE_PROP+"."+protocol.getName(),
                                    WritableRpcEngine.class);
      engine = (RpcEngine)ReflectionUtils.newInstance(impl, conf);
      PROTOCOL_ENGINES.put(protocol, engine);
    }
    return engine;
  }

  /**
   * A version mismatch for the RPC protocol.
   */
  public static class VersionMismatch extends IOException {
    private static final long serialVersionUID = 0;

    private String interfaceName;
    private long clientVersion;
    private long serverVersion;
    
    /**
     * Create a version mismatch exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }
    
    /**
     * Get the interface name
     * @return the java class name 
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }
    
    /**
     * Get the client's preferred version
     */
    public long getClientVersion() {
      return clientVersion;
    }
    
    /**
     * Get the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
  }

  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> T waitForProxy(
      Class<T> protocol,
      long clientVersion,
      InetSocketAddress addr,
      Configuration conf
      ) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
                             long clientVersion,
                             InetSocketAddress addr,
                             Configuration conf) throws IOException {
    return waitForProtocolProxy(
        protocol, clientVersion, addr, conf, Long.MAX_VALUE);
  }

  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> T waitForProxy(Class<T> protocol, long clientVersion,
                             InetSocketAddress addr, Configuration conf,
                             long connTimeout) throws IOException { 
    return waitForProtocolProxy(protocol, clientVersion, addr,
        conf, connTimeout).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param connTimeout time in milliseconds before giving up
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
                             long clientVersion,
                             InetSocketAddress addr, Configuration conf,
                             long connTimeout) throws IOException { 
    return waitForProtocolProxy(protocol, clientVersion, addr, conf, 0, null, connTimeout);
  }
  
  /**
   * Get a proxy connection to a remote server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param rpcTimeout timeout for each RPC
   * @param timeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> T waitForProxy(Class<T> protocol,
                             long clientVersion,
                             InetSocketAddress addr, Configuration conf,
                             int rpcTimeout,
                             long timeout) throws IOException {
    return waitForProtocolProxy(protocol, clientVersion, addr,
        conf, rpcTimeout, null, timeout).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param rpcTimeout timeout for each RPC
   * @param timeout time in milliseconds before giving up
   * @return the proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
                               long clientVersion,
                               InetSocketAddress addr, Configuration conf,
                               int rpcTimeout,
                               RetryPolicy connectionRetryPolicy,
                               long timeout) throws IOException { 
    long startTime = Time.now();
    IOException ioe;
    while (true) {
      try {
        return getProtocolProxy(protocol, clientVersion, addr, 
            UserGroupInformation.getCurrentUser(), conf, NetUtils
            .getDefaultSocketFactory(conf), rpcTimeout, connectionRetryPolicy);
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
        ioe = se;
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      } catch(NoRouteToHostException nrthe) { // perhaps a VIP is failing over
        LOG.info("No route to host for server: " + addr);
        ioe = nrthe;
      }
      // check if timed out
      if (Time.now()-timeout >= startTime) {
        throw ioe;
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }

  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  public static <T> T getProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr, Configuration conf,
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, conf, factory).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param conf configuration to use
   * @param factory socket factory
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr, Configuration conf,
                                SocketFactory factory) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return getProtocolProxy(protocol, clientVersion, addr, ugi, conf, factory);
  }
  
  /** Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address. 
   * @param <T>*/
  public static <T> T getProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, ticket, conf, factory).getProxy();
  }

  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol class
   * @param clientVersion client version
   * @param addr remote address
   * @param ticket user group information
   * @param conf configuration to use
   * @param factory socket factory
   * @return the protocol proxy
   * @throws IOException if the far end through a RemoteException
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory) throws IOException {
    return getProtocolProxy(
        protocol, clientVersion, addr, ticket, conf, factory, 0, null);
  }
  
  /**
   * Construct a client-side proxy that implements the named protocol,
   * talking to a server at the named address.
   * @param <T>
   * 
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @return the proxy
   * @throws IOException if any error occurs
   */
  public static <T> T getProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout) throws IOException {
    return getProtocolProxy(protocol, clientVersion, addr, ticket,
             conf, factory, rpcTimeout, null).getProxy();
  }
  
  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol protocol
   * @param clientVersion client's version
   * @param addr server address
   * @param ticket security ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout max time for each rpc; 0 means no timeout
   * @return the proxy
   * @throws IOException if any error occurs
   */
   public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr,
                                UserGroupInformation ticket,
                                Configuration conf,
                                SocketFactory factory,
                                int rpcTimeout,
                                RetryPolicy connectionRetryPolicy) throws IOException {    
    if (UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
    }
    return getProtocolEngine(protocol,conf).getProxy(protocol, clientVersion,
        addr, ticket, conf, factory, rpcTimeout, connectionRetryPolicy);
  }

   /**
    * Construct a client-side proxy object with the default SocketFactory
    * @param <T>
    * 
    * @param protocol
    * @param clientVersion
    * @param addr
    * @param conf
    * @return a proxy instance
    * @throws IOException
    */
   public static <T> T getProxy(Class<T> protocol,
                                 long clientVersion,
                                 InetSocketAddress addr, Configuration conf)
     throws IOException {

     return getProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
   }
  
  /**
   * Returns the server address for a given proxy.
   */
  public static InetSocketAddress getServerAddress(Object proxy) {
    return getConnectionIdForProxy(proxy).getAddress();
  }

  /**
   * Return the connection ID of the given object. If the provided object is in
   * fact a protocol translator, we'll get the connection ID of the underlying
   * proxy object.
   * 
   * @param proxy the proxy object to get the connection ID of.
   * @return the connection ID for the provided proxy object.
   */
  public static ConnectionId getConnectionIdForProxy(Object proxy) {
    if (proxy instanceof ProtocolTranslator) {
      proxy = ((ProtocolTranslator)proxy).getUnderlyingProxyObject();
    }
    RpcInvocationHandler inv = (RpcInvocationHandler) Proxy
        .getInvocationHandler(proxy);
    return inv.getConnectionId();
  }
   
  /**
   * Get a protocol proxy that contains a proxy connection to a remote server
   * and a set of methods that are supported by the server
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a protocol proxy
   * @throws IOException
   */
  public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
                                long clientVersion,
                                InetSocketAddress addr, Configuration conf)
    throws IOException {

    return getProtocolProxy(protocol, clientVersion, addr, conf, NetUtils
        .getDefaultSocketFactory(conf));
  }

  /**
   * Stop the proxy. Proxy must either implement {@link Closeable} or must have
   * associated {@link RpcInvocationHandler}.
   * 
   * @param proxy
   *          the RPC proxy object to be stopped
   * @throws HadoopIllegalArgumentException
   *           if the proxy does not implement {@link Closeable} interface or
   *           does not have closeable {@link InvocationHandler}
   */
  public static void stopProxy(Object proxy) {
    if (proxy == null) {
      throw new HadoopIllegalArgumentException(
          "Cannot close proxy since it is null");
    }
    try {
      if (proxy instanceof Closeable) {
        ((Closeable) proxy).close();
        return;
      } else {
        InvocationHandler handler = Proxy.getInvocationHandler(proxy);
        if (handler instanceof Closeable) {
          ((Closeable) handler).close();
          return;
        }
      }
    } catch (IOException e) {
      LOG.error("Closing proxy or invocation handler caused exception", e);
    } catch (IllegalArgumentException e) {
      LOG.error("RPC.stopProxy called on non proxy.", e);
    }
    
    // If you see this error on a mock object in a unit test you're
    // developing, make sure to use MockitoUtil.mockProtocol() to
    // create your mock.
    throw new HadoopIllegalArgumentException(
        "Cannot close proxy - is not Closeable or "
            + "does not provide closeable invocation handler "
            + proxy.getClass());
  }

  /** Construct a server for a protocol implementation instance. */
  public static Server getServer(Class<?> protocol,
                                 Object instance, String bindAddress,
                                 int port, Configuration conf) 
    throws IOException {
    return getServer(protocol, instance, bindAddress, port, 1, false, conf, null,
        null);
  }

  /** Construct a server for a protocol implementation instance. 
   * FIXME: Removed SecuretManager
   * */
  public static Server getServer(Class<?> protocol,
                                 Object instance, String bindAddress, int port,
                                 int numHandlers,
                                 boolean verbose, Configuration conf) 
    throws IOException {
    return getServer(protocol, instance, bindAddress, port, numHandlers, verbose,
        conf, null);
  }
  
  public static Server getServer(Class<?> protocol,
      Object instance, String bindAddress, int port,
      int numHandlers,
      boolean verbose, Configuration conf,
      String portRangeConfig) 
  throws IOException {
    return getProtocolEngine(protocol, conf)
      .getServer(protocol, instance, bindAddress, port, numHandlers, -1, -1,
                 verbose, conf, portRangeConfig);
  }

  /** Construct a server for a protocol implementation instance. */

  public static <PROTO extends VersionedProtocol, IMPL extends PROTO> 
        Server getServer(Class<PROTO> protocol,
                                 IMPL instance, String bindAddress, int port,
                                 int numHandlers, int numReaders, int queueSizePerHandler,
                                 boolean verbose, Configuration conf
                                 ) 
    throws IOException {
    
    return getProtocolEngine(protocol, conf)
      .getServer(protocol, instance, bindAddress, port, numHandlers,
                 numReaders, queueSizePerHandler, verbose, conf, null);
  }
}
