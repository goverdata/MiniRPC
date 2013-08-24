package com.github.dtf.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.classification.InterfaceAudience;
//import org.apache.hadoop.classification.InterfaceStability;
//import org.apache.hadoop.conf.Configurable;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.ObjectWritable;
//import org.apache.hadoop.io.UTF8;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.retry.RetryPolicy;
//import org.apache.hadoop.ipc.Client;
//import org.apache.hadoop.ipc.ClientCache;
//import org.apache.hadoop.ipc.ProtocolMetaInfoPB;
//import org.apache.hadoop.ipc.ProtocolProxy;
//import org.apache.hadoop.ipc.ProtocolSignature;
//import org.apache.hadoop.ipc.RPC;
//import org.apache.hadoop.ipc.RpcEngine;
//import org.apache.hadoop.ipc.RpcInvocationHandler;
//import org.apache.hadoop.ipc.VersionedProtocol;
//import org.apache.hadoop.ipc.Client.ConnectionId;
//import org.apache.hadoop.ipc.RPC.RpcInvoker;
//import org.apache.hadoop.ipc.RPC.Server.ProtoClassProtoImpl;
//import org.apache.hadoop.ipc.RPC.Server.ProtoNameVer;
//import org.apache.hadoop.ipc.RPC.Server.VerProtocolImpl;
//import org.apache.hadoop.ipc.WritableRpcEngine.Invocation;
//import org.apache.hadoop.ipc.WritableRpcEngine.Invoker;
//import org.apache.hadoop.ipc.WritableRpcEngine.Server;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.hadoop.security.token.SecretManager;
//import org.apache.hadoop.security.token.TokenIdentifier;
//import org.apache.hadoop.util.Time;

import com.github.dtf.conf.Configurable;
import com.github.dtf.conf.Configuration;
import com.github.dtf.io.ObjectWritable;
import com.github.dtf.protocol.ProtocolProxy;
import com.github.dtf.rpc.client.Client;
import com.github.dtf.rpc.client.ClientCache;
import com.github.dtf.rpc.client.ConnectionId;
import com.github.dtf.rpc.server.AbstractRpcServer;
import com.github.dtf.security.UserGroupInformation;
import com.github.dtf.transport.RetryPolicy;

public class WritableRpcEngine implements RpcEngine {
	  private static final Log LOG = LogFactory.getLog(RPC.class);
	  
	  //writableRpcVersion should be updated if there is a change
	  //in format of the rpc messages.
	  
	  // 2L - added declared class to Invocation
	  public static final long writableRpcVersion = 2L;
	  
	  /**
	   * Whether or not this class has been initialized.
	   */
	  private static boolean isInitialized = false;
	  
	  static { 
	    ensureInitialized();
	  }
	  
	  /**
	   * Initialize this class if it isn't already.
	   */
	  public static synchronized void ensureInitialized() {
	    if (!isInitialized) {
	      initialize();
	    }
	  }
	  
	  /**
	   * Register the rpcRequest deserializer for WritableRpcEngine
	   */
	  private static synchronized void initialize() {
		  com.github.dtf.rpc.server.AbstractServer.registerProtocolEngine(RPC.Type.RPC_WRITABLE,
	        Invocation.class, new Server.WritableRpcInvoker());
	    isInitialized = true;
	  }

	  
	  /** A method invocation, including the method name and its parameters.*/
	  private static class Invocation implements Writable, Configurable {
	    private String methodName;
	    private Class<?>[] parameterClasses;
	    private Object[] parameters;
	    private Configuration conf;
	    private long clientVersion;
	    private int clientMethodsHash;
	    private String declaringClassProtocolName;
	    
	    //This could be different from static writableRpcVersion when received
	    //at server, if client is using a different version.
	    private long rpcVersion;

	    @SuppressWarnings("unused") // called when deserializing an invocation
	    public Invocation() {}

	    public Invocation(Method method, Object[] parameters) {
	      this.methodName = method.getName();
	      this.parameterClasses = method.getParameterTypes();
	      this.parameters = parameters;
	      rpcVersion = writableRpcVersion;
//	      if (method.getDeclaringClass().equals(VersionedProtocol.class)) {
//	        //VersionedProtocol is exempted from version check.
//	        clientVersion = 0;
//	        clientMethodsHash = 0;
//	      } else {
//	        this.clientVersion = RPC.getProtocolVersion(method.getDeclaringClass());
//	        this.clientMethodsHash = ProtocolSignature.getFingerprint(method
//	            .getDeclaringClass().getMethods());
//	      }
	      clientVersion = 0;
	      clientMethodsHash = 0;
	      this.declaringClassProtocolName = 
	          RPC.getProtocolName(method.getDeclaringClass());
	    }

	    /** The name of the method invoked. */
	    public String getMethodName() { return methodName; }

	    /** The parameter classes. */
	    public Class<?>[] getParameterClasses() { return parameterClasses; }

	    /** The parameter instances. */
	    public Object[] getParameters() { return parameters; }
	    
	    private long getProtocolVersion() {
	      return clientVersion;
	    }

	    @SuppressWarnings("unused")
	    private int getClientMethodsHash() {
	      return clientMethodsHash;
	    }
	    
	    /**
	     * Returns the rpc version used by the client.
	     * @return rpcVersion
	     */
	    public long getRpcVersion() {
	      return rpcVersion;
	    }

	    @SuppressWarnings("deprecation")
	    public void readFields(DataInput in) throws IOException {
	      rpcVersion = in.readLong();
	      declaringClassProtocolName = UTF8.readString(in);
	      methodName = UTF8.readString(in);
	      clientVersion = in.readLong();
	      clientMethodsHash = in.readInt();
	      parameters = new Object[in.readInt()];
	      parameterClasses = new Class[parameters.length];
	      ObjectWritable objectWritable = new ObjectWritable();
	      for (int i = 0; i < parameters.length; i++) {
	        parameters[i] = 
	            ObjectWritable.readObject(in, objectWritable, this.conf);
	        parameterClasses[i] = objectWritable.getDeclaredClass();
	      }
	    }

	    @SuppressWarnings("deprecation")
	    public void write(DataOutput out) throws IOException {
	      out.writeLong(rpcVersion);
	      UTF8.writeString(out, declaringClassProtocolName);
	      UTF8.writeString(out, methodName);
	      out.writeLong(clientVersion);
	      out.writeInt(clientMethodsHash);
	      out.writeInt(parameterClasses.length);
	      for (int i = 0; i < parameterClasses.length; i++) {
	        ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
	                                   conf, true);
	      }
	    }

	    public String toString() {
	      StringBuilder buffer = new StringBuilder();
	      buffer.append(methodName);
	      buffer.append("(");
	      for (int i = 0; i < parameters.length; i++) {
	        if (i != 0)
	          buffer.append(", ");
	        buffer.append(parameters[i]);
	      }
	      buffer.append(")");
	      buffer.append(", rpc version="+rpcVersion);
	      buffer.append(", client version="+clientVersion);
	      buffer.append(", methodsFingerPrint="+clientMethodsHash);
	      return buffer.toString();
	    }

	    public void setConf(Configuration conf) {
	      this.conf = conf;
	    }

	    public Configuration getConf() {
	      return this.conf;
	    }

	  }

	  private static ClientCache CLIENTS=new ClientCache();
	  
	  private static class Invoker implements RpcInvocationHandler {
	    private ConnectionId remoteId;
	    private Client client;
	    private boolean isClosed = false;

	    public Invoker(Class<?> protocol,
	                   InetSocketAddress address,
	                   Configuration conf, SocketFactory factory,
	                   int rpcTimeout) throws IOException {
	      this.remoteId = ConnectionId.getConnectionId(address, protocol,
	          rpcTimeout, conf);
	      this.client = CLIENTS.getClient(conf, factory);
	    }

	    public Object invoke(Object proxy, Method method, Object[] args)
	      throws Throwable {
	      long startTime = 0;
	      if (LOG.isDebugEnabled()) {
	        startTime = System.currentTimeMillis();
	      }

	      //ObjectWritable value = (ObjectWritable)
	      ObjectWritable value = null;
	      client.call(RPC.Type.RPC_WRITABLE, new Invocation(method, args), remoteId);
	      if (LOG.isDebugEnabled()) {
	        long callTime = System.currentTimeMillis() - startTime;
	        LOG.debug("Call: " + method.getName() + " " + callTime);
	      }
	      return value.get();
	    }
	    
	    /* close the IPC client that's responsible for this invoker's RPCs */ 
	    synchronized public void close() {
	      if (!isClosed) {
	        isClosed = true;
	        CLIENTS.stopClient(client);
	      }
	    }

	    public ConnectionId getConnectionId() {
	      return remoteId;
	    }
	  }
	  
//	  // for unit testing only
//	  @InterfaceAudience.Private
//	  @InterfaceStability.Unstable
//	  static Client getClient(Configuration conf) {
//	    return CLIENTS.getClient(conf);
//	  }
	  
	  /** Construct a client-side proxy object that implements the named protocol,
	   * talking to a server at the named address. 
	   * @param <T>*/
	  @SuppressWarnings("unchecked")
	  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
	                         InetSocketAddress addr,
	                         Configuration conf, SocketFactory factory,
	                         int rpcTimeout, RetryPolicy connectionRetryPolicy)
	    throws IOException {    

	    if (connectionRetryPolicy != null) {
	      throw new UnsupportedOperationException(
	          "Not supported: connectionRetryPolicy=" + connectionRetryPolicy);
	    }

	    T proxy = (T) Proxy.newProxyInstance(protocol.getClassLoader(),
	        new Class[] { protocol }, new Invoker(protocol, addr, conf,
	            factory, rpcTimeout));
	    return new ProtocolProxy<T>(protocol, proxy, true);
	  }
	  
	  /* Construct a server for a protocol implementation instance listening on a
	   * port and address. */
	  public AbstractRpcServer getServer(Class<?> protocolClass,
	                      Object protocolImpl, String bindAddress, int port,
	                      int numHandlers, int numReaders, int queueSizePerHandler,
	                      boolean verbose, Configuration conf,
	                      String portRangeConfig) 
	    throws IOException {
	    return new Server(protocolClass, protocolImpl, conf, bindAddress, port,
	        numHandlers, numReaders, queueSizePerHandler, verbose, 
	        portRangeConfig);
	  }


	  /** An RPC Server. */
	  public static class Server extends AbstractRpcServer {
	    /** Construct an RPC server.
	     * @param protocolClass class
	     * @param protocolImpl the instance whose methods will be called
	     * @param conf the configuration to use
	     * @param bindAddress the address to bind on to listen for connection
	     * @param port the port to listen for connections on
	     */
	    public Server(Class<?> protocolClass, Object protocolImpl, 
	        Configuration conf, String bindAddress, int port) 
	      throws IOException {
	      this(protocolClass, protocolImpl, conf,  bindAddress, port, 1, -1, -1,
	          false, null);
	    }
	    
	    /** 
	     * Construct an RPC server.
	     * @param protocolClass - the protocol being registered
	     *     can be null for compatibility with old usage (see below for details)
	     * @param protocolImpl the protocol impl that will be called
	     * @param conf the configuration to use
	     * @param bindAddress the address to bind on to listen for connection
	     * @param port the port to listen for connections on
	     * @param numHandlers the number of method handler threads to run
	     * @param verbose whether each call should be logged
	     */
	    public Server(Class<?> protocolClass, Object protocolImpl,
	        Configuration conf, String bindAddress,  int port,
	        int numHandlers, int numReaders, int queueSizePerHandler, 
	        boolean verbose,
	        String portRangeConfig) 
	        throws IOException {
	      super(bindAddress, port, null, numHandlers, numReaders,
	          queueSizePerHandler, conf,
	          classNameBase(protocolImpl.getClass().getName()),
	          portRangeConfig);

	      this.verbose = verbose;
	      
	      
	      Class<?>[] protocols;
	      if (protocolClass == null) { // derive protocol from impl
	        /*
	         * In order to remain compatible with the old usage where a single
	         * target protocolImpl is suppled for all protocol interfaces, and
	         * the protocolImpl is derived from the protocolClass(es) 
	         * we register all interfaces extended by the protocolImpl
	         */
	        protocols = RPC.getProtocolInterfaces(protocolImpl.getClass());

	      } else {
	        if (!protocolClass.isAssignableFrom(protocolImpl.getClass())) {
	          throw new IOException("protocolClass "+ protocolClass +
	              " is not implemented by protocolImpl which is of class " +
	              protocolImpl.getClass());
	        }
	        // register protocol class and its super interfaces
	        registerProtocolAndImpl(RPC.Type.RPC_WRITABLE, protocolClass, protocolImpl);
	        protocols = RPC.getProtocolInterfaces(protocolClass);
	      }
	      for (Class<?> p : protocols) {
	        if (!p.equals(VersionedProtocol.class)) {
	          registerProtocolAndImpl(RPC.RpcKind.RPC_WRITABLE, p, protocolImpl);
	        }
	      }

	    }

	    private static void log(String value) {
	      if (value!= null && value.length() > 55)
	        value = value.substring(0, 55)+"...";
	      LOG.info(value);
	    }
	    
	    static class WritableRpcInvoker implements RpcInvoker {

	     public Writable call(AbstractServer server,
	          String protocolName, Writable rpcRequest, long receivedTime)
	          throws IOException {
	        try {
	          Invocation call = (Invocation)rpcRequest;
	          if (server.verbose) log("Call: " + call);

	          // Verify rpc version
	          if (call.getRpcVersion() != writableRpcVersion) {
	            // Client is using a different version of WritableRpc
	            throw new IOException(
	                "WritableRpc version mismatch, client side version="
	                    + call.getRpcVersion() + ", server side version="
	                    + writableRpcVersion);
	          }

	          long clientVersion = call.getProtocolVersion();
	          final String protoName;
	          ProtoClassProtoImpl protocolImpl;
	          if (call.declaringClassProtocolName.equals(VersionedProtocol.class.getName())) {
	            // VersionProtocol methods are often used by client to figure out
	            // which version of protocol to use.
	            //
	            // Versioned protocol methods should go the protocolName protocol
	            // rather than the declaring class of the method since the
	            // the declaring class is VersionedProtocol which is not 
	            // registered directly.
	            // Send the call to the highest  protocol version
	            VerProtocolImpl highest = server.getHighestSupportedProtocol(
	                RPC.RpcKind.RPC_WRITABLE, protocolName);
	            if (highest == null) {
	              throw new IOException("Unknown protocol: " + protocolName);
	            }
	            protocolImpl = highest.protocolTarget;
	          } else {
	            protoName = call.declaringClassProtocolName;

	            // Find the right impl for the protocol based on client version.
	            ProtoNameVer pv = 
	                new ProtoNameVer(call.declaringClassProtocolName, clientVersion);
	            protocolImpl = 
	                server.getProtocolImplMap(RPC.RpcKind.RPC_WRITABLE).get(pv);
	            if (protocolImpl == null) { // no match for Protocol AND Version
	               VerProtocolImpl highest = 
	                   server.getHighestSupportedProtocol(RPC.RpcKind.RPC_WRITABLE, 
	                       protoName);
	              if (highest == null) {
	                throw new IOException("Unknown protocol: " + protoName);
	              } else { // protocol supported but not the version that client wants
	                throw new RPC.VersionMismatch(protoName, clientVersion,
	                  highest.version);
	              }
	            }
	          }
	          

	          // Invoke the protocol method

	          long startTime = System.currentTimeMillis();
	          Method method = 
	              protocolImpl.protocolClass.getMethod(call.getMethodName(),
	              call.getParameterClasses());
	          method.setAccessible(true);
	          server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
	          Object value = 
	              method.invoke(protocolImpl.protocolImpl, call.getParameters());
	          int processingTime = (int) (System.currentTimeMillis() - startTime);
	          int qTime = (int) (startTime-receivedTime);
	          if (LOG.isDebugEnabled()) {
	            LOG.debug("Served: " + call.getMethodName() +
	                      " queueTime= " + qTime +
	                      " procesingTime= " + processingTime);
	          }
	          server.rpcMetrics.addRpcQueueTime(qTime);
	          server.rpcMetrics.addRpcProcessingTime(processingTime);
	          server.rpcDetailedMetrics.addProcessingTime(call.getMethodName(),
	                                               processingTime);
	          if (server.verbose) log("Return: "+value);

	          return new ObjectWritable(method.getReturnType(), value);

	        } catch (InvocationTargetException e) {
	          Throwable target = e.getTargetException();
	          if (target instanceof IOException) {
	            throw (IOException)target;
	          } else {
	            IOException ioe = new IOException(target.toString());
	            ioe.setStackTrace(target.getStackTrace());
	            throw ioe;
	          }
	        } catch (Throwable e) {
	          if (!(e instanceof IOException)) {
	            LOG.error("Unexpected throwable object ", e);
	          }
	          IOException ioe = new IOException(e.toString());
	          ioe.setStackTrace(e.getStackTrace());
	          throw ioe;
	        }
	      }
	    }
	  }

	public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
			InetSocketAddress addr, UserGroupInformation ticket,
			Configuration conf, SocketFactory factory, int rpcTimeout,
			RetryPolicy connectionRetryPolicy) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	}