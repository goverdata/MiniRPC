package com.github.dtf.rpc.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


import com.github.dtf.conf.Configuration;
import com.github.dtf.rpc.RPC;
import com.github.dtf.rpc.RpcInvoker;
import com.github.dtf.rpc.RpcType;
import com.github.dtf.rpc.Writable;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcKindProto;
import com.github.dtf.rpc.protocol.ProtobufRpcEngine;
import com.github.dtf.utils.ProtoUtil;

/** An RPC Server. */
public abstract class AbstractRpcServer extends AbstractServer {
	
	/**
	 * Register a RPC kind and the class to deserialize the rpc request.
	 * 
	 * Called by static initializers of rpcKind Engines
	 * 
	 * @param rpcKind
	 * @param rpcRequestWrapperClass
	 *            - this class is used to deserialze the the rpc request.
	 * @param rpcInvoker
	 *            - use to process the calls on SS.
	 */

	public static void registerProtocolEngine(RpcType rpcKind,
			Class<? extends Writable> rpcRequestWrapperClass,
			RpcInvoker rpcInvoker) {
		RpcTypeMapValue old = rpcKindMap.put(rpcKind, new RpcTypeMapValue(
				rpcRequestWrapperClass, rpcInvoker));
		if (old != null) {
			rpcKindMap.put(rpcKind, old);
			throw new IllegalArgumentException("ReRegistration of rpcKind: "
					+ rpcKind);
		}
		LOG.debug("rpcKind=" + rpcKind + ", rpcRequestWrapperClass="
				+ rpcRequestWrapperClass + ", rpcInvoker=" + rpcInvoker);
	}



	
	
// boolean verbose;
 static String classNameBase(String className) {
    String[] names = className.split("\\.", -1);
    if (names == null || names.length == 0) {
      return className;
    }
    return names[names.length-1];
  }
 
 /**
  * Store a map of protocol and version to its implementation
  */
 /**
  *  The key in Map
  */
 static class ProtoNameVer {
   final String protocol;
   final long   version;
   ProtoNameVer(String protocol, long ver) {
     this.protocol = protocol;
     this.version = ver;
   }
   @Override
   public boolean equals(Object o) {
     if (o == null) 
       return false;
     if (this == o) 
       return true;
     if (! (o instanceof ProtoNameVer))
       return false;
     ProtoNameVer pv = (ProtoNameVer) o;
     return ((pv.protocol.equals(this.protocol)) && 
         (pv.version == this.version));     
   }
   @Override
   public int hashCode() {
     return protocol.hashCode() * 37 + (int) version;    
   }
 }
 
 /**
  * The value in map
  */
 public static class ProtoClassProtoImpl {
   final Class<?> protocolClass;
   final Object protocolImpl; 
   ProtoClassProtoImpl(Class<?> protocolClass, Object protocolImpl) {
     this.protocolClass = protocolClass;
     this.protocolImpl = protocolImpl;
   }
 }

 ArrayList<Map<ProtoNameVer, ProtoClassProtoImpl>> protocolImplMapArray = 
     new ArrayList<Map<ProtoNameVer, ProtoClassProtoImpl>>(RpcType.MAX_INDEX);
 
 Map<ProtoNameVer, ProtoClassProtoImpl> getProtocolImplMap(RpcType rpcKind) {
   if (protocolImplMapArray.size() == 0) {// initialize for all rpc kinds
     for (int i=0; i <= RpcType.MAX_INDEX; ++i) {
       protocolImplMapArray.add(
           new HashMap<ProtoNameVer, ProtoClassProtoImpl>(10));
     }
   }
   return protocolImplMapArray.get(rpcKind.ordinal());   
 }
 
 // Register  protocol and its impl for rpc calls
 protected void registerProtocolAndImpl(RpcType rpcKind, Class<?> protocolClass, 
     Object protocolImpl) throws IOException {
   String protocolName = RPC.getProtocolName(protocolClass);
   long version;
   

   try {
     version = RPC.getProtocolVersion(protocolClass);
   } catch (Exception ex) {
     LOG.warn("Protocol "  + protocolClass + 
          " NOT registered as cannot get protocol version ");
     return;
   }


   getProtocolImplMap(rpcKind).put(new ProtoNameVer(protocolName, version),
       new ProtoClassProtoImpl(protocolClass, protocolImpl)); 
   LOG.debug("RpcKind = " + rpcKind + " Protocol Name = " + protocolName +  " version=" + version +
       " ProtocolImpl=" + protocolImpl.getClass().getName() + 
       " protocolClass=" + protocolClass.getName());
 }
 
 public static class VerProtocolImpl {
   final long version;
   final ProtoClassProtoImpl protocolTarget;
   VerProtocolImpl(long ver, ProtoClassProtoImpl protocolTarget) {
     this.version = ver;
     this.protocolTarget = protocolTarget;
   }
 }
 
 
 @SuppressWarnings("unused") // will be useful later.
 VerProtocolImpl[] getSupportedProtocolVersions(RpcType rpcKind,
     String protocolName) {
   VerProtocolImpl[] resultk = 
       new  VerProtocolImpl[getProtocolImplMap(rpcKind).size()];
   int i = 0;
   for (Map.Entry<ProtoNameVer, ProtoClassProtoImpl> pv :
                                     getProtocolImplMap(rpcKind).entrySet()) {
     if (pv.getKey().protocol.equals(protocolName)) {
       resultk[i++] = 
           new VerProtocolImpl(pv.getKey().version, pv.getValue());
     }
   }
   if (i == 0) {
     return null;
   }
   VerProtocolImpl[] result = new VerProtocolImpl[i];
   System.arraycopy(resultk, 0, result, 0, i);
   return result;
 }
 
 VerProtocolImpl getHighestSupportedProtocol(RpcType rpcKind, 
     String protocolName) {    
   Long highestVersion = 0L;
   ProtoClassProtoImpl highest = null;
   if (LOG.isDebugEnabled()) {
     LOG.debug("Size of protoMap for " + rpcKind + " ="
         + getProtocolImplMap(rpcKind).size());
   }
   for (Map.Entry<ProtoNameVer, ProtoClassProtoImpl> pv : 
         getProtocolImplMap(rpcKind).entrySet()) {
     if (pv.getKey().protocol.equals(protocolName)) {
       if ((highest == null) || (pv.getKey().version > highestVersion)) {
         highest = pv.getValue();
         highestVersion = pv.getKey().version;
       } 
     }
   }
   if (highest == null) {
     return null;
   }
   return new VerProtocolImpl(highestVersion,  highest);   
 }
 
 public AbstractRpcServer(String bindAddress, int port, 
		 Class<? extends Writable> paramClass, int handlerCount,
		 int numReaders, int queueSizePerHandler,
		 Configuration conf, String serverName, 
		 String portRangeConfig, Class rpcRequestClass) throws IOException {
	 super(bindAddress, port, paramClass, handlerCount, numReaders, queueSizePerHandler,
	          conf, serverName, portRangeConfig);
	 initProtocolMetaInfo(conf);
	 this.rpcRequestClass = rpcRequestClass;
 }

  public AbstractRpcServer(String bindAddress, int port, 
                   Class<? extends Writable> paramClass, int handlerCount,
                   int numReaders, int queueSizePerHandler,
                   Configuration conf, String serverName, 
                   String portRangeConfig) throws IOException {
    super(bindAddress, port, paramClass, handlerCount, numReaders, queueSizePerHandler,
          conf, serverName, portRangeConfig);
    initProtocolMetaInfo(conf);
  }
  
  private void initProtocolMetaInfo(Configuration conf)
      throws IOException {
    RPC.setProtocolEngine(conf, ProtocolMetaInfoPB.class,
        ProtobufRpcEngine.class);
    ProtocolMetaInfoServerSideTranslatorPB xlator = 
        new ProtocolMetaInfoServerSideTranslatorPB(this);
    BlockingService protocolInfoBlockingService = ProtocolInfoService
        .newReflectiveBlockingService(xlator);
    addProtocol(RpcType.RPC_PROTOCOL_BUFFER, ProtocolMetaInfoPB.class,
        protocolInfoBlockingService);
  }
  
  /**
   * Add a protocol to the existing server.
   * @param protocolClass - the protocol class
   * @param protocolImpl - the impl of the protocol that will be called
   * @return the server (for convenience)
   */
  public Server addProtocol(RpcType rpcKind, Class<?> protocolClass,
      Object protocolImpl) throws IOException {
    registerProtocolAndImpl(rpcKind, protocolClass, protocolImpl);
    return this;
  }
  
  public Writable call(RpcType rpcKind, String protocol,
      Writable rpcRequest, long receiveTime) throws Exception {
    return getRpcInvoker(rpcKind).call(this, protocol, rpcRequest,
        receiveTime);
  }
}
