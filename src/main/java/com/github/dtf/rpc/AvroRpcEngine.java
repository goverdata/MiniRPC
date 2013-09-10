package com.github.dtf.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.dtf.conf.Configuration;
import com.github.dtf.rpc.protocol.ProtocolProxy;
import com.github.dtf.rpc.protocol.ProtobufRpcEngine.Server;
import com.github.dtf.rpc.server.AbstractRpcServer;
import com.github.dtf.rpc.server.AbstractServer;
import com.github.dtf.security.UserGroupInformation;
import com.github.dtf.transport.RetryPolicy;

public class AvroRpcEngine implements RpcEngine {
	public static final Log LOG = LogFactory.getLog(AvroRpcEngine.class);

	static {
		// Register the rpcRequest deserializer for WritableRpcEngine
		AbstractRpcServer.registerProtocolEngine(RpcType.RPC_AVRO,
				RpcRequestWritable.class, new Server.ProtoBufRpcInvoker());
	}

	public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
			InetSocketAddress addr, UserGroupInformation ticket,
			Configuration conf, SocketFactory factory, int rpcTimeout,
			RetryPolicy connectionRetryPolicy) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public AbstractRpcServer getServer(Class<?> protocol, Object instance,
			String bindAddress, int port, int numHandlers, int numReaders,
			int queueSizePerHandler, boolean verbose, Configuration conf,
			String portRangeConfig) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	 public static class Server extends AbstractRpcServer {
		 
	 }

}
