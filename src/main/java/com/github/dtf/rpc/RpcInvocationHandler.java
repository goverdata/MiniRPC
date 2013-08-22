package com.github.dtf.rpc;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;

import com.github.dtf.rpc.client.ConnectionId;

/**
 * This interface must be implemented by all InvocationHandler implementations.
 */
public interface RpcInvocationHandler extends InvocationHandler, Closeable {

	/**
	 * Returns the connection id associated with the InvocationHandler instance.
	 * 
	 * @return ConnectionId
	 */
	ConnectionId getConnectionId();
}