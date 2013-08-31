package com.github.dtf.rpc.server;

import java.nio.ByteBuffer;

import com.github.dtf.rpc.RPC;
import com.github.dtf.rpc.RpcType;
import com.github.dtf.rpc.Writable;

/** A call queued for handling. */
public class Call {
	private final int callId; // the client's call id
	final Writable rpcRequest; // Serialized Rpc request from client
	private final Connection connection; // connection to client

	private long timestamp; // time received when response is null
							 // time served when response is not null
	private ByteBuffer rpcResponse; // the response for this call
	private final RpcType rpcKind;

	public Call(int id, Writable param, Connection connection) {
		this(id, param, connection, RpcType.RPC_BUILTIN);
	}

	public Call(int id, Writable param, Connection connection, RpcType kind) {
		this.callId = id;
		this.rpcRequest = param;
		this.connection = connection;
		this.setTimestamp(System.currentTimeMillis());
		this.setRpcResponse(null);
		this.rpcKind = kind;
	}

	public void setResponse(ByteBuffer response) {
		this.setRpcResponse(response);
	}

	public int getCallId() {
		return callId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public ByteBuffer getRpcResponse() {
		return rpcResponse;
	}

	public void setRpcResponse(ByteBuffer rpcResponse) {
		this.rpcResponse = rpcResponse;
	}

	public RpcType getRpcKind() {
		return rpcKind;
	}
	public Connection getConnection() {
		return connection;
	}

	@Override
	public String toString() {
		return rpcRequest.toString() + " from " + connection.toString();
	}
}