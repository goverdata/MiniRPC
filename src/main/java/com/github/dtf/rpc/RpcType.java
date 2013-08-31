package com.github.dtf.rpc;

public enum RpcType {
	RPC_BUILTIN((short) 1), // Used for built in calls by tests
	RPC_WRITABLE((short) 2), // Use WritableRpcEngine
	RPC_PROTOCOL_BUFFER((short) 3), // Use ProtobufRpcEngine
	RPC_AVRO((short) 4); // Use AVRO
	public final static short MAX_INDEX = RPC_AVRO.value; // used for array size
	public static final short FIRST_INDEX = RPC_BUILTIN.value;
	public final short value; // TODO make it private

	RpcType(short val) {
		this.value = val;
	}
}
