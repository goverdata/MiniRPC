package com.github.dtf.rpc.server;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;



public interface Server {
	/**
	 * The first four bytes of Hadoop RPC connections
	 */
	public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());

	/**
	 * Serialization type for ConnectionContext and RpcPayloadHeader
	 */
	public enum IpcSerializationType {
		// Add new serialization type to the end without affecting the enum
		// order
		PROTOBUF;

		public void write(DataOutput out) throws IOException {
			out.writeByte(this.ordinal());
		}

		static IpcSerializationType fromByte(byte b) {
			return IpcSerializationType.values()[b];
		}
	}
	
	/**
	 * If the user accidentally sends an HTTP GET to an IPC port, we detect this
	 * and send back a nicer response.
	 */
	static final ByteBuffer HTTP_GET_BYTES = ByteBuffer.wrap("GET "
			.getBytes());

	/**
	 * An HTTP response to send back if we detect an HTTP request to our IPC
	 * port.
	 */
	static final String RECEIVED_HTTP_REQ_RESPONSE = "HTTP/1.1 404 Not Found\r\n"
			+ "Content-type: text/plain\r\n\r\n"
			+ "It looks like you are making an HTTP request to a Hadoop IPC port. "
			+ "This is not the correct port for the web interface on this daemon.\r\n";

	// 1 : Introduce ping and server does not throw away RPCs
	// 3 : Introduce the protocol into the RPC connection header
	// 4 : Introduced SASL security layer
	// 5 : Introduced use of {@link ArrayPrimitiveWritable$Internal}
	// in ObjectWritable to efficiently transmit arrays of primitives
	// 6 : Made RPC payload header explicit
	// 7 : Changed Ipc Connection Header to use Protocol buffers
	public static final byte CURRENT_VERSION = 7;

	/**
	 * Initial and max size of response buffer
	 */
	static int INITIAL_RESP_BUF_SIZE = 10240;
	
	public boolean isRunning();

	public List<Connection> getConnectionList();

	public void closeConnection(Connection c);
}
