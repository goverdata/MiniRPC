package com.github.dtf.rpc.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.AccessControlException;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.common.utils.WritableUtils;
import com.github.dtf.conf.CommonConfigurationKeys;
import com.github.dtf.exception.IpcException;
import com.github.dtf.rpc.RPC;
import com.github.dtf.rpc.RPC.VersionMismatch;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcPayloadHeaderProto;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcPayloadOperationProto;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcStatusProto;
import com.github.dtf.rpc.RpcType;
import com.github.dtf.rpc.Writable;
import com.github.dtf.rpc.client.Client;
import com.github.dtf.rpc.server.Server.IpcSerializationType;
import com.github.dtf.utils.ProtoUtil;
import com.github.dtf.utils.ReflectionUtils;

public class Connection {
	public static final Log LOG = LogFactory.getLog(Connection.class);
	private boolean connectionHeaderRead = false; // connection header is read?
	private boolean connectionContextRead = false; // if connection context that
	// follows connection header is read
	/**
	 * If the user accidentally sends an HTTP GET to an IPC port, we detect this
	 * and send back a nicer response.
	 */
	private static final ByteBuffer HTTP_GET_BYTES = ByteBuffer.wrap("GET ".getBytes());

	private SocketChannel channel;
	private ByteBuffer data;
	private ByteBuffer dataLengthBuffer;
	LinkedList<Call> responseQueue;
	private volatile int rpcCount = 0; // number of outstanding rpcs
	private long lastContact;
	private int dataLength;
	private Socket socket;
	// Cache the remote host & port info so that even if the socket is
	// disconnected, we can say where it used to connect to.
	private String hostAddress;
	private int remotePort;
	private InetAddress addr;
	private int socketSendBufferSize;
	private int maxIdleTime; // the maximum idle time after
								// which a client may be disconnected
								// IpcConnectionContextProto connectionContext;
	String protocolName;
	// boolean useSasl;
	// SaslServer saslServer;
	// private AuthMethod authMethod;
	// private boolean saslContextEstablished;
	private boolean skipInitialSaslHandshake;
	private ByteBuffer connectionHeaderBuf = null;
	private ByteBuffer unwrappedData;
	private ByteBuffer unwrappedDataLengthBuffer;

	// UserGroupInformation user = null;
	// public UserGroupInformation attemptingUser = null; // user name before
	// auth

	// Fake 'call' for failed authorization response
	private static final int AUTHORIZATION_FAILED_CALLID = -1;
	private final Call authFailedCall = new Call(AUTHORIZATION_FAILED_CALLID,
			null, this);
	private ByteArrayOutputStream authFailedResponse = new ByteArrayOutputStream();
	// Fake 'call' for SASL context setup
	private static final int SASL_CALLID = -33;

	private final Call saslCall = new Call(SASL_CALLID, null, this);
	private final ByteArrayOutputStream saslResponse = new ByteArrayOutputStream();

	private boolean useWrap = false;
	AbstractServer server;

	public Connection(AbstractServer server, SelectionKey key,
			SocketChannel channel, long lastContact) {
		this.server = server;
		this.setChannel(channel);
		this.lastContact = lastContact;
		this.data = null;
		this.dataLengthBuffer = ByteBuffer.allocate(4);
		this.unwrappedData = null;
		this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
		this.socket = channel.socket();
		this.addr = socket.getInetAddress();
		if (addr == null) {
			this.hostAddress = "*Unknown*";
		} else {
			this.hostAddress = addr.getHostAddress();
		}
		this.remotePort = socket.getPort();
		this.responseQueue = new LinkedList<Call>();
		if (socketSendBufferSize != 0) {
			try {
				socket.setSendBufferSize(socketSendBufferSize);
			} catch (IOException e) {
				LOG.warn("Connection: unable to set socket send buffer size to "
						+ socketSendBufferSize);
			}
		}
	}

	@Override
	public String toString() {
		return getHostAddress() + ":" + remotePort;
	}

	public String getHostAddress() {
		return hostAddress;
	}

	public InetAddress getHostInetAddress() {
		return addr;
	}

	public void setLastContact(long lastContact) {
		this.lastContact = lastContact;
	}

	public long getLastContact() {
		return lastContact;
	}

	/* Return true if the connection has no outstanding rpc */
	private boolean isIdle() {
		return rpcCount == 0;
	}

	/* Decrement the outstanding RPC count */
	public void decRpcCount() {
		rpcCount--;
	}

	/* Increment the outstanding RPC count */
	private void incRpcCount() {
		rpcCount++;
	}

	public boolean timedOut(long currentTime) {
		if (isIdle() && currentTime - lastContact > maxIdleTime)
			return true;
		return false;
	}

	// private UserGroupInformation getAuthorizedUgi(String authorizedId)
	// throws IOException {
	// if (authMethod == SaslRpcServer.AuthMethod.DIGEST) {
	// TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authorizedId,
	// secretManager);
	// UserGroupInformation ugi = tokenId.getUser();
	// if (ugi == null) {
	// throw new AccessControlException(
	// "Can't retrieve username from tokenIdentifier.");
	// }
	// ugi.addTokenIdentifier(tokenId);
	// return ugi;
	// } else {
	// return UserGroupInformation.createRemoteUser(authorizedId);
	// }
	// }

	// private void saslReadAndProcess(byte[] saslToken) throws IOException,
	// InterruptedException {
	// if (!saslContextEstablished) {
	// byte[] replyToken = null;
	// try {
	// if (saslServer == null) {
	// switch (authMethod) {
	// case DIGEST:
	// if (secretManager == null) {
	// throw new AccessControlException(
	// "Server is not configured to do DIGEST authentication.");
	// }
	// secretManager.checkAvailableForRead();
	// saslServer = Sasl.createSaslServer(AuthMethod.DIGEST
	// .getMechanismName(), null, SaslRpcServer.SASL_DEFAULT_REALM,
	// SaslRpcServer.SASL_PROPS, new SaslDigestCallbackHandler(
	// secretManager, this));
	// break;
	// default:
	// UserGroupInformation current = UserGroupInformation
	// .getCurrentUser();
	// String fullName = current.getUserName();
	// if (LOG.isDebugEnabled())
	// LOG.debug("Kerberos principal name is " + fullName);
	// final String names[] = SaslRpcServer.splitKerberosName(fullName);
	// if (names.length != 3) {
	// throw new AccessControlException(
	// "Kerberos principal name does NOT have the expected "
	// + "hostname part: " + fullName);
	// }
	// current.doAs(new PrivilegedExceptionAction<Object>() {
	// @Override
	// public Object run() throws SaslException {
	// saslServer = Sasl.createSaslServer(AuthMethod.KERBEROS
	// .getMechanismName(), names[0], names[1],
	// SaslRpcServer.SASL_PROPS, new SaslGssCallbackHandler());
	// return null;
	// }
	// });
	// }
	// if (saslServer == null)
	// throw new AccessControlException(
	// "Unable to find SASL server implementation for "
	// + authMethod.getMechanismName());
	// if (LOG.isDebugEnabled())
	// LOG.debug("Created SASL server with mechanism = "
	// + authMethod.getMechanismName());
	// }
	// if (LOG.isDebugEnabled())
	// LOG.debug("Have read input token of size " + saslToken.length
	// + " for processing by saslServer.evaluateResponse()");
	// replyToken = saslServer.evaluateResponse(saslToken);
	// } catch (IOException e) {
	// IOException sendToClient = e;
	// Throwable cause = e;
	// while (cause != null) {
	// if (cause instanceof InvalidToken) {
	// sendToClient = (InvalidToken) cause;
	// break;
	// }
	// cause = cause.getCause();
	// }
	// doSaslReply(SaslStatus.ERROR, null, sendToClient.getClass().getName(),
	// sendToClient.getLocalizedMessage());
	// rpcMetrics.incrAuthenticationFailures();
	// String clientIP = this.toString();
	// // attempting user could be null
	// AUDITLOG.warn(AUTH_FAILED_FOR + clientIP + ":" + attemptingUser);
	// throw e;
	// }
	// if (replyToken != null) {
	// if (LOG.isDebugEnabled())
	// LOG.debug("Will send token of size " + replyToken.length
	// + " from saslServer.");
	// doSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null,
	// null);
	// }
	// if (saslServer.isComplete()) {
	// if (LOG.isDebugEnabled()) {
	// LOG.debug("SASL server context established. Negotiated QoP is "
	// + saslServer.getNegotiatedProperty(Sasl.QOP));
	// }
	// String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
	// useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
	// user = getAuthorizedUgi(saslServer.getAuthorizationID());
	// if (LOG.isDebugEnabled()) {
	// LOG.debug("SASL server successfully authenticated client: " + user);
	// }
	// rpcMetrics.incrAuthenticationSuccesses();
	// AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
	// saslContextEstablished = true;
	// }
	// } else {
	// if (LOG.isDebugEnabled())
	// LOG.debug("Have read input token of size " + saslToken.length
	// + " for processing by saslServer.unwrap()");
	//
	// if (!useWrap) {
	// processOneRpc(saslToken);
	// } else {
	// byte[] plaintextData = saslServer.unwrap(saslToken, 0,
	// saslToken.length);
	// processUnwrappedData(plaintextData);
	// }
	// }
	// }

	// private void doSaslReply(SaslStatus status, Writable rv,
	// String errorClass, String error) throws IOException {
	// saslResponse.reset();
	// DataOutputStream out = new DataOutputStream(saslResponse);
	// out.writeInt(status.state); // write status
	// if (status == SaslStatus.SUCCESS) {
	// rv.write(out);
	// } else {
	// WritableUtils.writeString(out, errorClass);
	// WritableUtils.writeString(out, error);
	// }
	// saslCall.setResponse(ByteBuffer.wrap(saslResponse.toByteArray()));
	// responder.doRespond(saslCall);
	// }

	// private void disposeSasl() {
	// if (saslServer != null) {
	// try {
	// saslServer.dispose();
	// } catch (SaslException ignored) {
	// }
	// }
	// }

	public int readAndProcess() throws IOException, InterruptedException {
		while (true) {
			// Read at most one RPC. If the header is not read completely yet
			// * then iterate until we read first RPC or until there is no data
			// left.

			int count = -1;
			if (dataLengthBuffer.remaining() > 0) {
				count = server.channelRead(getChannel(), dataLengthBuffer);
				if (count < 0 || dataLengthBuffer.remaining() > 0)
					return count;
			}

			if (!connectionHeaderRead) {
				// Every connection is expected to send the header.
				if (connectionHeaderBuf == null) {
					connectionHeaderBuf = ByteBuffer.allocate(3);
				}
				count = server.channelRead(getChannel(), connectionHeaderBuf);
				if (count < 0 || connectionHeaderBuf.remaining() > 0) {
					return count;
				}
				int version = connectionHeaderBuf.get(0);
				byte[] method = new byte[] { connectionHeaderBuf.get(1) };
				// authMethod = AuthMethod.read(new DataInputStream(
				// new ByteArrayInputStream(method)));
				dataLengthBuffer.flip();

				// Check if it looks like the user is hitting an IPC port
				// with an HTTP GET - this is a common error, so we can
				// send back a simple string indicating as much.
				if (HTTP_GET_BYTES.equals(dataLengthBuffer)) {
					setupHttpRequestOnIpcPortResponse();
					return -1;
				}

				if (!AbstractServer.HEADER.equals(dataLengthBuffer)
						|| version != AbstractServer.CURRENT_VERSION) {
					// Warning is ok since this is not supposed to happen.
					LOG.warn("Incorrect header or version mismatch from "
							+ hostAddress + ":" + remotePort + " got version "
							+ version + " expected version "
							+ AbstractServer.CURRENT_VERSION);
					setupBadVersionResponse(version);
					return -1;
				}

				IpcSerializationType serializationType = IpcSerializationType
						.fromByte(connectionHeaderBuf.get(2));
				if (serializationType != IpcSerializationType.PROTOBUF) {
					respondUnsupportedSerialization(serializationType);
					return -1;
				}

				dataLengthBuffer.clear();
				// if (authMethod == null) {
				// throw new
				// IOException("Unable to read authentication method");
				// }
				// if (isSecurityEnabled && authMethod == AuthMethod.SIMPLE) {
				// AccessControlException ae = new
				// AccessControlException("Authorization ("
				// + CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				// + ") is enabled but authentication ("
				// + CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION
				// +
				// ") is configured as simple. Please configure another method "
				// + "like kerberos or digest.");
				// setupResponse(authFailedResponse, authFailedCall,
				// RpcStatusProto.FATAL,
				// null, ae.getClass().getName(), ae.getMessage());
				// responder.doRespond(authFailedCall);
				// throw ae;
				// }
				// if (!isSecurityEnabled && authMethod != AuthMethod.SIMPLE) {
				// doSaslReply(SaslStatus.SUCCESS, new IntWritable(
				// SaslRpcServer.SWITCH_TO_SIMPLE_AUTH), null, null);
				// authMethod = AuthMethod.SIMPLE;
				// // client has already sent the initial Sasl message and we
				// // should ignore it. Both client and server should fall back
				// // to simple auth from now on.
				// skipInitialSaslHandshake = true;
				// }
				// if (authMethod != AuthMethod.SIMPLE) {
				// useSasl = true;
				// }

				connectionHeaderBuf = null;
				connectionHeaderRead = true;
				continue;
			}

			if (data == null) {
				dataLengthBuffer.flip();
				dataLength = dataLengthBuffer.getInt();
				if ((dataLength == Client.PING_CALL_ID) && (!useWrap)) {
					// covers the !useSasl too
					dataLengthBuffer.clear();
					return 0; // ping message
				}

				if (dataLength < 0) {
					LOG.warn("Unexpected data length " + dataLength
							+ "!! from " + getHostAddress());
				}
				data = ByteBuffer.allocate(dataLength);
			}

			count = server.channelRead(getChannel(), data);

			if (data.remaining() == 0) {
				dataLengthBuffer.clear();
				data.flip();
				if (skipInitialSaslHandshake) {
					data = null;
					skipInitialSaslHandshake = false;
					continue;
				}
				boolean isHeaderRead = connectionContextRead;
				// if (useSasl) {
				// saslReadAndProcess(data.array());
				// } else {
				processOneRpc(data.array());
				// }
				data = null;
				if (!isHeaderRead) {
					continue;
				}
			}
			return count;
		}
	}

	/**
	 * Try to set up the response to indicate that the client version is
	 * incompatible with the server. This can contain special-case code to speak
	 * enough of past IPC protocols to pass back an exception to the caller.
	 * 
	 * @param clientVersion
	 *            the version the caller is using
	 * @throws IOException
	 */
	private void setupBadVersionResponse(int clientVersion) throws IOException {
		String errMsg = "Server IPC version " + AbstractServer.CURRENT_VERSION
				+ " cannot communicate with client version " + clientVersion;
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		if (clientVersion >= 3) {
			Call fakeCall = new Call(-1, null, this);
			// Versions 3 and greater can interpret this exception
			// response in the same manner
			server.setupResponseOldVersionFatal(buffer, fakeCall, null,
					VersionMismatch.class.getName(), errMsg);

			server.getResponder().doRespond(fakeCall);
		} else if (clientVersion == 2) { // Hadoop 0.18.3
			Call fakeCall = new Call(0, null, this);
			DataOutputStream out = new DataOutputStream(buffer);
			out.writeInt(0); // call ID
			out.writeBoolean(true); // error
			WritableUtils.writeString(out, VersionMismatch.class.getName());
			WritableUtils.writeString(out, errMsg);
			fakeCall.setResponse(ByteBuffer.wrap(buffer.toByteArray()));

			server.getResponder().doRespond(fakeCall);
		}
	}

	private void respondUnsupportedSerialization(IpcSerializationType st)
			throws IOException {
		String errMsg = "Server IPC version " + AbstractServer.CURRENT_VERSION
				+ " do not support serilization " + st.toString();
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		Call fakeCall = new Call(-1, null, this);
		server.setupResponse(buffer, fakeCall, RpcStatusProto.FATAL, null,
				IpcException.class.getName(), errMsg);
		server.getResponder().doRespond(fakeCall);
	}

	private void setupHttpRequestOnIpcPortResponse() throws IOException {
		Call fakeCall = new Call(0, null, this);
		fakeCall.setResponse(ByteBuffer
				.wrap(AbstractServer.RECEIVED_HTTP_REQ_RESPONSE.getBytes()));
		server.getResponder().doRespond(fakeCall);
	}

	/** Reads the connection context following the connection header */
	private void processConnectionContext(byte[] buf) throws IOException {
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf));
		connectionContext = IpcConnectionContextProto.parseFrom(in);
		protocolName = connectionContext.hasProtocol() ? connectionContext
				.getProtocol() : null;

		// UserGroupInformation protocolUser =
		// ProtoUtil.getUgi(connectionContext);
		// if (!useSasl) {
		// user = protocolUser;
		// if (user != null) {
		// user.setAuthenticationMethod(AuthMethod.SIMPLE.authenticationMethod);
		// }
		// } else {
		// // user is authenticated
		// user.setAuthenticationMethod(authMethod.authenticationMethod);
		// //Now we check if this is a proxy user case. If the protocol user is
		// //different from the 'user', it is a proxy user scenario. However,
		// //this is not allowed if user authenticated with DIGEST.
		// if ((protocolUser != null)
		// && (!protocolUser.getUserName().equals(user.getUserName()))) {
		// if (authMethod == AuthMethod.DIGEST) {
		// // Not allowed to doAs if token authentication is used
		// throw new AccessControlException("Authenticated user (" + user
		// + ") doesn't match what the client claims to be ("
		// + protocolUser + ")");
		// } else {
		// // Effective user can be different from authenticated user
		// // for simple auth or kerberos auth
		// // The user is the real user. Now we create a proxy user
		// UserGroupInformation realUser = user;
		// user = UserGroupInformation.createProxyUser(protocolUser
		// .getUserName(), realUser);
		// // Now the user is a proxy user, set Authentication method Proxy.
		// user.setAuthenticationMethod(AuthenticationMethod.PROXY);
		// }
		// }
		// }
	}

	private void processUnwrappedData(byte[] inBuf) throws IOException,
			InterruptedException {
		ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(
				inBuf));
		// Read all RPCs contained in the inBuf, even partial ones
		while (true) {
			int count = -1;
			if (unwrappedDataLengthBuffer.remaining() > 0) {
				count = server.channelRead(ch, unwrappedDataLengthBuffer);
				if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
					return;
			}

			if (unwrappedData == null) {
				unwrappedDataLengthBuffer.flip();
				int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

				if (unwrappedDataLength == Client.PING_CALL_ID) {
					if (LOG.isDebugEnabled())
						LOG.debug("Received ping message");
					unwrappedDataLengthBuffer.clear();
					continue; // ping message
				}
				unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
			}

			count = server.channelRead(ch, unwrappedData);
			if (count <= 0 || unwrappedData.remaining() > 0)
				return;

			if (unwrappedData.remaining() == 0) {
				unwrappedDataLengthBuffer.clear();
				unwrappedData.flip();
				processOneRpc(unwrappedData.array());
				unwrappedData = null;
			}
		}
	}

	private void processOneRpc(byte[] buf) throws IOException,
			InterruptedException {
		if (connectionContextRead) {
			processData(buf);
		} else {
			processConnectionContext(buf);
			connectionContextRead = true;
			// if (!authorizeConnection()) {
			// throw new AccessControlException("Connection from " + this
			// + " for protocol " + connectionContext.getProtocol()
			// + " is unauthorized for user " + user);
			// }
		}
	}

	private void processData(byte[] buf) throws IOException,
			InterruptedException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf));
		RpcPayloadHeaderProto header = RpcPayloadHeaderProto
				.parseDelimitedFrom(dis);

		if (LOG.isDebugEnabled())
			LOG.debug(" got #" + header.getCallId());
		if (!header.hasRpcOp()) {
			throw new IOException(" IPC Server: No rpc op in rpcPayloadHeader");
		}
		if (header.getRpcOp() != RpcPayloadOperationProto.RPC_FINAL_PAYLOAD) {
			throw new IOException("IPC Server does not implement operation"
					+ header.getRpcOp());
		}
		// If we know the rpc kind, get its class so that we can deserialize
		// (Note it would make more sense to have the handler deserialize but
		// we continue with this original design.
		if (!header.hasRpcKind()) {
			throw new IOException(
					" IPC Server: No rpc kind in rpcPayloadHeader");
		}
		Class<? extends Writable> rpcRequestClass = server.getRpcRequestWrapper(header
				.getRpcKind());
		if (rpcRequestClass == null) {
			LOG.warn("Unknown rpc kind " + header.getRpcKind()
					+ " from client " + getHostAddress());
			final Call readParamsFailedCall = new Call(header.getCallId(),
					null, this);
			ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();

			server.setupResponse(responseBuffer, readParamsFailedCall,
					RpcStatusProto.FATAL, null, IOException.class.getName(),
					"Unknown rpc kind " + header.getRpcKind());
			server.getResponder().doRespond(readParamsFailedCall);
			return;
		}
		Writable rpcRequest;
		try { // Read the rpc request
			rpcRequest = ReflectionUtils.newInstance(rpcRequestClass,
					server.getConf());
			rpcRequest.readFields(dis);
		} catch (Throwable t) {
			// LOG.warn("Unable to read call parameters for client " +
			// getHostAddress() + "on connection protocol " +
			// this.protocolName + " for rpcKind " + header.getRpcKind(), t);
			// final Call readParamsFailedCall =
			// new Call(header.getCallId(), null, this);
			// ByteArrayOutputStream responseBuffer = new
			// ByteArrayOutputStream();
			//
			// setupResponse(responseBuffer, readParamsFailedCall,
			// RpcStatusProto.FATAL, null,
			// t.getClass().getName(),
			// "IPC server unable to read call parameters: " + t.getMessage());
			// responder.doRespond(readParamsFailedCall);
			return;
		}

//		Call call = new Call(header.getCallId(), rpcRequest, this,
//				ProtoUtil.convert(header.getRpcKind()));
		//FIXME Just for test
		Call call = new Call(header.getCallId(), rpcRequest, this,RpcType.RPC_WRITABLE);
		server.getCallQueue().put(call); // queue the call; maybe blocked here
		incRpcCount(); // Increment the rpc count
	}

	// private boolean authorizeConnection() throws IOException {
	// try {
	// // If auth method is DIGEST, the token was obtained by the
	// // real user for the effective user, therefore not required to
	// // authorize real user. doAs is allowed only for simple or kerberos
	// // authentication
	// if (user != null && user.getRealUser() != null
	// && (authMethod != AuthMethod.DIGEST)) {
	// ProxyUsers.authorize(user, this.getHostAddress(), conf);
	// }
	// authorize(user, protocolName, getHostInetAddress());
	// if (LOG.isDebugEnabled()) {
	// LOG.debug("Successfully authorized " + connectionContext);
	// }
	// rpcMetrics.incrAuthorizationSuccesses();
	// } catch (AuthorizationException ae) {
	// rpcMetrics.incrAuthorizationFailures();
	// setupResponse(authFailedResponse, authFailedCall, RpcStatusProto.FATAL,
	// null,
	// ae.getClass().getName(), ae.getMessage());
	// responder.doRespond(authFailedCall);
	// return false;
	// }
	// return true;
	// }

	public synchronized void close() throws IOException {
		// disposeSasl();
		data = null;
		dataLengthBuffer = null;
		if (!getChannel().isOpen())
			return;
		try {
			socket.shutdownOutput();
		} catch (Exception e) {
			LOG.debug("Ignoring socket shutdown exception", e);
		}
		if (getChannel().isOpen()) {
			try {
				getChannel().close();
			} catch (Exception e) {
			}
		}
		try {
			socket.close();
		} catch (Exception e) {
		}
	}

	public SocketChannel getChannel() {
		return channel;
	}

	public void setChannel(SocketChannel channel) {
		this.channel = channel;
	}
}