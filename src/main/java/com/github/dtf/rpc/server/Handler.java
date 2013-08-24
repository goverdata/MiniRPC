package com.github.dtf.rpc.server;

import java.io.ByteArrayOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.common.string.StringUtils;
import com.github.dtf.rpc.Writable;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcStatusProto;

/** Handles queued calls . */
public class Handler extends Thread {
	public static final Log LOG = LogFactory.getLog(Handler.class);
	AbstractServer server;

	public Handler(AbstractServer server, int instanceNumber) {
		this.server = server;
		this.setDaemon(true);
		this.setName("IPC Server handler " + instanceNumber + " on "
				+ server.getPort());
	}

	@Override
	public void run() {
		LOG.debug(getName() + ": starting");
		// SERVER.set(AbstractServer.this);
		ByteArrayOutputStream buf = new ByteArrayOutputStream(
				AbstractServer.INITIAL_RESP_BUF_SIZE);
		while (server.isRunning()) {
			try {
				final Call call = server.getCallQueue().take(); // pop the
																// queue; maybe
				// blocked here
				if (LOG.isDebugEnabled()) {
					LOG.debug(getName() + ": has Call#" + call.getCallId()
							+ "for RpcKind " + call.getRpcKind() + " from "
							+ call.getConnection());
				}
				String errorClass = null;
				String error = null;
				Writable value = null;

				server.getCurCall().set(call);
				try {
					// Make the call as the user via Subject.doAs, thus
					// associating
					// the call with the Subject
					// if (call.connection.user == null) {
					value = server.call(call.getRpcKind(),
							call.getConnection().protocolName, call.rpcRequest,
							call.getTimestamp());
					// } else {
					// value =
					// call.connection.user.doAs
					// (new PrivilegedExceptionAction<Writable>() {
					// @Override
					// public Writable run() throws Exception {
					// // make the call
					// return call(call.rpcKind, call.connection.protocolName,
					// call.rpcRequest, call.timestamp);
					//
					// }
					// }
					// );
					// }
				} catch (Throwable e) {
					String logMsg = getName() + ", call " + call + ": error: "
							+ e;
					if (e instanceof RuntimeException || e instanceof Error) {
						// These exception types indicate something is probably
						// wrong
						// on the server side, as opposed to just a normal
						// exceptional
						// result.
						LOG.warn(logMsg, e);
					} else {
						LOG.info(logMsg, e);
					}

					errorClass = e.getClass().getName();
					error = StringUtils.stringifyException(e);
					// Remove redundant error class name from the beginning of
					// the stack trace
					String exceptionHdr = errorClass + ": ";
					if (error.startsWith(exceptionHdr)) {
						error = error.substring(exceptionHdr.length());
					}
				}
				server.getCurCall().set(null);
				synchronized (call.getConnection().responseQueue) {
					// setupResponse() needs to be sync'ed together with
					// responder.doResponse() since setupResponse may use
					// SASL to encrypt response data and SASL enforces
					// its own message ordering.
					server.setupResponse(buf, call,
							(error == null) ? RpcStatusProto.SUCCESS
									: RpcStatusProto.ERROR, value, errorClass,
							error);

					// Discard the large buf and reset it back to smaller size
					// to free up heap
					if (buf.size() > server.getMaxRespSize()) {
						LOG.warn("Large response size " + buf.size()
								+ " for call " + call.toString());
						buf = new ByteArrayOutputStream(
								AbstractServer.INITIAL_RESP_BUF_SIZE);
					}
					server.getResponder().doRespond(call);
				}
			} catch (InterruptedException e) {
				if (server.isRunning()) { // unexpected -- log it
					LOG.info(getName() + " unexpectedly interrupted", e);
				}
			} catch (Exception e) {
				LOG.info(getName() + " caught an exception", e);
			}
		}
		LOG.debug(getName() + ": exiting");
	}

}