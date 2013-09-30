package com.github.dtf.rpc.server;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dtf.io.nio.MessageHandler;
import com.github.dtf.io.nio.NioTcpServer;
import com.github.dtf.rpc.Call;
import com.github.dtf.utils.ByteUtils;

public class Reader implements MessageHandler {
	boolean ISDEBUG = Log.isDebugEnabled();
	static final Logger LOG = LoggerFactory.getLogger(NioTcpServer.class);
	Server server;

	public Reader(Server svc) {
		server = svc;
	}

	public void processMessage(SocketChannel channel, ByteBuffer message) {
		if(ISDEBUG){
			LOG.debug("Server get request:" + System.currentTimeMillis());
			ByteUtils.printBytes(message.array());
		}
		int length = message.getInt();
		byte[] data = new byte[length];
		message.get(data, 0, length);
		Call newCall = new Call();
		newCall.setClientChannel(channel);
		newCall.setRequestBuffer(data);
		server.getRequestQueue().add(newCall);
	}

}
