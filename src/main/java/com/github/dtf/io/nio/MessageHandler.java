package com.github.dtf.io.nio;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public interface MessageHandler {
	public void processMessage(SocketChannel channel, ByteBuffer message);
}
