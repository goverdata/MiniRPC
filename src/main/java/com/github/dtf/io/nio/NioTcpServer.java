package com.github.dtf.io.nio;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dtf.rpc.server.Server;


public class NioTcpServer implements SelectorListener {
	static final Logger LOG = LoggerFactory.getLogger(NioTcpServer.class);
	
	
	Server server;
	Selector selector;
	// the key used for selecting accept event
    private SelectionKey acceptKey = null;

    // the server socket for accepting clients
    private ServerSocketChannel serverChannel = null;
    //SelectorLoop selectorLoop;
    
    private final SelectorLoop acceptSelectorLoop;

    private final SelectorLoopPool readWriteSelectorPool;
    private SocketAddress address;
    
	public NioTcpServer(Server server, SocketAddress add){
		this.server = server;
		acceptSelectorLoop = new NioSelectorLoop("Server-accept");
		address = add;
		readWriteSelectorPool = new FixedSelectorLoopPool("Server-rw", 2);
	}
	
	public void bind(SocketAddress address) {
		try {
			serverChannel = ServerSocketChannel.open();
//			serverChannel.socket().setReuseAddress(isReuseAddress());
			// FIXME What reuse address
			serverChannel.socket().setReuseAddress(true);
            serverChannel.socket().bind(address);
            serverChannel.configureBlocking(false);
			acceptSelectorLoop.register(true, false, false, false, this, serverChannel, null);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void ready(boolean accept, boolean connect, boolean read,
			ByteBuffer readBuffer, boolean write) {
		if (accept) {
            LOG.debug("acceptable new client");

            // accepted connection
            try {
                LOG.debug("new client accepted");
                createSession(serverChannel.accept());

            } catch (final IOException e) {
                LOG.error("error while accepting new client", e);
            }
        }

        if (read || write) {
            throw new IllegalStateException("should not receive read or write events");
        }
	}

	private void createSession(SocketChannel clientSocket) throws IOException {
		SocketChannel socketChannel = clientSocket;
		SelectorLoop readWriteSelectorLoop = readWriteSelectorPool.getSelectorLoop();
		socketChannel.configureBlocking(false);
		final NioTcpSession session = new NioTcpSession(this, socketChannel, readWriteSelectorLoop);

		readWriteSelectorLoop.register(false, false, true, false, session, socketChannel, cb);
	}
	
	RegistrationCallback cb = null;
	public void registCallback(RegistrationCallback callBack){
		cb = callBack;
	}
	
	public void start() {
		bind(address);
	}
}
