package com.github.dtf.rpc.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.common.http.NetUtils;

/** Listens on the socket. Creates jobs for the handler threads */
public class Listener extends Thread {
    public static final Log LOG = LogFactory.getLog(Listener.class);
	// FIXME Modify it for test 10-->2
	private int readThreads = 2; // number of read threads
	private ServerSocketChannel acceptChannel = null; // the accept channel
	private Selector selector = null; // the selector that we use for the server
	private Reader[] readers = null;
	private int currentReader = 0;
	private InetSocketAddress address; // the address we bind at
	private Random rand = new Random();
	private long lastCleanupRunTime = 0; // the last time when a cleanup connec-
											// -tion (for idle connections) ran
	private long cleanupInterval = 10000; // the minimum interval between
											// two cleanup runs

	private int thresholdIdleConnections; // the number of idle connections
	// after which we will start
	// cleaning up idle
	// connections
	int maxConnectionsToNuke; // the max number of
	// connections to nuke
	// during a cleanup

	private List<Connection> connectionList = Collections
			.synchronizedList(new LinkedList<Connection>());
	private int numConnections = 0;

	// private int backlogLength = conf.getInt(
	// CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
	// CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
	private int backlogLength = 100;
	AbstractServer server;
	boolean tcpNoDelay;

	public Listener(AbstractServer server, InetAddress addr, int port, boolean tcpNoDelay) throws IOException {
		this.server = server;
		this.tcpNoDelay = tcpNoDelay;
		address = new InetSocketAddress(addr, port);
		// Create a new server socket and set to non blocking mode
		acceptChannel = ServerSocketChannel.open();
		acceptChannel.configureBlocking(false);

		// Bind the server socket to the local host and port
		NetUtils.bind(acceptChannel.socket(), address, backlogLength);
		port = acceptChannel.socket().getLocalPort(); // Could be an ephemeral
														// port
		// create a selector;
		selector = Selector.open();
		readers = new Reader[readThreads];
		for (int i = 0; i < readThreads; i++) {
			Reader reader = new Reader("Socket Reader #" + (i + 1)
					+ " for port " + port);
			readers[i] = reader;
			reader.start();
		}

		// Register accepts on the server socket with the selector.
		acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
		this.setName("IPC Server listener on " + port);
		this.setDaemon(true);
	}

	private class Reader extends Thread {
		private volatile boolean adding = false;
		private final Selector readSelector;

		Reader(String name) throws IOException {
			super(name);

			this.readSelector = Selector.open();
		}

		public void run() {
			LOG.info("Starting " + getName());
			try {
				doRunLoop();
			} finally {
				try {
					readSelector.close();
				} catch (IOException ioe) {
					LOG.error(
							"Error closing read selector in " + this.getName(),
							ioe);
				}
			}
		}

		private synchronized void doRunLoop() {
			while (server.isRunning()) {
				SelectionKey key = null;
				try {
					readSelector.select();
					while (adding) {
						this.wait(1000);
					}

					Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
					while (iter.hasNext()) {
						key = iter.next();
						iter.remove();
						if (key.isValid()) {
							if (key.isReadable()) {
								doRead(key);
							}
						}
						key = null;
					}
				} catch (InterruptedException e) {
					if (server.isRunning()) { // unexpected -- log it
						LOG.info(getName() + " unexpectedly interrupted", e);
					}
				} catch (IOException ex) {
					LOG.error("Error in Reader", ex);
				}
			}
		}

		/**
		 * This gets reader into the state that waits for the new channel to be
		 * registered with readSelector. If it was waiting in select() the
		 * thread will be woken up, otherwise whenever select() is called it
		 * will return even if there is nothing to read and wait in
		 * while(adding) for finishAdd call
		 */
		public void startAdd() {
			adding = true;
			readSelector.wakeup();
		}

		public synchronized SelectionKey registerChannel(SocketChannel channel)
				throws IOException {
			return channel.register(readSelector, SelectionKey.OP_READ);
		}

		public synchronized void finishAdd() {
			adding = false;
			this.notify();
		}

		void shutdown() {
			assert !server.isRunning();
			readSelector.wakeup();
			try {
				join();
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * cleanup connections from connectionList. Choose a random range to scan
	 * and also have a limit on the number of the connections that will be
	 * cleanedup per run. The criteria for cleanup is the time for which the
	 * connection was idle. If 'force' is true then all connections will be
	 * looked at for the cleanup.
	 */
	private void cleanupConnections(boolean force) {
		if (force || numConnections > thresholdIdleConnections) {
			long currentTime = System.currentTimeMillis();
			if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
				return;
			}
			int start = 0;
			int end = numConnections - 1;
			if (!force) {
				start = rand.nextInt() % numConnections;
				end = rand.nextInt() % numConnections;
				int temp;
				if (end < start) {
					temp = start;
					start = end;
					end = temp;
				}
			}
			int i = start;
			int numNuked = 0;
			while (i <= end) {
				Connection c;
				synchronized (connectionList) {
					try {
						c = connectionList.get(i);
					} catch (Exception e) {
						return;
					}
				}
				if (c.timedOut(currentTime)) {
					if (LOG.isDebugEnabled())
						LOG.debug(getName() + ": disconnecting client "
								+ c.getHostAddress());
					closeConnection(c);
					numNuked++;
					end--;
					c = null;
					if (!force && numNuked == maxConnectionsToNuke)
						break;
				} else
					i++;
			}
			lastCleanupRunTime = System.currentTimeMillis();
		}
	}

	@Override
	public void run() {
		System.out.println("xxx");
		LOG.info(getName() + ": starting");
		// SERVER.set(AbstractServer.this);
		while (server.isRunning()) {
			SelectionKey key = null;
			try {
				getSelector().select();
				Iterator<SelectionKey> iter = getSelector().selectedKeys()
						.iterator();
				while (iter.hasNext()) {
					key = iter.next();
					iter.remove();
					try {
						if (key.isValid()) {
							if (key.isAcceptable())
								doAccept(key);
						}
					} catch (IOException e) {
					}
					key = null;
				}
			} catch (OutOfMemoryError e) {
				// we can run out of memory if we have too many threads
				// log the event and sleep for a minute and give
				// some thread(s) a chance to finish
				LOG.warn("Out of Memory in server select", e);
				closeCurrentConnection(key, e);
				cleanupConnections(true);
				try {
					Thread.sleep(60000);
				} catch (Exception ie) {
				}
			} catch (Exception e) {
				closeCurrentConnection(key, e);
			}
			cleanupConnections(false);
		}
		LOG.info("Stopping " + this.getName());

		synchronized (this) {
			try {
				acceptChannel.close();
				selector.close();
			} catch (IOException e) {
			}

			selector = null;
			acceptChannel = null;

			// clean up all connections
			while (!connectionList.isEmpty()) {
				closeConnection(connectionList.remove(0));
			}
		}
	}

	private void closeCurrentConnection(SelectionKey key, Throwable e) {
		if (key != null) {
			Connection c = (Connection) key.attachment();
			if (c != null) {
				if (LOG.isDebugEnabled())
					LOG.debug(getName() + ": disconnecting client "
							+ c.getHostAddress());
				closeConnection(c);
				c = null;
			}
		}
	}

	InetSocketAddress getAddress() {
		return (InetSocketAddress) acceptChannel.socket().getLocalSocketAddress();
	}

	void doAccept(SelectionKey key) throws IOException, OutOfMemoryError {
		Connection c = null;
		ServerSocketChannel server = (ServerSocketChannel) key.channel();
		SocketChannel channel;
		while ((channel = server.accept()) != null) {

			channel.configureBlocking(false);
			channel.socket().setTcpNoDelay(tcpNoDelay);

			Reader reader = getReader();
			try {
				reader.startAdd();
				SelectionKey readKey = reader.registerChannel(channel);
				c = new Connection(this.server, readKey, channel, System.currentTimeMillis());
				readKey.attach(c);
				synchronized (connectionList) {
					connectionList.add(numConnections, c);
					numConnections++;
				}
				/*
				 * if (LOG.isDebugEnabled()) LOG.debug("Server connection from "
				 * + c.toString() + "; # active connections: " + numConnections
				 * + "; # queued calls: " + callQueue.size());
				 */
			} finally {
				reader.finishAdd();
			}
		}
	}

    @Override
    public synchronized void start() {
        super.start();    //To change body of overridden methods use File | Settings | File Templates.
    }

    void doRead(SelectionKey key) throws InterruptedException {
		int count = 0;
		Connection c = (Connection) key.attachment();
		if (c == null) {
			return;
		}
		c.setLastContact(System.currentTimeMillis());

		try {
			count = c.readAndProcess();
		} catch (InterruptedException ieo) {
			LOG.info(
					getName() + ": readAndProcess caught InterruptedException",
					ieo);
			throw ieo;
		} catch (Exception e) {
			LOG.info(getName() + ": readAndProcess threw exception " + e
					+ " from client " + c.getHostAddress()
					+ ". Count of bytes read: " + count, e);
			count = -1; // so that the (count < 0) block is executed
		}
		if (count < 0) {
			/*
			 * if (LOG.isDebugEnabled()) LOG.debug(getName() +
			 * ": disconnecting client " + c +
			 * ". Number of active connections: "+ numConnections);
			 */
			closeConnection(c);
			c = null;
		} else {
			c.setLastContact(System.currentTimeMillis());
		}
	}

	synchronized void doStop() {
		if (selector != null) {
			selector.wakeup();
			Thread.yield();
		}
		if (acceptChannel != null) {
			try {
				acceptChannel.socket().close();
			} catch (IOException e) {
				LOG.info(getName() + ":Exception in closing listener socket. "
						+ e);
			}
		}
		for (Reader r : readers) {
			r.shutdown();
		}
	}

	synchronized Selector getSelector() {
		return selector;
	}

	// The method that will return the next reader to work with
	// Simplistic implementation of round robin for now
	Reader getReader() {
		currentReader = (currentReader + 1) % readers.length;
		return readers[currentReader];
	}

	public void closeConnection(Connection connection) {
		synchronized (connectionList) {
			if (connectionList.remove(connection))
				numConnections--;
		}
		try {
			connection.close();
		} catch (IOException e) {
		}
	}
}
