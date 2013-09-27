package com.github.dtf.rpc.server;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.github.dtf.rpc.Call;
import com.google.protobuf.BlockingService;

public class Server {
	private Class<?> protocol;
	private BlockingService impl;
	private Listener listener;
	private Handler handler;
	private Responder responder;
	private String host;
	private int port;
	private byte[] dataBuffer;
	private Queue<Call> requestQueue;
	private Queue<Call> reponseQueue;
	public Server(Class<?> protocol, BlockingService protocolImpl, String host, int port) {
		this.protocol = protocol;
		this.impl = protocolImpl;
		this.host = host;
		this.port = port;
		// 
		requestQueue = new LinkedBlockingQueue<Call>();
		reponseQueue = new LinkedBlockingQueue<Call>();
		listener = new Listener(this, host, port);
		handler = new Handler(this);
		responder = new Responder(this);
	}

	/** Starts the service. Must be called before any calls will be handled. */
	public synchronized void start() {
		// responder.start();
		listener.registMessageHandler(new Reader(this));
		listener.start();
		handler.start();
		responder.start();
	}

	public byte[] getDataBuffer() {
		return dataBuffer;
	}

	public void setDataBuffer(byte[] dataBuffer) {
		this.dataBuffer = dataBuffer;
	}

	public BlockingService getBlockingService() {
		return impl;
	}

	public Queue<Call> getRequestQueue() {
		return requestQueue;
	}

	public void setRequestQueue(ConcurrentLinkedQueue<Call> callQueue) {
		this.requestQueue = callQueue;
	}
	
	public Queue<Call> getResponseQueue() {
		return reponseQueue;
	}
	
	public void setResponseQueue(ConcurrentLinkedQueue<Call> callQueue) {
		this.reponseQueue = callQueue;
	}
}