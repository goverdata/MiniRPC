package com.github.dtf.rpc.server;

import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.github.dtf.rpc.Call;
import com.google.protobuf.BlockingService;

public class Server {
	private Class<?> protocol;
	private BlockingService impl;
	private ServerSocket ss;
	private Listener listener;
	private Handler handler;
	private Responder responder;
	private String host;
	private int port;
	private byte[] dataBuffer;
	private ConcurrentLinkedQueue<Call> requestQueue;
	private ConcurrentLinkedQueue<Call> reponseQueue;
	public Server(Class<?> protocol, BlockingService protocolImpl, String host, int port) {
		this.protocol = protocol;
		this.impl = protocolImpl;
		this.host = host;
		this.port = port;
		requestQueue = new ConcurrentLinkedQueue<Call>();
		reponseQueue = new ConcurrentLinkedQueue<Call>();
//		SocketAddress add = new InetSocketAddress(host, port);
		listener = new Listener(this, host, port);
		handler = new Handler(this);
		responder = new Responder(this);
	}

	/*public void run() {
		Socket clientSocket = null;
		DataOutputStream dos = null;
		DataInputStream dis = null;
		try {
			ss = new ServerSocket(port);
		} catch (IOException e) {
		}
		int testCount = 10; //

		while (testCount-- > 0) {
			try {
				clientSocket = ss.accept();
				dos = new DataOutputStream(clientSocket.getOutputStream());
				dis = new DataInputStream(clientSocket.getInputStream());
				int dataLen = dis.readInt();
				byte[] dataBuffer = new byte[dataLen];
				int readCount = dis.read(dataBuffer);
				byte[] result = processOneRpc(dataBuffer);

				dos.writeInt(result.length);
				dos.write(result);
				dos.flush();
			} catch (Exception e) {
			}
		}
		try {
			dos.close();
			dis.close();
			ss.close();
		} catch (Exception e) {
		}
		;

	}*/

	/** Starts the service. Must be called before any calls will be handled. */
	public synchronized void start() {
		// responder.start();
		listener.registCallback(new Reader(this));
		listener.start();
		handler.start();
		responder.start();
		// handlers = new Handler[handlerCount];
		//
		// for (int i = 0; i < handlerCount; i++) {
		// handlers[i] = new Handler(i);
		// handlers[i].start();
		// }
	}

	/*public byte[] processOneRpc(byte[] data) throws Exception {
		RequestProto request = RequestProto.parseFrom(data);
		String methodName = request.getMethodName();
		MethodDescriptor methodDescriptor = impl.getDescriptorForType()
				.findMethodByName(methodName);
		Message response = impl.callBlockingMethod(methodDescriptor, null,
				request);
		return response.toByteArray();
	}*/

	public byte[] getDataBuffer() {
		return dataBuffer;
	}

	public void setDataBuffer(byte[] dataBuffer) {
		this.dataBuffer = dataBuffer;
////		this.dataBuffer = new byte[length];
//		System.arraycopy(dataBuffer, 0, this.dataBuffer, 0, length);
	}

	public BlockingService getBlockingService() {
		return impl;
	}

	public ConcurrentLinkedQueue<Call> getRequestQueue() {
		return requestQueue;
	}

	public void setRequestQueue(ConcurrentLinkedQueue<Call> callQueue) {
		this.requestQueue = callQueue;
	}
	
	public ConcurrentLinkedQueue<Call> getResponseQueue() {
		return reponseQueue;
	}
	
	public void setResponseQueue(ConcurrentLinkedQueue<Call> callQueue) {
		this.reponseQueue = callQueue;
	}
}