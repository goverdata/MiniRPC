package com.github.dtf.rpc;

import java.nio.channels.SocketChannel;

public class Call {
	private byte[] requestBuffer;
	private byte[] reponseBuffer;
	
	private SocketChannel clientChannel;
	
	public byte[] getRequestBuffer() {
		return requestBuffer;
	}
	public void setRequestBuffer(byte[] databuffer) {
		this.requestBuffer = databuffer;
	}
	public SocketChannel getClientChannel() {
		return clientChannel;
	}
	public void setClientChannel(SocketChannel clientChannel) {
		this.clientChannel = clientChannel;
	}
	public byte[] getReponseBuffer() {
		return reponseBuffer;
	}
	public void setReponseBuffer(byte[] reponseBuffer) {
		this.reponseBuffer = reponseBuffer;
	}
}
