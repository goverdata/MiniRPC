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
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		if(clientChannel != null){
			sb.append("Rpc request from:" + clientChannel.socket().getRemoteSocketAddress() + "]");
		}
		if(requestBuffer != null){
			sb.append(" [request data:" + new String(requestBuffer) + "]");
		}
		if(reponseBuffer != null){
			sb.append(" [response data:" + new String(reponseBuffer)+ "]");
		}
		return sb.toString();
	}
}
