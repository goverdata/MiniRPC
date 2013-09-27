package com.github.dtf.rpc.server;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.github.dtf.rpc.Call;

public class Responder extends Thread{
	Server server;
	public Responder(Server svc){
		server = svc;
		this.setDaemon(true);
	}
	
	public void run(){
		while(true){
			try {
				if(server.getResponseQueue().size() > 0){
					System.out.println("Responder catch the msg from Reader!");
					Call newCall = server.getResponseQueue().remove();
					System.out.println("Responder call:" + newCall);
					byte[] result = newCall.getReponseBuffer();
					SocketChannel channel = newCall.getClientChannel();
					ByteBuffer buffer = ByteBuffer.allocate(4 + result.length);
					buffer.putInt(result.length);
					buffer.put(result);
					channel.write(buffer);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
