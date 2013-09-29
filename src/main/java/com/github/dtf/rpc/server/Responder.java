package com.github.dtf.rpc.server;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.github.dtf.rpc.Call;
import com.github.dtf.utils.ByteUtils;
import com.google.common.primitives.Bytes;

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
					System.out.println(newCall);
					byte[] result = newCall.getReponseBuffer();
					System.out.println("length:" + result.length);
					SocketChannel channel = newCall.getClientChannel();
//					OutputStream dos = channel.socket().getOutputStream(); 
//					dos.write(result.length);  
//		            dos.write(result);  
//		            dos.flush();  
					Integer length = result.length;
					System.out.println("Get from server");
					ByteBuffer buffer = ByteBuffer.allocate(4 + length);
					
					buffer.putInt(length);
					System.out.println(buffer);
					ByteUtils.printBytes(buffer.array());
					
					buffer.put(result);
					System.out.println(buffer);
					ByteUtils.printBytes(buffer.array());
					
//					buffer.position(0);
					buffer.flip();
					channel.write(buffer);
					System.out.println("Responder done!");
//					channel.finishConnect();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
