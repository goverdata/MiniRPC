package com.github.dtf.rpc.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.github.dtf.io.nio.RegistrationCallback;

public class Reader implements RegistrationCallback{
	Server server;
	public Reader(Server svc){
		server = svc;
	}
	
	public void done(SelectionKey selectionKey) {
		System.out.println("Reader!");
		SocketChannel channel = (SocketChannel) selectionKey.channel();  
        ByteBuffer buffer = ByteBuffer.allocate(10);  
        try {
			channel.read(buffer);
			byte[] data = buffer.array();  
			String msg = new String(data).trim();  
			System.out.println("Server receive message："+msg);  
			server.setDataBuffer(data);
			System.out.println("Reader put the data to server's buffer");  
			/*ByteBuffer outBuffer = ByteBuffer.wrap(msg.getBytes());  
			channel.write(outBuffer);*/
			System.out.println("Reader Done!");
		} catch (IOException e) {
			e.printStackTrace();
		}  
	}

}
