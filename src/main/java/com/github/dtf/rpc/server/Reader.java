package com.github.dtf.rpc.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.github.dtf.io.nio.RegistrationCallback;
import com.github.dtf.rpc.Call;

public class Reader implements RegistrationCallback{
	Server server;
	public Reader(Server svc){
		server = svc;
	}
	
	public void done(SelectionKey selectionKey) {
		System.out.println("Reader!");
		SocketChannel channel = (SocketChannel) selectionKey.channel();  
        ByteBuffer buffer = ByteBuffer.allocate(1024);  
        try {
			channel.read(buffer);
			buffer.position(0);
			int length = buffer.getInt();
			byte[] data = new byte[length];
			buffer.get(data, 0, length);
			//String msg = new String(data).trim();  
			//System.out.println("Server receive message："+msg);  
			//server.setDataBuffer(data);
			Call newCall = new Call();
			newCall.setClientChannel(channel);
			newCall.setRequestBuffer(data);
			server.getRequestQueue().add(newCall);
			System.out.println("Reader put the data to server's buffer");  
			/*ByteBuffer outBuffer = ByteBuffer.wrap(msg.getBytes());  
			channel.write(outBuffer);*/
			System.out.println("Reader Done!");
		} catch (IOException e) {
			e.printStackTrace();
		}  
	}

}
