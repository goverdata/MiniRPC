package com.github.dtf.rpc.server;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import com.github.dtf.io.nio.MessageHandler;
import com.github.dtf.io.nio.NioTcpServer;

public class Listener extends Thread{
	private NioTcpServer listener;
	
	Server server;
	public Listener(Server svc, String host, int port){
		server = svc;
		SocketAddress add = new InetSocketAddress(host, port);
		listener = new NioTcpServer(server, add);
		this.setDaemon(true);
	}
	
	public void run(){
		listener.start();
	}

	public void registMessageHandler(MessageHandler reader) {
		listener.registMessageHandler(reader);
	}

}
