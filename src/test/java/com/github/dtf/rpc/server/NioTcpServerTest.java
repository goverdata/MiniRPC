package com.github.dtf.rpc.server;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import org.junit.Test;

import com.github.dtf.io.nio.NioTcpServer;

public class NioTcpServerTest {

	@Test
	public void test() throws UnknownHostException, IOException {
		Server server = mock(Server.class);
		SocketAddress add = new InetSocketAddress("localhost", 3333);
		NioTcpServer tcpServer = new NioTcpServer(server, add);
		tcpServer.bind(add);
		 /*
         * Client
         */
        Socket socket = new Socket("localhost", 3333);
        socket.getOutputStream().write("hahahaha".getBytes());
        socket.getOutputStream().flush();
        socket.close();
        //assertTrue(counter.await(10, TimeUnit.SECONDS));
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
