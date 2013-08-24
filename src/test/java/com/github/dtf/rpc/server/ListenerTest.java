package com.github.dtf.rpc.server;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.net.InetAddress;

import org.junit.Before;
import org.junit.Test;

public class ListenerTest {

	AbstractServer server = null;
	
	@Before
	public void init(){
		server = mock(AbstractServer.class);
	}
	
	@Test
	public void test() throws IOException {
		when(server.isRunning()).thenReturn(true);
		InetAddress addr = InetAddress.getByName("localhost");
		Listener lis = new Listener(server, addr, 2233, true);
		lis.start();
		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			System.err.println(e);
		}
	}

}
