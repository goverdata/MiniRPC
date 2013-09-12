package com.github.dtf.rpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.net.SocketFactory;

import com.github.dtf.conf.Configuration;
import com.github.dtf.io.RetryPolicy;
import com.github.dtf.protocol.TaskUmbilicalProtocol;
import com.github.dtf.protocol.Test1;
import com.github.dtf.rpc.WritableRpcEngine;
import com.github.dtf.rpc.protocol.ProtobufRpcEngine;
import com.github.dtf.rpc.protocol.ProtocolProxy;
import com.github.dtf.security.UserGroupInformation;

public class MockClient {
	
	public static void main(String[] args) throws InterruptedException, IOException {
//		Client cli = new Client(null, null);
		InetSocketAddress addr = new InetSocketAddress("localhost", 2233);
//		UserGroupInformation ticket = new UserGroupInformation();
		Configuration conf = new Configuration();
//		TestParam param = new TestParam();
//		cli.call(param, addr, TaskUmbilicalProtocol.class, ticket, 10000, conf);
		ProtobufRpcEngine engine = new ProtobufRpcEngine();
		SocketFactory sf = SocketFactory.getDefault();
		RetryPolicy connectionRetryPolicy = null;
		Test1 sys = engine.getProxy(Test1.class, 1L, addr, conf, sf, 5000, connectionRetryPolicy).getProxy();
		sys.printName("johnny");
	}
}
