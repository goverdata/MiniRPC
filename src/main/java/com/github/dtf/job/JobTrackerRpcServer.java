package com.github.dtf.job;


import com.github.dtf.conf.Configuration;
import com.github.dtf.rpc.RPC;
import com.github.dtf.rpc.server.Server;

public class JobTrackerRpcServer {

	private Server clientRpcServer;

	public JobTrackerRpcServer(Configuration conf, JobTracker jt) {
		clientRpcServer = RPC.getServer(protocol, instance, bindAddress, port,
				numHandlers, numReaders, queueSizePerHandler, verbose, conf);
	}

	public void start() {
		clientRpcServer.start();
	}

}
