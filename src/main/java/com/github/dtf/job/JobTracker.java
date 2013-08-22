package com.github.dtf.job;

public class JobTracker {
	JobTrackerRpcServer rpcServer;
	
	void init(){
		rpcServer = new JobTrackerRpcServer();
		rpcServer.start();
		
	}
	
	public static void main(String[] args) {
		JobTracker jt = new JobTracker();
		jt.init();
	}
}
