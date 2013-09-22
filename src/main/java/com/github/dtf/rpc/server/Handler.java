package com.github.dtf.rpc.server;

import com.github.dtf.rpc.proto.CalculatorMsg.RequestProto;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.MethodDescriptor;

public class Handler extends Thread{
	Server server;
	private BlockingService impl;
	public Handler(Server svc){
		server = svc;
		impl = server.getBlockingService();
		this.setDaemon(true);
	}
	
	public void run(){
		while(server.getDataBuffer() != null && server.getDataBuffer().length > 0){
			try {
				processOneRpc(server.getDataBuffer());
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public byte[] processOneRpc(byte[] data) throws Exception {
		RequestProto request = RequestProto.parseFrom(data);
		String methodName = request.getMethodName();
		MethodDescriptor methodDescriptor = impl.getDescriptorForType()
				.findMethodByName(methodName);
		Message response = impl.callBlockingMethod(methodDescriptor, null,
				request);
		return response.toByteArray();
	}
}
