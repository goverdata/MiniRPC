package com.github.dtf.rpc.server;

import com.github.dtf.rpc.Call;
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
		while(true){
			try {
				if(server.getRequestQueue().size() > 0){
					System.out.println("Handler catch the msg from Reader!");
					Call newCall = server.getRequestQueue().remove();
					System.out.println("Handler call info :" + newCall.toString());
					byte[] request = newCall.getRequestBuffer();
					byte[] response = processOneRpc(request);
					newCall.setReponseBuffer(response);
					server.getResponseQueue().add(newCall);
				}
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
