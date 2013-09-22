package com.github.dtf.rpc.server;

import java.io.DataInputStream;  
import java.io.DataOutputStream;  
import java.io.IOException;  
import java.net.*;  

import com.github.dtf.rpc.proto.CalculatorMsg.RequestProto;
import com.google.protobuf.*;  
import com.google.protobuf.Descriptors.MethodDescriptor;
  
public class Server1 extends Thread {  
   private Class<?> protocol;  
   private BlockingService impl;  
   private int port;  
   private ServerSocket ss;  
  
   public Server1(Class<?> protocol, BlockingService protocolImpl, int port){  
      this.protocol = protocol;  
      this.impl = protocolImpl;   
      this.port = port;  
   }  
  
   public void run(){  
      Socket clientSocket = null;  
      DataOutputStream dos = null;  
      DataInputStream dis = null;  
      try {  
           ss = new ServerSocket(port);  
       }catch(IOException e){  
       }      
       int testCount = 10; //
  
       while(testCount-- > 0){  
          try {  
               clientSocket = ss.accept();  
               dos = new DataOutputStream(clientSocket.getOutputStream());  
               dis = new DataInputStream(clientSocket.getInputStream());  
               int dataLen = dis.readInt();  
               byte[] dataBuffer = new byte[dataLen];  
               int readCount = dis.read(dataBuffer);  
               byte[] result = processOneRpc(dataBuffer);  
  
               dos.writeInt(result.length);  
               dos.write(result);  
               dos.flush();  
           }catch(Exception e){  
           }  
       }  
       try {   
           dos.close();  
           dis.close();  
           ss.close();  
       }catch(Exception e){  
       };  
  
   }  
  
   public byte[] processOneRpc (byte[] data) throws Exception {  
      RequestProto request = RequestProto.parseFrom(data);  
      String methodName = request.getMethodName();  
      MethodDescriptor methodDescriptor = impl.getDescriptorForType().findMethodByName(methodName);  
      Message response = impl.callBlockingMethod(methodDescriptor, null, request);  
      return response.toByteArray();  
   }  
}  