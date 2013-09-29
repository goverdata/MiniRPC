package com.github.dtf.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Random;

import com.github.dtf.rpc.proto.CalculatorMsg.RequestProto;
import com.github.dtf.rpc.proto.CalculatorMsg.ResponseProto;
import com.github.dtf.rpc.protocol.Calculator;

public class TestCalculator implements Calculator {  
	  
    public int doTest(String op, int a, int b){  
        Socket s = null;  
        DataOutputStream out = null;  
        DataInputStream in = null;  
        int ret = 0;  
        try {  
        	System.out.println("Client start to connect:" + System.currentTimeMillis());
            s= new Socket("localhost", 8038);  
            System.out.println("Client connect completed:" + System.currentTimeMillis());
            out = new DataOutputStream(s.getOutputStream());  
            in = new DataInputStream(s.getInputStream());  
              
            RequestProto.Builder builder = RequestProto.newBuilder();  
            builder.setMethodName(op);  
            builder.setNum1(a);  
            builder.setNum2(b);  
            RequestProto request = builder.build();  
              
            byte [] bytes = request.toByteArray();  
            out.writeInt(bytes.length);  
            out.write(bytes);  
            out.flush();  
            System.out.println("Client send complete:" + System.currentTimeMillis());
       
            int dataLen = in.readInt();  
            byte[] data = new byte[dataLen];  
            int count = in.read(data);  
            if(count != dataLen){  
                System.err.println("something bad happened!");  
            }  
              
            ResponseProto result = ResponseProto.parseFrom(data);  
            System.out.println(a + " " + op + " " +  b + "=" + result.getResult());              
            ret =  result.getResult();  
              
        }catch(Exception e){  
            e.printStackTrace();  
            System.err.println(e.toString());  
        }finally {  
            try{  
            in.close();  
            out.close();  
            s.close();  
            }catch(IOException e){  
                e.printStackTrace();  
            }  
        }  
        return ret;  
    }  
    
    public int add(int a, int b) {  
        return doTest("add", a, b);  
    }  

    public int minus(int a, int b) {  
        return doTest("minus", a, b);  
    }  
  
    /** 
     * @param args 
     */  
    public static void main(String[] args) {
        TestCalculator tc = new TestCalculator();  
        int testCount = 5;  
        Random rand = new Random();  
        while(testCount-- > 0){  
            int a = rand.nextInt(100);  
            int b = rand.nextInt(100);  
            tc.add(a,b);  
            tc.minus(a, b);  
        }          
          
    }  
  
}  