package com.github.dtf.rpc.protocol;

import com.github.dtf.rpc.proto.CalculatorMsg.RequestProto;
import com.github.dtf.rpc.proto.CalculatorMsg.ResponseProto;
import com.google.protobuf.RpcController;  
import com.google.protobuf.ServiceException;  
  
public class CalculatorPBServiceImpl implements CalculatorPB {  
  
    public Calculator real;  
      
    public CalculatorPBServiceImpl(Calculator impl){  
        this.real = impl;  
    }  
      
    public ResponseProto add(RpcController controller, RequestProto request) throws ServiceException {  
        ResponseProto proto = ResponseProto.getDefaultInstance();  
        ResponseProto.Builder build = ResponseProto.newBuilder();  
        int add1 = request.getNum1();  
        int add2 = request.getNum2();  
        int sum = real.add(add1, add2);  
        ResponseProto result = null;  
        build.setResult(sum);  
        result = build.build();  
        return result;  
    }  
  
    public ResponseProto minus(RpcController controller, RequestProto request) throws ServiceException { 
        ResponseProto proto = ResponseProto.getDefaultInstance();  
        ResponseProto.Builder build = ResponseProto.newBuilder();  
        int add1 = request.getNum1();  
        int add2 = request.getNum2();  
        int sum = real.minus(add1, add2);  
        ResponseProto result = null;  
        build.setResult(sum);  
        result = build.build();  
        return result;  
    }  
  
}  