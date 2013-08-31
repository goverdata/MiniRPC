package com.github.dtf.utils;

import java.io.DataInput;

import com.github.dtf.protocol.AddressBookProtos;
import com.github.dtf.protocol.AddressBookProtos.AddressBook;
import com.github.dtf.rpc.RPC;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcKindProto;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcPayloadHeaderProto;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcPayloadOperationProto;
import com.github.dtf.rpc.RpcType;


public class ProtoUtil {
	static RpcKindProto convert(RpcType kind) {
	    switch (kind) {
	    case RPC_BUILTIN: return RpcKindProto.RPC_BUILTIN;
	    case RPC_WRITABLE: return RpcKindProto.RPC_WRITABLE;
	    case RPC_PROTOCOL_BUFFER: return RpcKindProto.RPC_PROTOCOL_BUFFER;
	    }
	    return null;
	  }
	  
	  
	  public static RpcType convert( RpcKindProto kind) {
	    switch (kind) {
	    case RPC_BUILTIN: return RpcType.RPC_BUILTIN;
	    case RPC_WRITABLE: return RpcType.RPC_WRITABLE;
	    case RPC_PROTOCOL_BUFFER: return RpcType.RPC_PROTOCOL_BUFFER;
	    }
	    return null;
	  }
	 
	  public static RpcPayloadHeaderProto makeRpcPayloadHeader(RpcType rpcKind,
	      RpcPayloadOperationProto operation, int callId) {
	    RpcPayloadHeaderProto.Builder result = RpcPayloadHeaderProto.newBuilder();
	    result.setRpcKind(convert(rpcKind)).setRpcOp(operation).setCallId(callId);
	    return result.build();
	  }
	  
	  public static AddressBook makeAddressBookProtosHeader(RpcType rpcKind,
			  RpcPayloadOperationProto operation, int callId) {
		  AddressBookProtos.AddressBook.Builder addBook = AddressBookProtos.AddressBook.newBuilder();
		  AddressBookProtos.Person.Builder person = AddressBookProtos.Person.newBuilder();
		  addBook.setPerson(0, person);
		  return addBook.build();
	  }


	public static int readRawVarint32(DataInput in) {
		// TODO Auto-generated method stub
		return 0;
	}
}
