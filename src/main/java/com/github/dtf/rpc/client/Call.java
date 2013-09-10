package com.github.dtf.rpc.client;

import java.io.IOException;

import com.github.dtf.rpc.RPC;
import com.github.dtf.rpc.RpcType;
import com.github.dtf.rpc.Writable;


/** 
 * Class that represents an RPC call
 */
public class Call {
  final int id;               // call id
  final Writable rpcRequest;  // the serialized rpc request - RpcPayload
  Writable rpcResponse;       // null if rpc has error
  IOException error;          // exception, null if success
  final RpcType rpcKind;      // Rpc EngineKind
  boolean done;               // true when call is done

  protected Call(RpcType rpcKind, Writable param, Client client) {
    this.rpcKind = rpcKind;
    this.rpcRequest = param;
    synchronized (client) {
      this.id = client.getCounter();
    }
  }

  /** Indicate when the call is complete and the
   * value or error are available.  Notifies by default.  */
  protected synchronized void callComplete() {
    this.done = true;
    notify();                                 // notify caller
  }

  /** Set the exception when there is an error.
   * Notify the caller the call is done.
   * 
   * @param error exception thrown by the call; either local or remote
   */
  public synchronized void setException(IOException error) {
    this.error = error;
    callComplete();
  }
  
  /** Set the return value when there is no error. 
   * Notify the caller the call is done.
   * 
   * @param rpcResponse return value of the rpc call.
   */
  public synchronized void setRpcResponse(Writable rpcResponse) {
    this.rpcResponse = rpcResponse;
    callComplete();
  }
  
  public synchronized Writable getRpcResult() {
    return rpcResponse;
  }
}