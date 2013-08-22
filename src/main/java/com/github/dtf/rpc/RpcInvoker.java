package com.github.dtf.rpc;

import java.io.IOException;


public interface RpcInvoker {   
  /**
   * Process a client call on the server side
   * @param server the server within whose context this rpc call is made
   * @param protocol - the protocol name (the class of the client proxy
   *      used to make calls to the rpc server.
   * @param rpcRequest  - deserialized
   * @param receiveTime time at which the call received (for metrics)
   * @return the call's return
   * @throws IOException
   **/
  public Writable call(AbstractServer server, String protocol,
      Writable rpcRequest, long receiveTime) throws Exception ;
}