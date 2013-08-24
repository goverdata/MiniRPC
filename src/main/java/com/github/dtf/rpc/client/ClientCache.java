package com.github.dtf.rpc.client;

import java.util.HashMap;
import java.util.Map;

import javax.net.SocketFactory;

import com.github.dtf.conf.Configuration;
import com.github.dtf.io.ObjectWritable;
import com.github.dtf.rpc.Writable;


/* Cache a client using its socket factory as the hash key */
public class ClientCache {
  private Map<SocketFactory, Client> clients =
    new HashMap<SocketFactory, Client>();

  /**
   * Construct & cache an IPC client with the user-provided SocketFactory 
   * if no cached client exists.
   * 
   * @param conf Configuration
   * @param factory SocketFactory for client socket
   * @param valueClass Class of the expected response
   * @return an IPC client
   */
  public synchronized Client getClient(Configuration conf,
      SocketFactory factory, Class<? extends Writable> valueClass) {
    // Construct & cache client.  The configuration is only used for timeout,
    // and Clients have connection pools.  So we can either (a) lose some
    // connection pooling and leak sockets, or (b) use the same timeout for all
    // configurations.  Since the IPC is usually intended globally, not
    // per-job, we choose (a).
    Client client = clients.get(factory);
    if (client == null) {
      client = new Client(valueClass, conf, factory);
      clients.put(factory, client);
    } else {
      client.incCount();
    }
    return client;
  }

  /**
   * Construct & cache an IPC client with the default SocketFactory 
   * and default valueClass if no cached client exists. 
   * 
   * @param conf Configuration
   * @return an IPC client
   */
  public synchronized Client getClient(Configuration conf) {
    return getClient(conf, SocketFactory.getDefault(), ObjectWritable.class);
  }
  
  /**
   * Construct & cache an IPC client with the user-provided SocketFactory 
   * if no cached client exists. Default response type is ObjectWritable.
   * 
   * @param conf Configuration
   * @param factory SocketFactory for client socket
   * @return an IPC client
   */
  public synchronized Client getClient(Configuration conf, SocketFactory factory) {
    return this.getClient(conf, factory, ObjectWritable.class);
  }

  /**
   * Stop a RPC client connection 
   * A RPC client is closed only when its reference count becomes zero.
   */
  public void stopClient(Client client) {
    synchronized (this) {
      client.decCount();
      if (client.isZeroReference()) {
        clients.remove(client.getSocketFactory());
      }
    }
    if (client.isZeroReference()) {
      client.stop();
    }
  }
}