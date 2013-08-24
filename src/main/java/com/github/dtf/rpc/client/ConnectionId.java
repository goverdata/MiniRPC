package com.github.dtf.rpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.github.dtf.conf.CommonConfigurationKeys;
import com.github.dtf.conf.CommonConfigurationKeysPublic;
import com.github.dtf.conf.Configuration;
import com.github.dtf.security.UserGroupInformation;
import com.github.dtf.transport.RetryPolicy;

//@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
//@InterfaceStability.Evolving
public class ConnectionId {
  InetSocketAddress address;
  UserGroupInformation ticket;
  final Class<?> protocol;
  private static final int PRIME = 16777619;
  private final int rpcTimeout;
  private final String serverPrincipal;
  private final int maxIdleTime; //connections will be culled if it was idle for 
  //maxIdleTime msecs
  private static RetryPolicy connectionRetryPolicy;
  // the max. no. of retries for socket connections on time out exceptions
  private final int maxRetriesOnSocketTimeouts;
  private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  private final boolean doPing; //do we need to send ping message
  private final int pingInterval; // how often sends ping to the server in msecs
  
  public ConnectionId(InetSocketAddress address, Class<?> protocol){
	  this.address = address;
	  this.protocol = protocol;
	  rpcTimeout = 100;
	  serverPrincipal = null;
	  maxIdleTime = 100;
	  connectionRetryPolicy = null;
	  maxRetriesOnSocketTimeouts = 100;
	  tcpNoDelay = false;
	  doPing = false;
	  pingInterval = 100;
  }
  
  public ConnectionId(InetSocketAddress address, Class<?> protocol, 
		   int rpcTimeout,
		  String serverPrincipal, int maxIdleTime, 
		  RetryPolicy connectionRetryPolicy, int maxRetriesOnSocketTimeouts,
		  boolean tcpNoDelay, boolean doPing, int pingInterval) {
	  this.protocol = protocol;
	  this.address = address;
	  this.rpcTimeout = rpcTimeout;
	  this.serverPrincipal = serverPrincipal;
	  this.maxIdleTime = maxIdleTime;
	  this.connectionRetryPolicy = connectionRetryPolicy;
	  this.maxRetriesOnSocketTimeouts = maxRetriesOnSocketTimeouts;
	  this.tcpNoDelay = tcpNoDelay;
	  this.doPing = doPing;
	  this.pingInterval = pingInterval;
  }
  
  
  public ConnectionId(InetSocketAddress address, Class<?> protocol, 
               UserGroupInformation ticket, int rpcTimeout,
               String serverPrincipal, int maxIdleTime, 
               RetryPolicy connectionRetryPolicy, int maxRetriesOnSocketTimeouts,
               boolean tcpNoDelay, boolean doPing, int pingInterval) {
    this.protocol = protocol;
    this.address = address;
    this.ticket = ticket;
    this.rpcTimeout = rpcTimeout;
    this.serverPrincipal = serverPrincipal;
    this.maxIdleTime = maxIdleTime;
    this.connectionRetryPolicy = connectionRetryPolicy;
    this.maxRetriesOnSocketTimeouts = maxRetriesOnSocketTimeouts;
    this.tcpNoDelay = tcpNoDelay;
    this.doPing = doPing;
    this.pingInterval = pingInterval;
  }
  
  public InetSocketAddress getAddress() {
    return address;
  }
  
  public Class<?> getProtocol() {
    return protocol;
  }
  
  public UserGroupInformation getTicket() {
    return ticket;
  }
  
  public int getRpcTimeout() {
    return rpcTimeout;
  }
  
  public String getServerPrincipal() {
    return serverPrincipal;
  }
  
  public int getMaxIdleTime() {
    return maxIdleTime;
  }
  
  /** max connection retries on socket time outs */
  public int getMaxRetriesOnSocketTimeouts() {
    return maxRetriesOnSocketTimeouts;
  }
  
  boolean getTcpNoDelay() {
    return tcpNoDelay;
  }
  
  boolean getDoPing() {
    return doPing;
  }
  
  int getPingInterval() {
    return pingInterval;
  }
  
//  static ConnectionId getConnectionId(InetSocketAddress addr,
//      Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
//      Configuration conf) throws IOException {
//    return getConnectionId(addr, protocol, ticket, rpcTimeout, null, conf);
//  }

  /**
   * Returns a ConnectionId object. 
   * @param addr Remote address for the connection.
   * @param protocol Protocol for RPC.
   * @param ticket UGI
   * @param rpcTimeout timeout
   * @param conf Configuration object
   * @return A ConnectionId instance
   * @throws IOException
   */
  public static ConnectionId getConnectionId(InetSocketAddress addr,
      Class<?> protocol, int rpcTimeout,
       Configuration conf) throws IOException {

//    if (connectionRetryPolicy == null) {
//      final int max = conf.getInt(
//          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
//          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);
//      connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
//          max, 1, TimeUnit.SECONDS);
//    }

//    String remotePrincipal = getRemotePrincipal(conf, addr, protocol);
    boolean doPing =
      conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
    return new ConnectionId(addr, protocol, rpcTimeout, null,
        conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT),
        connectionRetryPolicy,
        conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT),
        conf.getBoolean(CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT),
        doPing, 
        (doPing ? Client.getPingInterval(conf) : 0));
  }
  
  /*private static String getRemotePrincipal(Configuration conf,
      InetSocketAddress address, Class<?> protocol) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled() || protocol == null) {
      return null;
    }
    KerberosInfo krbInfo = SecurityUtil.getKerberosInfo(protocol, conf);
    if (krbInfo != null) {
      String serverKey = krbInfo.serverPrincipal();
      if (serverKey == null) {
        throw new IOException(
            "Can't obtain server Kerberos config key from protocol="
                + protocol.getCanonicalName());
      }
      return SecurityUtil.getServerPrincipal(conf.get(serverKey), address
          .getAddress());
    }
    return null;
  }*/
  
  static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof ConnectionId) {
      ConnectionId that = (ConnectionId) obj;
      return isEqual(this.address, that.address)
          && this.doPing == that.doPing
          && this.maxIdleTime == that.maxIdleTime
          && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
          && this.pingInterval == that.pingInterval
          && isEqual(this.protocol, that.protocol)
          && this.rpcTimeout == that.rpcTimeout
          && isEqual(this.serverPrincipal, that.serverPrincipal)
          && this.tcpNoDelay == that.tcpNoDelay
          && isEqual(this.ticket, that.ticket);
    }
    return false;
  }
  
  @Override
  public int hashCode() {
		// int result = connectionRetryPolicy.hashCode();
		// FIXME TEST
    int result = 1024;
    result = PRIME * result + ((address == null) ? 0 : address.hashCode());
    result = PRIME * result + (doPing ? 1231 : 1237);
    result = PRIME * result + maxIdleTime;
    result = PRIME * result + pingInterval;
    result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
    result = PRIME * result + rpcTimeout;
    result = PRIME * result
        + ((serverPrincipal == null) ? 0 : serverPrincipal.hashCode());
    result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
    result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
    return result;
  }
  
  @Override
  public String toString() {
    return serverPrincipal + "@" + address;
  }
}  