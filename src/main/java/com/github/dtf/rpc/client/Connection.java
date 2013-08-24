package com.github.dtf.rpc.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.security.PrivilegedExceptionAction;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.github.common.http.NetUtils;
import com.github.common.utils.IOUtils;
import com.github.common.utils.WritableUtils;
import com.github.dtf.conf.Configuration;
import com.github.dtf.io.DataOutputBuffer;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcPayloadHeaderProto;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcPayloadOperationProto;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcResponseHeaderProto;
import com.github.dtf.rpc.RpcPayloadHeaderProtos.RpcStatusProto;
import com.github.dtf.rpc.Writable;
import com.github.dtf.rpc.server.Server;
import com.github.dtf.security.UserGroupInformation;
import com.github.dtf.transport.RetryPolicy;
import com.github.dtf.utils.ProtoUtil;
import com.github.dtf.utils.ReflectionUtils;


/** Thread that reads responses and notifies callers.  Each connection owns a
 * socket connected to a remote address.  Calls are multiplexed through this
 * socket: responses may be delivered out of order. */
public class Connection extends Thread {
  public static final Log LOG = LogFactory.getLog(Connection.class);
  Client client;
  final static int PING_CALL_ID = -1;
//  final private Configuration conf;
  private Class<? extends Writable> valueClass;   // class of call values
  private int counter;                            // counter for call ids
  
  private InetSocketAddress server;             // server ip:port
  private String serverPrincipal;  // server's krb5 principal name
//  private IpcConnectionContextProto connectionContext;   // connection context
  private final ConnectionId remoteId;                // connection id
//  private AuthMethod authMethod; // authentication method
  private boolean useSasl;
//  private Token<? extends TokenIdentifier> token;
//  private SaslRpcClient saslRpcClient;
  
  private Socket socket = null;                 // connected socket
  private DataInputStream in;
  private DataOutputStream out;
  private int rpcTimeout;
  private int maxIdleTime; //connections will be culled if it was idle for 
  //maxIdleTime msecs
//  private final RetryPolicy connectionRetryPolicy;
  private int maxRetriesOnSocketTimeouts;
  private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  private boolean doPing; //do we need to send ping message
  private int pingInterval; // how often sends ping to the server in msecs
  
  // currently active calls
  private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
  private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
  private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
  private IOException closeException; // close reason

  public Connection(Client client, ConnectionId remoteId) throws IOException {
	this.client = client;
    this.remoteId = remoteId;
    this.server = remoteId.getAddress();
    if (server.isUnresolved()) {
      /*throw NetUtils.wrapException(server.getHostName(),
          server.getPort(),
          null,
          0,
          new UnknownHostException());*/
    	throw new IOException();
    }
    this.rpcTimeout = remoteId.getRpcTimeout();
    this.maxIdleTime = remoteId.getMaxIdleTime();
//    this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
    this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();
    this.tcpNoDelay = remoteId.getTcpNoDelay();
    this.doPing = remoteId.getDoPing();
    this.pingInterval = remoteId.getPingInterval();
    if (LOG.isDebugEnabled()) {
      LOG.debug("The ping interval is " + this.pingInterval + " ms.");
    }

    UserGroupInformation ticket = remoteId.getTicket();
    Class<?> protocol = remoteId.getProtocol();
    /*this.useSasl = UserGroupInformation.isSecurityEnabled();
    if (useSasl && protocol != null) {
      TokenInfo tokenInfo = SecurityUtil.getTokenInfo(protocol, conf);
      if (tokenInfo != null) {
        TokenSelector<? extends TokenIdentifier> tokenSelector = null;
        try {
          tokenSelector = tokenInfo.value().newInstance();
        } catch (InstantiationException e) {
          throw new IOException(e.toString());
        } catch (IllegalAccessException e) {
          throw new IOException(e.toString());
        }
        token = tokenSelector.selectToken(
            SecurityUtil.buildTokenService(server),
            ticket.getTokens());
      }
      KerberosInfo krbInfo = SecurityUtil.getKerberosInfo(protocol, conf);
      if (krbInfo != null) {
        serverPrincipal = remoteId.getServerPrincipal();
        if (LOG.isDebugEnabled()) {
          LOG.debug("RPC Server's Kerberos principal name for protocol="
              + protocol.getCanonicalName() + " is " + serverPrincipal);
        }
      }
    }
    
    if (!useSasl) {
      authMethod = AuthMethod.SIMPLE;
    } else if (token != null) {
      authMethod = AuthMethod.DIGEST;
    } else {
      authMethod = AuthMethod.KERBEROS;
    }
    
    connectionContext = ProtoUtil.makeIpcConnectionContext(
        RPC.getProtocolName(protocol), ticket, authMethod);
    
    if (LOG.isDebugEnabled())
      LOG.debug("Use " + authMethod + " authentication for protocol "
          + protocol.getSimpleName());
    
    this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
        server.toString() +
        " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));*/
    this.setDaemon(true);
  }

  /** Update lastActivity with the current time. */
  private void touch() {
    lastActivity.set(System.currentTimeMillis());
  }

  /**
   * Add a call to this connection's call queue and notify
   * a listener; synchronized.
   * Returns false if called during shutdown.
   * @param call to add
   * @return true if the call was added.
   */
  synchronized boolean addCall(Call call) {
    if (shouldCloseConnection.get()){
      return false;
    }
    calls.put(call.id, call);
    notify();
    return true;
  }

  /** This class sends a ping to the remote side when timeout on
   * reading. If no failure is detected, it retries until at least
   * a byte is read.
   */
  private class PingInputStream extends FilterInputStream {
    /* constructor */
    protected PingInputStream(InputStream in) {
      super(in);
    }

    /* Process timeout exception
     * if the connection is not going to be closed or 
     * is not configured to have a RPC timeout, send a ping.
     * (if rpcTimeout is not set to be 0, then RPC should timeout.
     * otherwise, throw the timeout exception.
     */
    private void handleTimeout(SocketTimeoutException e) throws IOException {
      if (shouldCloseConnection.get() || !running.get() || rpcTimeout > 0) {
        throw e;
      } else {
        sendPing();
      }
    }
    
    /** Read a byte from the stream.
     * Send a ping if timeout on read. Retries if no failure is detected
     * until a byte is read.
     * @throws IOException for any IO problem other than socket timeout
     */
    public int read() throws IOException {
      do {
        try {
          return super.read();
        } catch (SocketTimeoutException e) {
          handleTimeout(e);
        }
      } while (true);
    }

    /** Read bytes into a buffer starting from offset <code>off</code>
     * Send a ping if timeout on read. Retries if no failure is detected
     * until a byte is read.
     * 
     * @return the total number of bytes read; -1 if the connection is closed.
     */
    public int read(byte[] buf, int off, int len) throws IOException {
      do {
        try {
          return super.read(buf, off, len);
        } catch (SocketTimeoutException e) {
          handleTimeout(e);
        }
      } while (true);
    }
  }
  
//  private synchronized void disposeSasl() {
//    if (saslRpcClient != null) {
//      try {
//        saslRpcClient.dispose();
//        saslRpcClient = null;
//      } catch (IOException ignored) {
//      }
//    }
//  }
  
//  private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
//    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
//    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
//    UserGroupInformation realUser = currentUser.getRealUser();
//    if (authMethod == AuthMethod.KERBEROS && loginUser != null &&
//    // Make sure user logged in using Kerberos either keytab or TGT
//        loginUser.hasKerberosCredentials() &&
//        // relogin only in case it is the login user (e.g. JT)
//        // or superuser (like oozie).
//        (loginUser.equals(currentUser) || loginUser.equals(realUser))) {
//      return true;
//    }
//    return false;
//  }
  
//  private synchronized boolean setupSaslConnection(final InputStream in2, 
//      final OutputStream out2) 
//      throws IOException {
//    saslRpcClient = new SaslRpcClient(authMethod, token, serverPrincipal);
//    return saslRpcClient.saslConnect(in2, out2);
//  }

  /**
   * Update the server address if the address corresponding to the host
   * name has changed.
   *
   * @return true if an addr change was detected.
   * @throws IOException when the hostname cannot be resolved.
   */
  private synchronized boolean updateAddress() throws IOException {
    // Do a fresh lookup with the old host name.
    InetSocketAddress currentAddr = NetUtils.createSocketAddrForHost(
                             server.getHostName(), server.getPort());

    if (!server.equals(currentAddr)) {
      LOG.warn("Address change detected. Old: " + server.toString() +
                               " New: " + currentAddr.toString());
      server = currentAddr;
      return true;
    }
    return false;
  }
  
  private synchronized void setupConnection() throws IOException {
    short ioFailures = 0;
    short timeoutFailures = 0;
    while (true) {
      try {
        this.socket = client.getSocketFactory().createSocket();
        this.socket.setTcpNoDelay(tcpNoDelay);
        
        /*
         * Bind the socket to the host specified in the principal name of the
         * client, to ensure Server matching address of the client connection
         * to host name in principal passed.
         */
        /*if (UserGroupInformation.isSecurityEnabled()) {
          KerberosInfo krbInfo = 
            remoteId.getProtocol().getAnnotation(KerberosInfo.class);
          if (krbInfo != null && krbInfo.clientPrincipal() != null) {
            String host = 
              SecurityUtil.getHostFromPrincipal(remoteId.getTicket().getUserName());
            
            // If host name is a valid local address then bind socket to it
            InetAddress localAddr = NetUtils.getLocalInetAddress(host);
            if (localAddr != null) {
              this.socket.bind(new InetSocketAddress(localAddr, 0));
            }
          }
        }*/
        
        // connection time out is 20s
        NetUtils.connect(this.socket, server, 20000);
        if (rpcTimeout > 0) {
          pingInterval = rpcTimeout;  // rpcTimeout overwrites pingInterval
        }
        this.socket.setSoTimeout(pingInterval);
        return;
      } catch (SocketTimeoutException toe) {
        /* Check for an address change and update the local reference.
         * Reset the failure counter if the address was changed
         */
        if (updateAddress()) {
          timeoutFailures = ioFailures = 0;
        }
        handleConnectionFailure(timeoutFailures++,
            maxRetriesOnSocketTimeouts, toe);
      } catch (IOException ie) {
        if (updateAddress()) {
          timeoutFailures = ioFailures = 0;
        }
        // FIXME
        //handleConnectionFailure(ioFailures++, ie);
      }
    }
  }

  /**
   * If multiple clients with the same principal try to connect to the same
   * server at the same time, the server assumes a replay attack is in
   * progress. This is a feature of kerberos. In order to work around this,
   * what is done is that the client backs off randomly and tries to initiate
   * the connection again. The other problem is to do with ticket expiry. To
   * handle that, a relogin is attempted.
   */
//  private synchronized void handleSaslConnectionFailure(
//      final int currRetries, final int maxRetries, final Exception ex,
//      final Random rand, final UserGroupInformation ugi) throws IOException,
//      InterruptedException {
//    ugi.doAs(new PrivilegedExceptionAction<Object>() {
//      public Object run() throws IOException, InterruptedException {
//        final short MAX_BACKOFF = 5000;
//        closeConnection();
//        disposeSasl();
//        if (shouldAuthenticateOverKrb()) {
//          if (currRetries < maxRetries) {
//            if(LOG.isDebugEnabled()) {
//              LOG.debug("Exception encountered while connecting to "
//                  + "the server : " + ex);
//            }
//            // try re-login
//            if (UserGroupInformation.isLoginKeytabBased()) {
//              UserGroupInformation.getLoginUser().reloginFromKeytab();
//            } else {
//              UserGroupInformation.getLoginUser().reloginFromTicketCache();
//            }
//            // have granularity of milliseconds
//            //we are sleeping with the Connection lock held but since this
//            //connection instance is being used for connecting to the server
//            //in question, it is okay
//            Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));
//            return null;
//          } else {
//            String msg = "Couldn't setup connection for "
//                + UserGroupInformation.getLoginUser().getUserName() + " to "
//                + serverPrincipal;
//            LOG.warn(msg);
//            throw (IOException) new IOException(msg).initCause(ex);
//          }
//        } else {
//          LOG.warn("Exception encountered while connecting to "
//              + "the server : " + ex);
//        }
//        if (ex instanceof RemoteException)
//          throw (RemoteException) ex;
//        throw new IOException(ex);
//      }
//    });
//  }

  
  /** Connect to the server and set up the I/O streams. It then sends
   * a header to the server and starts
   * the connection thread that waits for responses.
   */
  synchronized void setupIOstreams() throws InterruptedException {
    if (socket != null || shouldCloseConnection.get()) {
      return;
    } 
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to "+server);
      }
      short numRetries = 0;
      final short MAX_RETRIES = 5;
      Random rand = null;
      while (true) {
        setupConnection();
        InputStream inStream = NetUtils.getInputStream(socket);
        OutputStream outStream = NetUtils.getOutputStream(socket);
        // FIXME
        writeConnectionHeader(outStream);
        if (useSasl) {
          final InputStream in2 = inStream;
          final OutputStream out2 = outStream;
          UserGroupInformation ticket = remoteId.getTicket();
          boolean continueSasl = false;
          /*boolean continueSasl = false;
          if (authMethod == AuthMethod.KERBEROS) {
            if (ticket.getRealUser() != null) {
              ticket = ticket.getRealUser();
            }
          }
          try {
            continueSasl = ticket
                .doAs(new PrivilegedExceptionAction<Boolean>() {
                  @Override
                  public Boolean run() throws IOException {
                    return setupSaslConnection(in2, out2);
                  }
                });
          } catch (Exception ex) {
            if (rand == null) {
              rand = new Random();
            }
            handleSaslConnectionFailure(numRetries++, MAX_RETRIES, ex, rand,
                ticket);
            continue;
          }
          if (continueSasl) {
            // Sasl connect is successful. Let's set up Sasl i/o streams.
            inStream = saslRpcClient.getInputStream(inStream);
            outStream = saslRpcClient.getOutputStream(outStream);
          } else {
            // fall back to simple auth because server told us so.
            authMethod = AuthMethod.SIMPLE;
            // remake the connectionContext             
            connectionContext = ProtoUtil.makeIpcConnectionContext(
                connectionContext.getProtocol(), 
                ProtoUtil.getUgi(connectionContext.getUserInfo()),
                authMethod);
            useSasl = false;
          }*/
        }
      
        if (doPing) {
          this.in = new DataInputStream(new BufferedInputStream(
              new PingInputStream(inStream)));
        } else {
          this.in = new DataInputStream(new BufferedInputStream(inStream));
        }
        this.out = new DataOutputStream(new BufferedOutputStream(outStream));
        // FIXME
        //writeConnectionContext();

        // update last activity time
        //touch();

        // start the receiver thread after the socket connection has been set
        // up
        start();
        return;
      }
    } catch (Throwable t) {
      if (t instanceof IOException) {
        markClosed((IOException)t);
      } else {
        markClosed(new IOException("Couldn't set up IO streams", t));
      }
      close();
    }
  }
  
  private void closeConnection() {
    if (socket == null) {
      return;
    }
    // close the current connection
    try {
      socket.close();
    } catch (IOException e) {
      LOG.warn("Not able to close a socket", e);
    }
    // set socket to null so that the next call to setupIOstreams
    // can start the process of connect all over again.
    socket = null;
  }

  /* Handle connection failures
   *
   * If the current number of retries is equal to the max number of retries,
   * stop retrying and throw the exception; Otherwise backoff 1 second and
   * try connecting again.
   *
   * This Method is only called from inside setupIOstreams(), which is
   * synchronized. Hence the sleep is synchronized; the locks will be retained.
   *
   * @param curRetries current number of retries
   * @param maxRetries max number of retries allowed
   * @param ioe failure reason
   * @throws IOException if max number of retries is reached
   */
  private void handleConnectionFailure(
      int curRetries, int maxRetries, IOException ioe) throws IOException {

    closeConnection();

    // throw the exception if the maximum number of retries is reached
    if (curRetries >= maxRetries) {
      throw ioe;
    }

    // otherwise back off and retry
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignored) {}
    
    LOG.info("Retrying connect to server: " + server + ". Already tried "
        + curRetries + " time(s); maxRetries=" + maxRetries);
  }

  private void handleConnectionFailure(int curRetries, IOException ioe
      ) throws IOException {
    closeConnection();

    final RetryAction action;
    try {
      action = connectionRetryPolicy.shouldRetry(ioe, curRetries, 0, true);
    } catch(Exception e) {
      throw e instanceof IOException? (IOException)e: new IOException(e);
    }
    if (action.action == RetryAction.RetryDecision.FAIL) {
      if (action.reason != null) {
        LOG.warn("Failed to connect to server: " + server + ": "
            + action.reason, ioe);
      }
      throw ioe;
    }

    try {
      Thread.sleep(action.delayMillis);
    } catch (InterruptedException e) {
      throw (IOException)new InterruptedIOException("Interrupted: action="
          + action + ", retry policy=" + connectionRetryPolicy).initCause(e);
    }
    LOG.info("Retrying connect to server: " + server + ". Already tried "
        + curRetries + " time(s); retry policy is " + connectionRetryPolicy);
  }

  /**
   * Write the connection header - this is sent when connection is established
   * +----------------------------------+
   * |  "hrpc" 4 bytes                  |      
   * +----------------------------------+
   * |  Version (1 bytes)               |      
   * +----------------------------------+
   * |  Authmethod (1 byte)             |      
   * +----------------------------------+
   * |  IpcSerializationType (1 byte)   |      
   * +----------------------------------+
   */
  private void writeConnectionHeader(OutputStream outStream)
      throws IOException {
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));
    // Write out the header, version and authentication method
    out.write(Server.HEADER.array());
    out.write(Server.CURRENT_VERSION);
    //authMethod.write(out);
    Server.IpcSerializationType.PROTOBUF.write(out);
    out.flush();
  }
  
  /* Write the connection context header for each connection
   * Out is not synchronized because only the first thread does this.
   */
/*  private void writeConnectionContext() throws IOException {
    // Write out the ConnectionHeader
    DataOutputBuffer buf = new DataOutputBuffer();
    connectionContext.writeTo(buf);
    
    // Write out the payload length
    int bufLen = buf.getLength();

    out.writeInt(bufLen);
    out.write(buf.getData(), 0, bufLen);
  }*/
  
  /* wait till someone signals us to start reading RPC response or
   * it is idle too long, it is marked as to be closed, 
   * or the client is marked as not running.
   * 
   * Return true if it is time to read a response; false otherwise.
   */
  private synchronized boolean waitForWork() {
    if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
      long timeout = maxIdleTime - (System.currentTimeMillis()-lastActivity.get());
      if (timeout>0) {
        try {
          wait(timeout);
        } catch (InterruptedException e) {}
      }
    }
    
    if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
      return true;
    } else if (shouldCloseConnection.get()) {
      return false;
    } else if (calls.isEmpty()) { // idle connection closed or stopped
      markClosed(null);
      return false;
    } else { // get stopped but there are still pending requests 
      markClosed((IOException)new IOException().initCause(
          new InterruptedException()));
      return false;
    }
  }

  public InetSocketAddress getRemoteAddress() {
    return server;
  }


  /* Send a ping to the server if the time elapsed 
   * since last I/O activity is equal to or greater than the ping interval
   */
  private synchronized void sendPing() throws IOException {
    long curTime = System.currentTimeMillis();
    if ( curTime - lastActivity.get() >= pingInterval) {
      lastActivity.set(curTime);
      synchronized (out) {
        out.writeInt(PING_CALL_ID);
        out.flush();
      }
    }
  }

  public void run() {
    /*if (LOG.isDebugEnabled())
      LOG.debug(getName() + ": starting, having connections " 
          + connections.size());*/

    try {
      while (waitForWork()) {//wait here for work - read or close connection
        receiveResponse();
      }
    } catch (Throwable t) {
      // This truly is unexpected, since we catch IOException in receiveResponse
      // -- this is only to be really sure that we don't leave a client hanging
      // forever.
      LOG.warn("Unexpected error reading responses on connection " + this, t);
      markClosed(new IOException("Error reading responses", t));
    }
    
    close();
    
    /*if (LOG.isDebugEnabled())
      LOG.debug(getName() + ": stopped, remaining connections "
          + connections.size());*/
  }

  /** Initiates a call by sending the parameter to the remote server.
   * Note: this is not called from the Connection thread, but by other
   * threads.
   */
  public void sendParam(Call call) {
    if (shouldCloseConnection.get()) {
      return;
    }

    DataOutputBuffer buffer=null;
    try {
      synchronized (this.out) {
        if (LOG.isDebugEnabled()){
          LOG.debug(getName() + " sending #" + call.id);
        }
        
        // Serializing the data to be written.
        // Format:
        // 0) Length of rest below (1 + 2)
        // 1) PayloadHeader  - is serialized Delimited hence contains length
        // 2) the Payload - the RpcRequest
        //
        buffer = new DataOutputBuffer();
        
        //Add the header
        RpcPayloadHeaderProto header = ProtoUtil.makeRpcPayloadHeader(
           call.rpcKind, RpcPayloadOperationProto.RPC_FINAL_PAYLOAD, call.id);
        
        header.writeDelimitedTo(buffer);
        call.rpcRequest.write(buffer);
        byte[] data = buffer.getData();
 
        int totalLength = buffer.getLength();
        out.writeInt(totalLength); // Total Length
        out.write(data, 0, totalLength);//PayloadHeader + RpcRequest
        out.flush();
      }
    } catch(IOException e) {
      markClosed(e);
    } finally {
      //the buffer is just an in-memory buffer, but it is still polite to
      // close early
      IOUtils.closeStream(buffer);
    }
  }  

  /* Receive a response.
   * Because only one receiver, so no synchronization on in.
   */
  private void receiveResponse() {
    if (shouldCloseConnection.get()) {
      return;
    }
    touch();
    
    try {
      RpcResponseHeaderProto response = 
          RpcResponseHeaderProto.parseDelimitedFrom(in);
      if (response == null) {
        throw new IOException("Response is null.");
      }

      int callId = response.getCallId();
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + " got value #" + callId);

      Call call = calls.get(callId);
      RpcStatusProto status = response.getStatus();
      if (status == RpcStatusProto.SUCCESS) {
        Writable value = ReflectionUtils.newInstance(valueClass, conf);
        value.readFields(in);                 // read value
        call.setRpcResponse(value);
        calls.remove(callId);
      } else if (status == RpcStatusProto.ERROR) {
        call.setException(new RemoteException(WritableUtils.readString(in),
                                              WritableUtils.readString(in)));
        calls.remove(callId);
      } else if (status == RpcStatusProto.FATAL) {
        // Close the connection
        markClosed(new RemoteException(WritableUtils.readString(in), 
                                       WritableUtils.readString(in)));
      }
    } catch (IOException e) {
      markClosed(e);
    }
  }
  
  private synchronized void markClosed(IOException e) {
    if (shouldCloseConnection.compareAndSet(false, true)) {
      closeException = e;
      notifyAll();
    }
  }
  
  /** Close the connection. */
  private synchronized void close() {
    if (!shouldCloseConnection.get()) {
      LOG.error("The connection is not in the closed state");
      return;
    }

    // release the resources
    // first thing to do;take the connection out of the connection list
    synchronized (client.getConnections()) {
      if (connections.get(remoteId) == this) {
        connections.remove(remoteId);
      }
    }

    // close the streams and therefore the socket
    IOUtils.closeStream(out);
    IOUtils.closeStream(in);
    disposeSasl();

    // clean up all calls
    if (closeException == null) {
      if (!calls.isEmpty()) {
        LOG.warn(
            "A connection is closed for no cause and calls are not empty");

        // clean up calls anyway
        closeException = new IOException("Unexpected closed connection");
        cleanupCalls();
      }
    } else {
      // log the info
      if (LOG.isDebugEnabled()) {
        LOG.debug("closing ipc connection to " + server + ": " +
            closeException.getMessage(),closeException);
      }

      // cleanup calls
      cleanupCalls();
    }
    if (LOG.isDebugEnabled())
      LOG.debug(getName() + ": closed");
  }
  
  /* Cleanup all calls and mark them as done */
  private void cleanupCalls() {
    Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
    while (itor.hasNext()) {
      Call c = itor.next().getValue(); 
      c.setException(closeException); // local exception
      itor.remove();         
    }
  }
}