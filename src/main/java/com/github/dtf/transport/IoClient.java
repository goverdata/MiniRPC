package com.github.dtf.transport;

import java.net.SocketAddress;

/**
 * Connects to several end-points, communicates with the server, and fires events to
 * {@link org.apache.mina.service.IoHandler}s.
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public interface IoClient {

    /**
     * Connects to the specified remote address.
     * 
     * @param remoteAddress Remote {@link SocketAddress} to connect
     * @return the {@link IoFuture} instance which is completed when the connection attempt initiated by this call
     *         succeeds or fails.
     */
    IoFuture<IoSession> connect(SocketAddress remoteAddress);
}
