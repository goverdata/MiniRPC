package com.github.dtf.transport;

import java.net.SocketAddress;

/**
 * 
 * A network server bound to a local address.
 * <p>
 * Will crate a new {@link IoSession} for each new incoming connection.
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 * 
 */
public interface IoServer {
    /**
     * Returns the local addresses which are bound currently.
     */
    SocketAddress getBoundAddress();

    /**
     * Binds to the specified local addresses and start to accept incoming connections.
     */
    void bind(SocketAddress localAddress);

    /**
     * Binds the server to the specified port.
     * 
     * @param port the local TCP port to bind.
     */
    void bind(int port);

    /**
     * Unbinds from the local addresses that this service is bound to and stops to accept incoming connections. This
     * method returns silently if no local address is bound yet.
     * 
     */
    void unbind();
}
