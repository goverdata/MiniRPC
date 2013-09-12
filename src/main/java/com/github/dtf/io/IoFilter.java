
package com.github.dtf.io;

import org.apache.mina.filterchain.ReadFilterChainController;
import org.apache.mina.filterchain.WriteFilterChainController;
import org.apache.mina.session.WriteRequest;

/**
 * Filter are interceptors/processors for incoming data received/sent.
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public interface IoFilter {

    /**
     * Invoked when a connection has been opened.
     * 
     * @param session {@link IoSession} associated with the invocation
     */
    void sessionOpened(IoSession session);

    /**
     * Invoked when a connection is closed.
     * 
     * @param session {@link IoSession} associated with the invocation
     */
    void sessionClosed(IoSession session);

    /**
     * Invoked with the related {@link IdleStatus} when a connection becomes idle.
     * 
     * @param session {@link IoSession} associated with the invocation
     */
    void sessionIdle(IoSession session, IdleStatus status);

    /**
     * Invoked when a message is received.
     * 
     * @param session {@link IoSession} associated with the invocation
     * @param message the incoming message to process
     */
    void messageReceived(IoSession session, Object message, ReadFilterChainController controller);

    /**
     * Invoked when a message is under writing. The filter is supposed to apply the needed transformation.
     * 
     * @param session {@link IoSession} associated with the invocation
     * @param message the message to process before writing
     */
    void messageWriting(IoSession session, WriteRequest message, WriteFilterChainController controller);

    /**
     * Invoked when a high level message was written to the low level O/S buffer.
     * 
     * @param session {@link IoSession} associated with the invocation
     * @param message the incoming message to process
     */
    void messageSent(IoSession session, Object message);
}