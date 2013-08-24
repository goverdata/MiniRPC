package com.github.dtf.transport;

import org.apache.mina.service.executor.IoHandlerExecutor;

/**
 * Handle all the I/O events generated by a {@link IoService}.
 * <p>
 * You should handle your business logic in an IoHandler implementation.
 * <p>
 * The {@link IoFilter} is dedicated to message transformation, but the IoHandler is mean to be the core of your
 * business logic.
 * <p>
 * If you need to implement blocking code in your {@link IoHandler}, then you need to add a {@link IoHandlerExecutor} in
 * the enclosing {@link IoService}.
 */
public interface IoHandler {

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
    void messageReceived(IoSession session, Object message);

    /**
     * Invoked when a high level message was written to the low level O/S buffer.
     * 
     * @param session {@link IoSession} associated with the invocation
     * @param message the incoming message to process
     */
    void messageSent(IoSession session, Object message);

    /**
     * Invoked when a new service is activated by an {@link IoService}.
     * 
     * @param service the {@link IoService}
     */
    void serviceActivated(IoService service);

    /**
     * Invoked when a service is inactivated by an {@link IoService}.
     * 
     * @param service the {@link IoService}
     */
    void serviceInactivated(IoService service);

    /**
     * Invoked when any runtime exception is thrown during session processing (filters, unexpected error, etc..).
     * 
     * @param session the session related to the exception
     * @param cause the caught exception
     */
    void exceptionCaught(IoSession session, Exception cause);
}
