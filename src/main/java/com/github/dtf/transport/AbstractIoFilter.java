package com.github.dtf.transport;

import org.apache.mina.filterchain.ReadFilterChainController;
import org.apache.mina.filterchain.WriteFilterChainController;
import org.apache.mina.session.WriteRequest;

/**
 * A convenient {@link IoFilter} implementation to be sub-classed for easier IoFilter implementation.
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public abstract class AbstractIoFilter implements IoFilter {

    /**
     * {@inheritDoc}
     */
    @Override
    public void sessionOpened(final IoSession session) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sessionClosed(final IoSession session) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sessionIdle(final IoSession session, final IdleStatus status) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageReceived(final IoSession session, final Object message,
            final ReadFilterChainController controller) {
        controller.callReadNextFilter(message);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageWriting(IoSession session, WriteRequest message, WriteFilterChainController controller) {
        controller.callWriteNextFilter(message);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageSent(final IoSession session, final Object message) {
    }
}