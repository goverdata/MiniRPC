package com.github.dtf.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenient {@link IoHandler} implementation to be sub-classed for easier {@link IoHandler} implementation.
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public abstract class AbstractIoHandler implements IoHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractIoHandler.class);

    /**
     * {@inheritDoc}
     */
    public void sessionOpened(final IoSession session) {
    }

    /**
     * {@inheritDoc}
     */
    public void sessionClosed(final IoSession session) {
    }

    /**
     * {@inheritDoc}
     */
    public void sessionIdle(final IoSession session, final IdleStatus status) {
    }

    /**
     * {@inheritDoc}
     */
    
    public void messageReceived(final IoSession session, final Object message) {
    }

    /**
     * {@inheritDoc}
     */
    public void messageSent(final IoSession session, final Object message) {
    }

    /**
     * {@inheritDoc}
     */
    
    public void serviceActivated(final IoService service) {
    }

    /**
     * {@inheritDoc}
     */
    
    public void serviceInactivated(final IoService service) {
    }

    /**
     * {@inheritDoc}
     */
    
    public void exceptionCaught(final IoSession session, final Exception cause) {
        LOG.error("Unexpected exception, we close the session : ", cause);
        session.close(true);
    }
}
