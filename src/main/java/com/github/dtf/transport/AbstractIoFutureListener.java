package com.github.dtf.transport;

/**
 * 
 * Convenient base implementation for {@link IoFutureListener}. if something wrong happen the exception is rethrown,
 * which will produce an exception caught event for the session
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public abstract class AbstractIoFutureListener<V> implements IoFutureListener<V> {

    /**
     * {@inheritDoc}
     */
    public void exception(Throwable t) {
        throw new MinaRuntimeException(t);
    }
}
