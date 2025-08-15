package org.dcache.cells;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import java.util.function.BiConsumer;


/**
 * Abstract base class for MessageCallback implementations.
 *
 * Example usage:
 *
 *
 * <pre>
 *     // with Java8 CompletableFuture
 *      var message = new Message();
 *      var callback = new MyMessageCallback();
 *      cellStub.send(cellPath, message, message.getClass(), timeout)
 *                 .whenCompleteAsync(callback, executor);
 * </pre>
 *
 * or
 *
 *  <pre>
 *      // with Guava ListenableFuture
 *      var message = new Message();
 *      var callback = new MyMessageCallback();
 *      var listenableFuture = cellStub.send(cellPath, message, timeout);
 *      CellStub.addCallback(listenableFuture, callback, executor);
 *  </pre>
 *
 */
public abstract class AbstractMessageCallback<T extends Message> implements MessageCallback<T>, BiConsumer<T, Throwable> {

    private T _reply;

    public abstract void success(T message);

    public T getReply() {
        return _reply;
    }

    @Override
    public void setReply(T message) {
        _reply = message;
    }

    @Override
    public void success() {
        success(getReply());
    }

    @Override
    public void noroute(CellPath path) {
        failure(CacheException.TIMEOUT, "No route to " + path);
    }

    @Override
    public void timeout(String message) {
        failure(CacheException.TIMEOUT, message);
    }

    @Override
    public void accept(T t, Throwable e) {
        if (e == null) {
            setReply(t);
            if (t.getReturnCode() == 0) {
                success(t);
            } else {
                failure(t.getReturnCode(), t.getErrorObject());
            }
        } else {
            var cause = e.getCause();
            if (cause instanceof TimeoutCacheException) {
                timeout(cause.getMessage());
            } else if (cause instanceof CacheException) {
                CacheException cacheException = (CacheException) cause;
                failure(cacheException.getRc(), cacheException.getMessage());
            } else if (cause instanceof NoRouteToCellException) {
                noroute(((NoRouteToCellException) cause).getDestinationPath());
            } else {
                failure(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, cause);
            }
        }
    }
}
