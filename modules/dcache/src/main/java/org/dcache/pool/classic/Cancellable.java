package org.dcache.pool.classic;

import com.google.common.base.Optional;
import java.io.Serializable;

/**
 * An interface for object which allow to interrupt an operation in progress.
 */
public interface Cancellable
{
    void cancel();
    Optional<? extends Serializable> getAttachment();
}
