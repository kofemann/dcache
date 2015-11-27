package org.dcache.pool.classic;

import diskCacheV111.util.CacheException;
import org.dcache.pool.movers.Mover;

/**
 *
 */
@FunctionalInterface
public interface MoverSupplier {

    Mover<?> createMover() throws CacheException;
}
