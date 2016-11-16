package org.dcache.chimera.nfsv41.door;

import org.glassfish.grizzly.memory.DefaultMemoryManagerFactory;
import org.glassfish.grizzly.memory.HeapMemoryManager;
import org.glassfish.grizzly.memory.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tigran
 */
public class ExtendedMemoryManagerFactory implements DefaultMemoryManagerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ExtendedMemoryManagerFactory.class);
    private static final int MAX_ALLOC_CACHE_SIZE = 128*1024;

    @Override
    public MemoryManager createMemoryManager() {
        LOG.info("Using ExtendedMemoryManagerFactory with allocation size = {}", MAX_ALLOC_CACHE_SIZE);
        return new HeapMemoryManager(MAX_ALLOC_CACHE_SIZE);
    }

}
