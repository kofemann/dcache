package org.dcache.chimera.nfsv41.door.proxy;

import java.io.IOException;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.Inode;

public interface ProxyIoFactory {

    ProxyIoAdapter getOrCreateProxy(final Inode inode, final stateid4 stateid,
            final CompoundContext context, final boolean isWrite) throws IOException;

    ProxyIoAdapter createIoAdapter(Inode inode, stateid4 stateid,
            CompoundContext context, boolean isWrite) throws IOException;

    void shutdownAdapter(stateid4 stateid);

    Iterable<ProxyIoAdapter> getAdapters();

    void shutdown();
}
