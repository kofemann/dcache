package org.dcache.chimera.nfsv41.door.proxy;

import java.io.IOException;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.OperationOPEN;
import org.dcache.nfs.v4.xdr.layoutiomode4;
import org.dcache.nfs.v4.xdr.layouttype4;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.xdr.opentype4;
import org.dcache.nfs.vfs.Inode;
import org.dcache.xdr.OncRpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tigran
 */
public class ProxyOpen extends OperationOPEN {

    private static final Logger _log = LoggerFactory.getLogger(ProxyOpen.class);
    public ProxyOpen(nfs_argop4 args) {
        super(args);
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {
        super.process(context, result);

        // on success start a mover if it's write
        if (_args.opopen.openhow.opentype == opentype4.OPEN4_CREATE) {
            try {
                Inode inode = context.currentInode();
                context.getDeviceManager().layoutGet(context, inode,
                layouttype4.LAYOUT4_NFSV4_1_FILES,
                layoutiomode4.LAYOUTIOMODE4_RW , result.opopen.resok4.stateid);
            } catch (IOException e) {
                _log.warn("failed to pre-start mover: {} ", e.getMessage() );
            }
        }
    }

}
