package org.dcache.chimera.nfsv41.door;

import diskCacheV111.vehicles.DoorRequestInfoMessage;
import java.io.IOException;
import org.dcache.cells.CellStub;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.NFSv4OperationFactory;
import org.dcache.nfs.v4.OperationREMOVE;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.vfs.Inode;

/**
 *
 * @author tigran
 */
public class BillingAwareOperationFactory extends MDSOperationFactory {

    private final JdbcFs _jdbcFs;
    private final CellStub _billing;
    private final NFSv4OperationFactory _opFactory;
    private final String _cellName;

    public BillingAwareOperationFactory(JdbcFs jdbcFs, CellStub billing, String cellName, NFSv4OperationFactory opFactory) {
        _jdbcFs = jdbcFs;
        _billing = billing;
        _opFactory = opFactory;
        _cellName = cellName;
    }

    @Override
    public AbstractNFSv4Operation getOperation(nfs_argop4 op) {

        if (op.argop == nfs_opnum4.OP_REMOVE) {
            return new OpRemove(op);
        } else {
            return _opFactory.getOperation(op);
        }
    }

    private class OpRemove extends OperationREMOVE {

        public OpRemove(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException {

            Inode parent = context.currentInode();
            FsInode cParentInode = _jdbcFs.inodeFromBytes(parent.getFileId());

            super.process(context, result);
            DoorRequestInfoMessage infoRemove  = new DoorRequestInfoMessage(_cellName, "remove");
            infoRemove.setSubject(context.getSubject());
            infoRemove.setFileSize(0);
            infoRemove.setBillingPath("inode:" + cParentInode);
            infoRemove.setClient(context.getRemoteSocketAddress().getAddress().getHostAddress());

            _billing.notify(infoRemove);
        }

    }
}
