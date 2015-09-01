package org.dcache.chimera.nfsv41.door;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.chimera.FsInode;
import org.dcache.chimera.JdbcFs;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.OperationREMOVE;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.nfs.v4.AttributeMap;
import org.dcache.nfs.v4.OperationCREATE;
import org.dcache.nfs.v4.OperationOPEN;
import org.dcache.nfs.v4.OperationGETATTR;
import org.dcache.nfs.v4.OperationRENAME;
import org.dcache.nfs.v4.OperationSETATTR;
import org.dcache.nfs.v4.xdr.opentype4;
import org.dcache.util.NetLoggerBuilder;
import org.dcache.nfs.vfs.Inode;
import org.dcache.xdr.OncRpcException;

import static java.nio.charset.StandardCharsets.UTF_8;
/**
 *
 */
public class BillingAwareOperationFactory extends MDSOperationFactory {

    private static final Logger ACCESS_LOGGER = LoggerFactory.getLogger("org.dcache.access.ftp");

    private static final String[] TYPES = {
        null,
        "REG",
        "DIR",
        "BLK",
        "CHR",
        "LNK",
        "SOCK",
        "FIFO",
        "ATTRDIR",
        "NAMEDATTR"
    };

    private final JdbcFs _jdbcFs;

    public BillingAwareOperationFactory(JdbcFs fs) {
        _jdbcFs = fs;
    }

    @Override
    public AbstractNFSv4Operation getOperation(nfs_argop4 op) {
        switch (op.argop) {
            case nfs_opnum4.OP_REMOVE:
                return new OpRemove(op);
            case nfs_opnum4.OP_CREATE:
                return new OpCreate(op);
            case nfs_opnum4.OP_RENAME:
                return new OpRename(op);
            case nfs_opnum4.OP_OPEN:
                return new OpOpen(op);
            case nfs_opnum4.OP_SETATTR:
                return new OpSetattr(op);
            default:
                return super.getOperation(op);
        }
    }

    private class OpSetattr extends OperationSETATTR {

        public OpSetattr(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode parent = context.currentInode();
            FsInode cInode = _jdbcFs.inodeFromBytes(parent.getFileId());

            super.process(context, result);

            NetLoggerBuilder nl = new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.setattr")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER)
                    .add("subject", context.getSubject())
                    .add("host.remote", context.getRemoteSocketAddress().getAddress().getHostAddress())
                    .add("obj.id", cInode)
                    .add("obj.path", _jdbcFs.inode2path(cInode))
                    ;

            AttributeMap attributeMap = new AttributeMap(_args.opsetattr.obj_attributes);
            for(int attr: result.opsetattr.attrsset) {
                nl.add("attr." + OperationGETATTR.attrMask2String(attr).trim(), attributeMap.get(attr).get());
            }
            nl.log();
        }
    }

    private class OpRename extends OperationRENAME {

        public OpRename(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode dst = context.currentInode();
            Inode src = context.savedInode();

            FsInode cDestParentInode = _jdbcFs.inodeFromBytes(dst.getFileId());
            FsInode cSrcParentInode = _jdbcFs.inodeFromBytes(src.getFileId());

            super.process(context, result);

                new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.rename")
                        .omitNullValues()
                        .onLogger(ACCESS_LOGGER)
                        .add("subject", context.getSubject())
                        .add("host.remote", context.getRemoteSocketAddress().getAddress().getHostAddress())
                        .add("old.path", _jdbcFs.inode2path(cSrcParentInode) + "/" + new String(_args.oprename.oldname.value, UTF_8))
                        .add("new.path", _jdbcFs.inode2path(cDestParentInode) + "/" + new String(_args.oprename.newname.value, UTF_8))
                        .log();
        }
    }

    private class OpOpen extends OperationOPEN {

        public OpOpen(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode parent = context.currentInode();
            FsInode cParentInode = _jdbcFs.inodeFromBytes(parent.getFileId());

            super.process(context, result);

            if (_args.opopen.openhow.opentype == opentype4.OPEN4_CREATE) {

                Inode inode = context.currentInode();
                FsInode cInode = _jdbcFs.inodeFromBytes(inode.getFileId());

                new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.create")
                        .omitNullValues()
                        .onLogger(ACCESS_LOGGER)
                        .add("subject", context.getSubject())
                        .add("host.remote", context.getRemoteSocketAddress().getAddress().getHostAddress())
                        .add("obj.path", _jdbcFs.inode2path(cParentInode) + "/" + new String(_args.opopen.claim.file.value, UTF_8))
                        .add("obj.type", TYPES[1])
                        .add("obj.id", cInode)
                        .log();
            }
        }
    }

    private class OpCreate extends OperationCREATE {

        public OpCreate(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode parent = context.currentInode();
            FsInode cParentInode = _jdbcFs.inodeFromBytes(parent.getFileId());

            super.process(context, result);

            Inode inode = context.currentInode();
            FsInode cInode = _jdbcFs.inodeFromBytes(inode.getFileId());

            new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.create")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER)
                    .add("subject", context.getSubject())
                    .add("host.remote", context.getRemoteSocketAddress().getAddress().getHostAddress())
                    .add("obj.name", _jdbcFs.inode2path(cParentInode) + "/" + new String(_args.opcreate.objname.value, UTF_8))
                    .add("obj.type", TYPES[_args.opcreate.objtype.type])
                    .add("obj.id", cInode)
                    .log();
        }
    }

    private class OpRemove extends OperationREMOVE {

        public OpRemove(nfs_argop4 args) {
            super(args);
        }

        @Override
        public void process(CompoundContext context, nfs_resop4 result) throws ChimeraNFSException, IOException, OncRpcException {

            Inode parent = context.currentInode();
            FsInode cParentInode = _jdbcFs.inodeFromBytes(parent.getFileId());

            String name = new String(_args.opremove.target.value, UTF_8);
            FsInode cInode = _jdbcFs.inodeOf(cParentInode, name);

            super.process(context, result);

            new NetLoggerBuilder(NetLoggerBuilder.Level.INFO, "org.dcache.nfs.remove")
                    .omitNullValues()
                    .onLogger(ACCESS_LOGGER)
                    .add("subject", context.getSubject())
                    .add("host.remote", context.getRemoteSocketAddress().getAddress().getHostAddress())
                    .add("obj.path", _jdbcFs.inode2path(cParentInode) + "/" + name)
                    .add("obj.id", cInode)
                    .log();
        }
    }
}
