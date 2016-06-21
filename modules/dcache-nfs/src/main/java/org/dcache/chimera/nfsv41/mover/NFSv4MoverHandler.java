package org.dcache.chimera.nfsv41.mover;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.util.PnfsId;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NFSv4Defaults;
import org.dcache.nfs.v4.NFSv4OperationFactory;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.OperationBIND_CONN_TO_SESSION;
import org.dcache.nfs.v4.OperationCREATE_SESSION;
import org.dcache.nfs.v4.OperationDESTROY_CLIENTID;
import org.dcache.nfs.v4.OperationDESTROY_SESSION;
import org.dcache.nfs.v4.OperationEXCHANGE_ID;
import org.dcache.nfs.v4.OperationGETATTR;
import org.dcache.nfs.v4.OperationILLEGAL;
import org.dcache.nfs.v4.OperationPUTFH;
import org.dcache.nfs.v4.OperationPUTROOTFH;
import org.dcache.nfs.v4.OperationRECLAIM_COMPLETE;
import org.dcache.nfs.v4.OperationSEQUENCE;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.v4.xdr.stateid4;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.Stat.Type;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.pool.movers.IoMode;
import org.dcache.util.PortRange;
import org.dcache.util.Bytes;
import org.dcache.vehicles.DoorValidateMoverMessage;
import org.dcache.xdr.IpProtocolType;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcSvcBuilder;
import org.dcache.xdr.RpcLoginService;
import org.dcache.xdr.gss.GssSessionManager;
import org.dcache.xdr.OncRpcException;

/**
 *
 * Pool embedded NFSv4.1 data server
 *
 */
public class NFSv4MoverHandler {

    private static final Logger _log = LoggerFactory.getLogger(NFSv4MoverHandler.class.getName());

    private final VirtualFileSystem _fs = new VirtualFileSystem() {

        @Override
        public int access(Inode inode, int mode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void commit(Inode inode, long offset, int count) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode create(Inode parent, Type type, String path, Subject subject, int mode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public FsStat getFsStat() throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode getRootInode() throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode lookup(Inode parent, String path) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public List<DirectoryEntry> list(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode parentOf(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String readlink(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void remove(Inode parent, String path) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Stat getattr(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setattr(Inode inode, Stat stat) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public nfsace4[] getAcl(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean hasIOLayout(Inode inode) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public AclCheckable getAclCheckable() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public NfsIdMapping getIdMapper() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    };

    /**
     * RPC service
     */
    private final OncRpcSvc _rpcService;

    private final Map<stateid4, NfsMover> _activeIO = new ConcurrentHashMap<>();
    private final NFSv4OperationFactory _operationFactory = new EDSNFSv4OperationFactory();

    /**
     * for The nfs commit operation comes without a stateid. We need a different way to
     * wind corresponding mover. The assumption, is that nor now we have only one writer and,
     * as a result, pnfsid will point only to a single mover.
     */
    private final Map<PnfsId, NfsMover> _activeWrites = new ConcurrentHashMap<>();
    private final NFSServerV41 _embededDS;

    /**
     * A CellStub for communication with doors.
     */
    private final CellStub _door;

    /**
     * A time window in millis during which we accept idle movers.
     */
    private static final long IDLE_PERIOD = TimeUnit.SECONDS.toMillis(NFSv4Defaults.NFS4_LEASE_TIME * 5);

    private final ScheduledExecutorService _cleanerExecutor;
    private final long _bootVerifier;

    public NFSv4MoverHandler(PortRange portRange, boolean withGss, String serverId, CellStub door, long bootVerifier)
            throws IOException , GSSException, OncRpcException {

        _embededDS = new NFSServerV41(_operationFactory, null, _fs, null);
        OncRpcSvcBuilder oncRpcSvcBuilder = new OncRpcSvcBuilder()
                .withMinPort(portRange.getLower())
                .withMaxPort(portRange.getUpper())
                .withTCP()
                .withoutAutoPublish()
                .withRpcService(new OncRpcProgram(nfs4_prot.NFS4_PROGRAM, nfs4_prot.NFS_V4), _embededDS)
                .withSameThreadIoStrategy();

        if (withGss) {
            RpcLoginService rpcLoginService = (t, gss) -> Subjects.NOBODY;
            GssSessionManager gss = new GssSessionManager(rpcLoginService);
            oncRpcSvcBuilder.withGssSessionManager(gss);
        }

        _rpcService = oncRpcSvcBuilder.build();
        _rpcService.start();
        _door = door;
        _bootVerifier = bootVerifier;
        _cleanerExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setNameFormat("NFS mover validationthread")
                .build()
        );
        _cleanerExecutor.scheduleAtFixedRate(new MoverValidator(), IDLE_PERIOD, IDLE_PERIOD, TimeUnit.MILLISECONDS);
    }

    /**
     * Add mover into list of allowed transfers.
     *
     * @param mover
     */
    public void add(NfsMover mover) {
        _log.debug("registering new mover {}", mover);
        _activeIO.put(mover.getStateId(), mover );
        if (mover.getIoMode() == IoMode.WRITE) {
            _activeWrites.put(mover.getFileAttributes().getPnfsId(), mover);
        }
    }

    /**
     * Removes mover from the list of allowed transfers.
     *
     * @param mover
     */
    public void remove(NfsMover mover) {
        _log.debug("un-removing io handler for stateid {}", mover);
        _activeIO.remove(mover.getStateId());
        if (mover.getIoMode() == IoMode.WRITE) {
            _activeWrites.remove(mover.getFileAttributes().getPnfsId());
        }
    }

    private class EDSNFSv4OperationFactory implements NFSv4OperationFactory {

        @Override
        public AbstractNFSv4Operation getOperation(nfs_argop4 op) {

            switch (op.argop) {
                case nfs_opnum4.OP_COMMIT:
                    return new EDSOperationCOMMIT(op, _activeWrites);
                case nfs_opnum4.OP_GETATTR:
                    return new OperationGETATTR(op);
                case nfs_opnum4.OP_PUTFH:
                    return new OperationPUTFH(op);
                case nfs_opnum4.OP_PUTROOTFH:
                    return new OperationPUTROOTFH(op);
                case nfs_opnum4.OP_READ:
                    return new EDSOperationREAD(op, NFSv4MoverHandler.this);
                case nfs_opnum4.OP_WRITE:
                    return new EDSOperationWRITE(op, NFSv4MoverHandler.this);
                case nfs_opnum4.OP_EXCHANGE_ID:
                    return new OperationEXCHANGE_ID(op, nfs4_prot.EXCHGID4_FLAG_USE_PNFS_DS);
                case nfs_opnum4.OP_CREATE_SESSION:
                    return new OperationCREATE_SESSION(op);
                case nfs_opnum4.OP_DESTROY_SESSION:
                    return new OperationDESTROY_SESSION(op);
                case nfs_opnum4.OP_SEQUENCE:
                    return new OperationSEQUENCE(op);
                case nfs_opnum4.OP_RECLAIM_COMPLETE:
                    return new OperationRECLAIM_COMPLETE(op);
                case nfs_opnum4.OP_BIND_CONN_TO_SESSION:
                    return new OperationBIND_CONN_TO_SESSION(op);
                case nfs_opnum4.OP_DESTROY_CLIENTID:
                    return new OperationDESTROY_CLIENTID(op);
                case nfs_opnum4.OP_ILLEGAL:
            }

            return new OperationILLEGAL(op);
        }
    }

    NfsMover getOrCreateMover(InetSocketAddress remoteAddress, stateid4 stateid, byte[] fh) throws ChimeraNFSException {
        NfsMover mover = _activeIO.get(stateid);
        if (mover == null) {
            /*
             * a mover for the same file and the same client can be re-used.
             */
            /**
             * FIXME: this is verry fragile, as we assume that stateid
             * structure is known and contains clientid.
             */
            long clientId = Bytes.getLong(stateid.other, 0);
            return _activeIO.values().stream()
                    .filter(m -> Bytes.getLong(m.getStateId().other, 0) == clientId)
                    .filter(m -> Arrays.equals(fh, m.getNfsFilehandle()))
                    .findFirst()
                    .orElse(null);
        }
        return mover;
    }

    /**
     * Get TCP port number used by handler.
     * @return port number.
     */
    public InetSocketAddress getLocalAddress(){
        return _rpcService.getInetSocketAddress(IpProtocolType.TCP);
    }

    public void shutdown() throws IOException {
        _rpcService.stop();
        _cleanerExecutor.shutdown();
    }

    NFSServerV41 getNFSServer() {
        return _embededDS;
    }

    class MoverValidator implements Runnable {

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            _activeIO.values()
                    .stream()
                    .filter(mover -> (!mover.hasSession() && (now - mover.getLastTransferred() > IDLE_PERIOD)))
                    .forEach(mover -> {
                        _log.debug("Verifing inactive mover {}", mover);
                        final org.dcache.chimera.nfs.v4.xdr.stateid4 legacyStateId = mover.getProtocolInfo().stateId();
                        CellStub.addCallback(_door.send(mover.getPathToDoor(),
                                new DoorValidateMoverMessage<>(-1, mover.getFileAttributes().getPnfsId(), _bootVerifier, legacyStateId)),
                                new NfsMoverValidationCallback(mover),
                                    _cleanerExecutor);
                    });
        }

    }
}
