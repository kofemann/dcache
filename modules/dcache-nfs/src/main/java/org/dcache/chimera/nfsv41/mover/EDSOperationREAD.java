package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AbstractNFSv4Operation;
import org.dcache.nfs.v4.CompoundContext;
import org.dcache.nfs.v4.NFSv4Defaults;
import org.dcache.nfs.v4.xdr.READ4res;
import org.dcache.nfs.v4.xdr.READ4resok;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfs_resop4;
import org.dcache.oncrpc4j.grizzly.FileChannelChunk;
import org.dcache.oncrpc4j.rpc.IoStrategy;
import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.xdr.Xdr;
import org.dcache.oncrpc4j.xdr.XdrEncodingStream;
import org.dcache.pool.repository.RepositoryChannel;
import org.glassfish.grizzly.FileChunk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EDSOperationREAD extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(EDSOperationREAD.class.getName());

    // Bind a direct buffer to each thread.
    private static final ThreadLocal<ByteBuffer> BUFFERS = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect((int) NFSv4Defaults.NFS4_MAXIOBUFFERSIZE);
        }
    };

    private final NfsTransferService nfsTransferService;

    public EDSOperationREAD(nfs_argop4 args, NfsTransferService nfsTransferService) {
        super(args, nfs_opnum4.OP_READ);
        this.nfsTransferService = nfsTransferService;
    }

    @Override
    public void process(CompoundContext context, nfs_resop4 result) {
        final READ4res res = result.opread;

        try {

            long offset = _args.opread.offset.value;
            int count = _args.opread.count.value;

            NfsMover mover = nfsTransferService.getMoverByStateId(context, _args.opread.stateid);
            if (mover == null) {
                res.status = nfsstat.NFSERR_BAD_STATEID;
                _log.debug("No mover associated with given stateid: ", _args.opread.stateid);
                return;
            }

            RepositoryChannel fc = mover.getMoverChannel();
            count = (int)Math.min((long)count, fc.size() - offset);

            res.status = nfsstat.NFS_OK;

            var r1 = new ShallowREAD4resok();
            r1.chunk = new RpositoryFileChunk(fc, offset, count);


            res.resok4 = r1;
            if (offset + count == fc.size()) {
                res.resok4.eof = true;
            }

            _log.debug("MOVER: {}@{} read, {} requested.", count, offset,
                  _args.opread.count.value);

        } catch (IOException ioe) {
            _log.error("DSREAD: ", ioe);
            res.status = nfsstat.NFSERR_IO;
        } catch (Exception e) {
            _log.error("DSREAD: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }
    }

    // version of READ4resok that uses shallow encoding to avoid extra copy
    public static class ShallowREAD4resok extends READ4resok {

        FileChunk chunk;

        public ShallowREAD4resok() {
        }

        public void xdrEncode(XdrEncodingStream xdr)
              throws OncRpcException, IOException {
            xdr.xdrEncodeBoolean(eof);
            ((Xdr) xdr).xdrEncodeFileChunk(chunk);
        }
    }


    private static class RpositoryFileChunk implements FileChunk {

        RepositoryChannel fc;
        long position;
        int len;

        public RpositoryFileChunk(RepositoryChannel fc, long offset, int count) {
            this.fc = fc;
            this.position = offset;
            this.len = count;
        }

        @Override
        public long writeTo(WritableByteChannel writableByteChannel) throws IOException {
            long n = fc.transferTo(position, len, writableByteChannel);

            if (n > 0) {
                len -= n;
            }

            return n;
        }

        @Override
        public boolean hasRemaining() {
            return len > 0;
        }

        @Override
        public int remaining() {
            return len;
        }

        @Override
        public boolean release() {
            return true;
        }

        @Override
        public boolean isExternal() {
            return true;
        }
    }

}
