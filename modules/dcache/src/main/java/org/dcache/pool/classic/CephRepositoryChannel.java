package org.dcache.pool.classic;

import com.ceph.rados.exceptions.RadosException;
import com.ceph.rbd.Rbd;
import com.ceph.rbd.RbdException;
import com.ceph.rbd.RbdImage;
import com.ceph.rbd.jna.RbdImageInfo;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.dcache.pool.repository.RepositoryChannel;

public class CephRepositoryChannel implements RepositoryChannel {

    private final String name;
    private final String mode;
    private final Rbd rbd;
    private RbdImage rbdImage;
    private final boolean rdOnly;

    private long offset = 0;

    public CephRepositoryChannel(Rbd rbd, String name, String mode) throws RadosException, RbdException {
        this.name = name;
        this.mode = mode;
        this.rbd = rbd;
        switch (mode) {
            case "rw":
            case "rwd":
            case "rws":
                rbd.create(name, 0);
                rbdImage = rbd.open(name);
                rdOnly = false;
                break;
            case "r":
                rbdImage = rbd.openReadOnly(name);
                rdOnly = true;
                break;

            default:
                throw new IllegalArgumentException("Illegal mode: " + mode);
        }
    }

    @Override
    public synchronized long position() throws IOException {
        return offset;
    }

    @Override
    public synchronized RepositoryChannel position(long position) throws IOException {
        try {
            final long size = size();
            if (rdOnly) {
                offset = Math.min(size, position);
            } else {
                offset = position;
                if (offset > size) {
                    rbdImage.resize(offset);
                }
            }
        } catch (RbdException e) {
            throw new IOException(e);
        }
        return this;
    }

    @Override
    public synchronized long size() throws IOException {
        try {
            RbdImageInfo info = rbdImage.stat();
            return info.size;
        } catch (RbdException e) {
            throw new IOException(e);
        }
    }

    @Override
    public synchronized int write(ByteBuffer buffer, long position) throws IOException {
        try {
            if (position + buffer.remaining() > size()) {
                rbdImage.resize(position + buffer.remaining());
            }

            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            rbdImage.write(data, position, data.length);
            return data.length;
        } catch (RbdException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        byte[] data = new byte[buffer.remaining()];
        int n = rbdImage.read(position, data, data.length);
        if (n > 0) {
            buffer.put(data, 0, n);
        }
        return n;
    }

    @Override
    public RepositoryChannel truncate(long size) throws IOException {
        try {
            rbdImage.resize(size);
        } catch (RbdException e) {
            throw new IOException(e);
        }
        return this;
    }

    @Override
    public void sync() throws SyncFailedException, IOException {
        // NOP
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        byte[] data = new byte[src.remaining()];
        try {
        rbdImage.write(data);
        } catch (RbdException e) {
            throw new IOException(e.getMessage(), e);
        }
        return data.length;
    }

    @Override
    public synchronized boolean isOpen() {
        return rbdImage == null;
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            rbd.close(rbdImage);
            rbdImage = null;
        } catch (RbdException e) {
            throw new IOException(e);
        }
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException {
        byte[] data = new byte[dst.remaining()];
        int n = rbdImage.read(offset, data, data.length);
        if (n > 0) {
            dst.put(data, 0, n);
            offset += n;
        }
        return n;
    }

}
