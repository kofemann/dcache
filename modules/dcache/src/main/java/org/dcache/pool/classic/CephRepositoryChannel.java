package org.dcache.pool.classic;

import org.dcache.rados4j.RadosException;
import org.dcache.rados4j.Rbd;
import org.dcache.rados4j.RbdImage;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.dcache.pool.repository.RepositoryChannel;

public class CephRepositoryChannel implements RepositoryChannel {

    private final Rbd rbd;
    private RbdImage rbdImage;
    private final boolean rdOnly;
    private long size;
    private long offset = 0;

    public CephRepositoryChannel(Rbd rbd, String name, String mode) throws RadosException {
        this.rbd = rbd;
        switch (mode) {
            case "rw":
            case "rwd":
            case "rws":
                rbd.create(name, 0);
                rbdImage = rbd.open(name);
                rdOnly = false;
                size = 0;
                break;
            case "r":
                rbdImage = rbd.openReadOnly(name);
                rdOnly = true;
                size = rbdImage.stat().obj_size.get();
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

        if (rdOnly) {
            offset = Math.min(size, position);
        } else {
            offset = position;
            if (offset > size) {
                rbdImage.resize(offset);
            }
        }
        return this;
    }

    @Override
    public synchronized long size() throws IOException {
        return size;
    }

    @Override
    public synchronized int write(ByteBuffer buffer, long position) throws IOException {

        if (position + buffer.remaining() > size) {
            rbdImage.resize(position + buffer.remaining());
            size = position + buffer.remaining();
        }

        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        rbdImage.write(data, position, data.length);
        return data.length;
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        byte[] data = new byte[buffer.remaining()];
        int n = rbdImage.read(data, position, data.length);
        if (n > 0) {
            buffer.put(data, 0, n);
        }
        return n;
    }

    @Override
    public RepositoryChannel truncate(long size) throws IOException {
        rbdImage.resize(size);
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
    public synchronized int write(ByteBuffer src) throws IOException {
        byte[] data = new byte[src.remaining()];
        rbdImage.write(data, 0, data.length);
        return data.length;
    }

    @Override
    public synchronized boolean isOpen() {
        return rbdImage == null;
    }

    @Override
    public synchronized void close() throws IOException {
        rbdImage.close();
        rbdImage = null;
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
        int bytesToRead = offset + dst.remaining() > size ? (int)(size - offset) : dst.remaining();

        if (bytesToRead == 0) {
            return -1;
        }

        byte[] data = new byte[bytesToRead];
        int n = rbdImage.read(data, offset, data.length);
        if (n > 0) {
            dst.put(data, 0, n);
            offset += n;
        }
        return n;
    }

}
