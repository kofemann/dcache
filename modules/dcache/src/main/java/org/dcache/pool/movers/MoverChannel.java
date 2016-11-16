package org.dcache.pool.movers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.SyncFailedException;
import java.nio.file.OpenOption;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;

import diskCacheV111.vehicles.ProtocolInfo;

import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.OutOfDiskException;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.util.ByteUnit.MiB;

import static org.dcache.commons.stats.Histograms.*;
/**
 * A wrapper for RepositoryChannel adding features used by movers.
 */
public class MoverChannel<T extends ProtocolInfo> implements RepositoryChannel
{
    private static final Logger _logSpaceAllocation =
        LoggerFactory.getLogger("logger.dev.org.dcache.poolspacemonitor." +
                                MoverChannel.class.getName());

    private static final Logger LOGGER
            = LoggerFactory.getLogger(MoverChannel.class);

    public enum AllocatorMode {
        SOFT,
        HARD
    }
    /**
     * The minimum number of bytes to increment the space
     * allocation.
     */
    private static final long SPACE_INC = MiB.toBytes(50);

    /**
     * Inner channel to which most operations are delegated.
     */
    private final RepositoryChannel _channel;

    /**
     * The {@link OpenOption} of the mover that created this MoverChannel.
     */
    private final Set<? extends OpenOption> _mode;

    /**
     * Space allocator provided by the pool.
     */
    private final Allocator _allocator;

    /**
     * Timestamp of when the transfer started.
     */
    private final long _transferStarted =
        System.currentTimeMillis();

    /**
     * Timestamp of when the last block was transferred.
     */
    private final AtomicLong _lastTransferred =
        new AtomicLong(_transferStarted);

    /**
     * The number of bytes transferred.
     */
    private final AtomicLong _bytesTransferred =
        new AtomicLong(0);

    /**
     * ProtocolInfo associated with the transfer.
     */
    private final T _protocolInfo;

    /**
     * The FileAttributes associated with the file being transfered.
     */
    private final FileAttributes _fileAttributes;

    /**
     * The number of bytes reserved in the space allocator. Only
     * updated while the monitor lock is held.
     */
    private volatile long _reserved;

    /**
     * Tells, should allocator block for available space or not.
     */
    private final AllocatorMode _allocatorMode;

    private final Histogram<Integer> readSizes = synchronizedHistogram( newIntHistogram("Read sizes", 10000));
    private final Histogram<Integer> writeSizes = synchronizedHistogram( newIntHistogram("Write sizes", 10000));

    public MoverChannel(Mover<T> mover, RepositoryChannel channel, AllocatorMode allocatorMode)
    {
        this(mover.getIoMode(), mover.getFileAttributes(), mover.getProtocolInfo(), channel, mover.getIoHandle(), allocatorMode);
    }

    public MoverChannel(Set<? extends OpenOption> mode, FileAttributes attributes, T protocolInfo,
            RepositoryChannel channel, Allocator allocator, AllocatorMode allocatorMode)
    {
        _mode = mode;
        _protocolInfo = protocolInfo;
        _channel = channel;
        _allocator = allocator;
        _fileAttributes = attributes;
        _allocatorMode = allocatorMode;
    }

    @Override
    public long position() throws IOException
    {
        return _channel.position();
    }

    @Override
    public synchronized MoverChannel<T> position(long position)
        throws IOException
    {
        _channel.position(position);
        return this;
    }

    @Override
    public long size() throws IOException
    {
        return _channel.size();
    }

    @Override
    public void sync() throws SyncFailedException, IOException
    {
        _channel.sync();
    }

    @Override
    public synchronized MoverChannel<T> truncate(long size) throws IOException
    {
        try {
            _channel.truncate(size);
            return this;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public void close() throws IOException
    {
        LOGGER.info("stats: \n{}\n{}", readSizes, writeSizes );
        _lastTransferred.set(System.currentTimeMillis());
        _channel.close();
    }

    @Override
    public boolean isOpen()
    {
        return _channel.isOpen();
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException
    {
        try {
            readSizes.add(dst.remaining());
            int bytes = _channel.read(dst);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public int read(ByteBuffer buffer, long position) throws IOException {
        try {
            readSizes.add(buffer.remaining());
            int bytes = _channel.read(buffer, position);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        try {
            for(ByteBuffer b: dsts) {
                readSizes.add(b.remaining());
            }
            long bytes = _channel.read(dsts, offset, length);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized long read(ByteBuffer[] dsts) throws IOException {
        try {
            for (ByteBuffer b : dsts) {
                readSizes.add(b.remaining());
            }

            long bytes = _channel.read(dsts);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized int write(ByteBuffer src) throws IOException {
        try {
            writeSizes.add(src.remaining());
            preallocate(position() + src.remaining());
            int bytes = _channel.write(src);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        try {
            writeSizes.add(buffer.remaining());
            preallocate(position + buffer.remaining());
            int bytes = _channel.write(buffer, position);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        try {
            long remaining = 0;
            for (int i = offset; i < offset + length; i++) {
                writeSizes.add(srcs[i].remaining());
                remaining += srcs[i].remaining();
            }

            preallocate(position() + remaining);

            long bytes = _channel.write(srcs, offset, length);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public synchronized long write(ByteBuffer[] srcs) throws IOException {
        try {
            long remaining = 0;
            for (ByteBuffer src: srcs) {
                writeSizes.add(src.remaining());
                remaining += src.remaining();
            }
            preallocate(position() + remaining);

            long bytes = _channel.write(srcs);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        try {
            long bytes = _channel.transferTo(position, count, target);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        try {
            preallocate(position + count);
            long bytes = _channel.transferFrom(src, position, count);
            _bytesTransferred.getAndAdd(bytes);
            return bytes;
        } finally {
            _lastTransferred.set(System.currentTimeMillis());
        }
    }

    public Set<? extends OpenOption> getIoMode() {
        return _mode;
    }

    public T getProtocolInfo() {
        return _protocolInfo;
    }

    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    public long getBytesTransferred() {
        return _bytesTransferred.get();
    }

    public long getTransferTime() {
        return (_channel.isOpen()
                ? System.currentTimeMillis()
                : getLastTransferred()) - _transferStarted;
    }

    public long getLastTransferred() {
        return _lastTransferred.get();
    }

    public long getAllocated() {
        return _reserved;
    }

    private synchronized void preallocate(long pos)
        throws IOException
    {
        try {
            checkArgument(pos >= 0);

            if (pos > _reserved) {
                long delta = Math.max(pos - _reserved, SPACE_INC);
                _logSpaceAllocation.trace("preallocate: {}", delta);
                if (_allocatorMode == AllocatorMode.HARD) {
                    _allocator.allocate(delta);
                } else if (!_allocator.allocateNow(delta)) {
                    throw new OutOfDiskException();
                }
                _reserved += delta;
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        } catch (IllegalStateException e) {
            throw new ClosedChannelException();
        }
    }
}
