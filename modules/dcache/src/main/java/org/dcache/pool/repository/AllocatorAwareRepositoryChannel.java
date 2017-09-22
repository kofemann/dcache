package org.dcache.pool.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.stream.Stream;

import static org.dcache.util.ByteUnit.MiB;
import static com.google.common.base.Preconditions.checkArgument;

/**
 *
 */
public class AllocatorAwareRepositoryChannel extends ForwardingRepositoryChannel {

    public enum AllocatorMode {
        SOFT,
        HARD
    }

    private final Logger LOGGER = LoggerFactory.getLogger(AllocatorAwareRepositoryChannel.class);

    private final RepositoryChannel inner;
    private final Allocator allocator;
    private long allocated;

    /**
     * The minimum number of bytes to increment the space allocation.
     */
    static final long SPACE_INC = MiB.toBytes(50);

    /**
     * synchronization object used by allocator
     */
    private final Object allocationLock = new Object();

    private final Object positionLock = new Object();

    private final AllocatorMode allocatorMode = AllocatorMode.HARD;

    public AllocatorAwareRepositoryChannel(RepositoryChannel inner, Allocator allocator) throws IOException {
        this.inner = inner;
        this.allocator = allocator;
        // file existing in the repository already have allocated space.
        this.allocated = inner.size();
    }

    @Override
    protected RepositoryChannel delegate() {
        return inner;
    }

    @Override
    public synchronized void close() throws IOException {

        synchronized (allocationLock) {
            long length = size();
            LOGGER.warn("Adjusting allocation: allocated: {}, file size: {}",
                    allocated, length);
            if (length > allocated) {
                LOGGER.error("BUG detected! Under allocation detected: expected {}, current: {}.", length, allocated);
                try {
                    allocator.allocate(length - allocated);
                } catch (InterruptedException e) {
                    /*
                     * Space allocation is broken now. The entry size
                     * matches up with what was actually allocated,
                     * however the file on disk is too large.
                     *
                     * Should only happen during shutdown, so no harm done.
                     */
                    LOGGER.warn("Failed to adjust space reservation because "
                            + "the operation was interrupted. The pool is now over allocated.");
                    Thread.currentThread().interrupt();
                }
            } else {
                allocator.free(allocated - length);
            }
        }
        super.close();
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return this.write(srcs, 0, srcs.length);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {

        long byteToWrite = Stream.of(srcs)
                .skip(offset)
                .mapToLong(Buffer::remaining)
                .limit(length)
                .sum();

        synchronized (positionLock) {
            preallocate(position() + byteToWrite);
            return super.write(srcs, offset, length);
        }
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        preallocate(size);
        return super.truncate(size);
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        synchronized (positionLock) {
            preallocate(newPosition);
            return super.position(newPosition);
        }
    }

    @Override
    public long position() throws IOException {
        synchronized (positionLock) {
            return super.position();
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        synchronized (positionLock) {
            preallocate(position() + src.remaining());
            return super.write(src);
        }
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        preallocate(position + count);
        return super.transferFrom(src, position, count);
    }

    @Override
    public int write(ByteBuffer buffer, long position) throws IOException {
        preallocate(position + buffer.remaining());
        return super.write(buffer, position);
    }

    private void preallocate(long pos) throws IOException {
        synchronized (allocationLock) {
            try {
                checkArgument(pos >= 0);

                if (pos > allocated) {
                    long delta = Math.max(pos - allocated, SPACE_INC);
                    LOGGER.trace("preallocate: {}", delta);
                    if (allocatorMode == AllocatorMode.HARD) {
                        allocator.allocate(delta);
                    } else if (!allocator.allocateNow(delta)) {
                        throw new OutOfDiskException();
                    }
                    allocated += delta;
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException(e.getMessage());
            } catch (IllegalStateException e) {
                throw new ClosedChannelException();
            }
        }
    }
}
