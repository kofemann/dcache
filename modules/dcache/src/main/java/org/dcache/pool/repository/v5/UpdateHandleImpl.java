package org.dcache.pool.repository.v5;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsHandler;

import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.*;
import java.util.Collections;
import static org.dcache.namespace.FileAttribute.*;

class UpdateHandleImpl implements ReplicaDescriptor
{
    enum HandleState
    {
        OPEN, COMMITTED, CLOSED
    }

    private static final Logger _log =
        LoggerFactory.getLogger("logger.org.dcache.repository");

    /**
     * Time that a new CACHED file with no sticky flags will be marked
     * sticky.
     */
    private static final long HOLD_TIME = 5 * 60 * 1000; // 5 minutes

    private final CacheRepositoryV5 _repository;

    /** Space allocation is delegated to this allocator. */
    private final Allocator _allocator;

    /** The handler provides access to this entry. */
    private final MetaDataRecord _entry;

    /** Stub for talking to the PNFS manager. */
    private final PnfsHandler _pnfs;

    /** The entry state used during transfer. */
    private final EntryState _initialState;

    /** The entry state used when the handle is committed. */
    private EntryState _targetState;

    /** The state of the write handle. */
    private HandleState _state;

    /** Amount of space allocated for this handle. */
    private long _allocated;

    /** Current thread which performs allocation. */
    private Thread _allocationThread;

    private FileAttributes _fileAttributes;

    UpdateHandleImpl(CacheRepositoryV5 repository,
                    Allocator allocator,
                    PnfsHandler pnfs,
                    MetaDataRecord entry ) throws DiskErrorCacheException
    {
        _repository = checkNotNull(repository);
        _allocator = checkNotNull(allocator);
        _pnfs = checkNotNull(pnfs);
        _entry = checkNotNull(entry);
        _initialState = entry.getState();
        _state = HandleState.OPEN;
        _allocated = _entry.getSize();
        _fileAttributes = _entry.getFileAttributes();
    }

    private synchronized void setState(HandleState state)
    {
        _state = state;
        if (state != HandleState.OPEN && _allocationThread != null) {
            _allocationThread.interrupt();
        }
    }

    private synchronized boolean isOpen()
    {
        return _state == HandleState.OPEN;
    }

    /**
     * Sets the allocation thread to the calling thread. Blocks if
     * allocation thread is already set.
     *
     * @throws InterruptedException if thread is interrupted
     * @throws IllegalStateException if handle is closed
     */
    private synchronized void setAllocationThread()
        throws InterruptedException,
               IllegalStateException
    {
        while (_allocationThread != null) {
            wait();
        }

        if (!isOpen()) {
            throw new IllegalStateException("Handle is closed");
        }

        _allocationThread = Thread.currentThread();
    }

    /**
     * Clears the allocation thread field.
     */
    private synchronized void clearAllocationThread()
    {
        _allocationThread = null;
        notifyAll();
    }

    /**
     * Allocate space and block until space becomes available.
     *
     * @param size in bytes
     * @throws InterruptedException if thread is interrupted
     * @throws IllegalStateException if handle is closed
     * @throws IllegalArgumentException
     *             if <i>size</i> &lt; 0
     */
    @Override
    public void allocate(long size)
        throws IllegalStateException, IllegalArgumentException, InterruptedException
    {
        if (size < 0) {
            throw new IllegalArgumentException("Size is negative");
        }

        setAllocationThread();
        try {
            _allocator.allocate(size);
        } catch (InterruptedException e) {
            if (!isOpen()) {
                throw new IllegalStateException("Handle is closed");
            }
            throw e;
        } finally {
            clearAllocationThread();
        }

        synchronized (this) {
            _allocated += size;
            _entry.setSize(_allocated);
        }
    }

    /**
     * Freeing space through a write handle is not supported. This
     * method always throws IllegalStateException.
     */
    @Override
    public void free(long size)
        throws IllegalStateException
    {
        throw new IllegalStateException("Space cannot be freed through a write handle");
    }

    /**
     * Adjust space reservation. Will log an error in case of under
     * allocation.
     */
    private synchronized void adjustReservation(long length)
        throws InterruptedException
    {
        try {
            if (_allocated < length) {
                _log.error("Under allocation detected. This is a bug. Please report it.");
                _allocator.allocate(length - _allocated);
            } else if (_allocated > length) {
                _allocator.free(_allocated - length);
            }
            _allocated = length;
            _entry.setSize(length);
        } catch (InterruptedException e) {
            /* Space allocation is broken now. The entry size
             * matches up with what was actually allocated,
             * however the file on disk is too large.
             *
             * Should only happen during shutdown, so no harm done.
             */
            _log.warn("Failed to adjust space reservation because the operation was interrupted. The pool is now over allocated.");
            throw e;
        }
    }

    private void registerFileAttributesInNameSpace(long length)
            throws CacheException
    {
        FileAttributes fileAttributes = new FileAttributes();
        fileAttributes.setSize(length);
        _pnfs.setFileAttributes(_entry.getPnfsId(), fileAttributes);        
    }


    @Override
    public synchronized void commit()
        throws IllegalStateException, InterruptedException, CacheException
    {
        if (_state != HandleState.OPEN) {
            throw new IllegalStateException("Handle is closed");
        }

        try {
            _entry.touch();

            long length = getFile().length();
            adjustReservation(length);

            registerFileAttributesInNameSpace(length);
            _entry.setSize(length);

            setState(HandleState.COMMITTED);
        } catch (CacheException e) {
            /* If any of the PNFS operations return FILE_NOT_FOUND,
             * then we change the target state and the close method
             * will take care of removing the file.
             */
            if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                _targetState = EntryState.REMOVED;
            }
            throw e;
        }
    }

    /**
     * Fails the operation. Called by close without a successful
     * commit. The file is either removed or marked bad, depending on
     * its state.
     */
    private synchronized void fail()
    {
        long length = getFile().length();
        try {
            adjustReservation(length);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        /* Files from tape or from another pool are deleted in case of
         * errors.
         */
        if (_initialState == EntryState.FROM_POOL ||
            _initialState == EntryState.FROM_STORE) {
            _targetState = EntryState.REMOVED;
        }

        /* Unless replica is to be removed, register cache location and
         * other attributes.
         */
        if (_targetState != EntryState.REMOVED) {
            try {
                /* We register cache location separately in the failure flow, because
                 * updating other attributes (such as checksums) may itself trigger
                 * failures in PNFS, and at the very least our cache location should
                 * be registered.
                 */
                _pnfs.addCacheLocation(_entry.getPnfsId(), _repository.getPoolName());
                registerFileAttributesInNameSpace(length);
                _entry.setSize(length);
            } catch (CacheException e) {
                if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                    _targetState = EntryState.REMOVED;
                } else {
                    _log.warn("Failed to register after failed replica creation");
                }
            }
        }

        if (_targetState == EntryState.REMOVED) {
            _repository.setState(_entry, EntryState.REMOVED);
        } else {
            _log.warn("Marking pool entry as BROKEN");
            _repository.setState(_entry, EntryState.BROKEN);
        }
    }

    @Override
    public synchronized void close()
        throws IllegalStateException
    {
        switch (_state) {
        case CLOSED:
            throw new IllegalStateException("Handle is closed");

        case OPEN:
            fail();
            setState(HandleState.CLOSED);
            break;

        case COMMITTED:
            setState(HandleState.CLOSED);
            break;
        }
        _repository.destroyWhenRemovedAndUnused(_entry);
    }

    /**
     * @return disk file
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    @Override
    public synchronized File getFile() throws IllegalStateException
    {
        if (_state == HandleState.CLOSED) {
            throw new IllegalStateException("Handle is closed");
        }

        return _entry.getDataFile();
    }

    @Override
    public FileAttributes getFileAttributes()  throws IllegalStateException
    {
        return _fileAttributes;
    }

    @Override
    public synchronized Iterable<Checksum> getChecksums() throws CacheException
    {
        return Collections.EMPTY_SET;
    }

    @Override
    public synchronized void addChecksums(Iterable<Checksum> checksums)
    {

    }
}
