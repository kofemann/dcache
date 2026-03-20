package org.dcache.pool.migration;

import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import org.dcache.cells.CellStub;
import org.dcache.pool.repository.Repository;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Describes the context of migration jobs.
 */
public class MigrationContextImpl implements MigrationContext, CellIdentityAware {

    private String _poolName;
    private ScheduledExecutorService _executor;
    private CellStub _pool;
    private CellStub _pnfs;
    private CellStub _poolManager;
    private CellStub _pinManager;
    private Repository _repository;
    private final ConcurrentMap<PnfsId, PnfsId> _locks =
          new ConcurrentHashMap<>();

    @Override
    public void setCellAddress(CellAddressCore address) {
        _poolName = address.getCellName();
    }

    @Override
    public String getPoolName() {
        return _poolName;
    }

    @Override
    public ScheduledExecutorService getExecutor() {
        return _executor;
    }

    @Autowired
    public void setExecutor(ScheduledExecutorService executor) {
        _executor = executor;
    }

    @Override
    public CellStub getPoolStub() {
        return _pool;
    }

    @Autowired
    public void setPoolStub(CellStub pool) {
        _pool = pool;
    }

    @Override
    public CellStub getPnfsStub() {
        return _pnfs;
    }

    @Autowired
    public void setPnfsStub(CellStub pnfs) {
        _pnfs = pnfs;
    }

    @Override
    public CellStub getPoolManagerStub() {
        return _poolManager;
    }

    @Autowired
    public void setPoolManagerStub(CellStub poolManager) {
        _poolManager = poolManager;
    }

    @Override
    public CellStub getPinManagerStub() {
        return _pinManager;
    }

    @Autowired
    public void setPinManagerStub(CellStub pinManager) {
        _pinManager = pinManager;
    }

    @Override
    public Repository getRepository() {
        return _repository;
    }

    @Autowired
    public void setRepository(Repository repository) {
        _repository = repository;
    }

    @Override
    public boolean lock(PnfsId pnfsId) {
        return (_locks.put(pnfsId, pnfsId) == null);
    }

    @Override
    public void unlock(PnfsId pnfsId) {
        _locks.remove(pnfsId);
    }

    @Override
    public boolean isActive(PnfsId pnfsId) {
        return _locks.containsKey(pnfsId);
    }
}
