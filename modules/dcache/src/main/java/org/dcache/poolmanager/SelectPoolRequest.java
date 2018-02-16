/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.poolmanager;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.concurrent.GuardedBy;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SelectPoolRequest implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectPoolRequest.class);

    public enum State {
        INIT,
        NO_LOCATIONS,
        READ_DENED,
        FAILED,
        DONE,
        WAITING,
        LOST_FILE,
        NO_ONLINE_POOLS,
        DISPOSE,
        NEED_STAGE,
        P2P_DENIED,
        STAGE_COMPLETE,
        P2P_COMPLETE,
        NEED_P2P
    }

    private State state;
    private final PnfsId pnfsId;
    private final FileAttributes attributes;
    private final PoolSelector poolSelector;
    private final CellEndpoint messageSender;
    private final Executor executor;
    private final Map<String, SelectPoolRequest> waitingRequests;
    private final String identity;
    /**
     * Messages that pending on this requests
     */
    private final Set<CellMessage> cellMessages = new HashSet<>();
    /**
     * A payload object to pass between runs. The object type is depends on
     * state.
     */
    private Object payload;

    public SelectPoolRequest(CellEndpoint messageSender, PoolMgrSelectReadPoolMsg request, PoolMonitor poolMonitor, Executor executor, Map<String, SelectPoolRequest> waitingRequests) {
        this.state = State.INIT;
        this.messageSender = messageSender;
        this.attributes = request.getFileAttributes();
        this.pnfsId = attributes.getPnfsId();
        this.poolSelector = poolMonitor.getPoolSelector(attributes, request.getProtocolInfo(), request.getLinkGroup());
        this.waitingRequests = waitingRequests;
        this.executor = executor;
        this.identity = pnfsId.toString();
    }

    public void addCellMessage(CellMessage cellMessage) {
        cellMessages.add(cellMessage);
    }

    public void addAllCellMessages(Set<CellMessage> cellMessages) {
        this.cellMessages.addAll(cellMessages);
    }

    @Override
    public int hashCode() {
        return identity.hashCode();
    }

    public String getIdentity() {
        return identity;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SelectPoolRequest other = (SelectPoolRequest) obj;
        return this.identity.equals(other.identity);
    }

    public State getState() {
        return state;
    }

    public void setState(State newState) {
        this.state = newState;
    }

    @Override
    public void run() {
        switch (state) {
            case INIT:
                // clear old state, if retry
                payload = null;
                if (!attributes.isDefined(FileAttribute.LOCATIONS) || attributes.getLocations().isEmpty()) {
                    state = State.NO_LOCATIONS;
                    break;
                }
                try {
                    SelectedPool pool = poolSelector.selectReadPool();
                    cellMessages.forEach((dmg.cells.nucleus.CellMessage m) -> {
                        PoolMgrSelectReadPoolMsg reply = (PoolMgrSelectReadPoolMsg) m.getMessageObject();
                        reply.setPoolName(pool.name());
                        reply.setPoolAddress(pool.address());
                        reply.setAssumption(pool.assumption());
                        reply.setSucceeded();
                        m.revertDirection();
                        messageSender.sendMessage(m);
                    });
                    state = State.DONE;
                } catch (PermissionDeniedCacheException e) {
                    state = State.READ_DENED;
                } catch (FileNotInCacheException e) {
                    state = State.NO_LOCATIONS;
                } catch (CacheException e) {
                    state = State.FAILED;
                    payload = e;
                }
                break;
            case NO_LOCATIONS:
                if (attributes.getRetentionPolicy() != RetentionPolicy.CUSTODIAL) {
                    state = State.NO_ONLINE_POOLS;
                    break;
                }
                if (!attributes.getStorageInfo().isStored()) {
                    state = State.LOST_FILE;
                    break;
                }
                state = State.NEED_STAGE;
                break;
            case NEED_STAGE:
                try {
                    String previousPool;
                    String previousHost;
                    SelectedPool pool = poolSelector.selectStagePool(identity, identity);
                    //  remember what we did last time
                    payload = pool;
                    state = State.WAITING;
                    synchronized (waitingRequests) {
                        if (addToWaitingQueue()) {
                            CellMessage cellMessage = new CellMessage(new CellPath(pool.address()), new PoolFetchFileMessage(pool.name(), attributes));
                            messageSender.sendMessage(cellMessage);
                        }
                    }
                } catch (CacheException e) {
                    state = State.FAILED;
                    payload = e;
                }
                break;
            case P2P_DENIED:
                if (attributes.getRetentionPolicy() != RetentionPolicy.CUSTODIAL || !attributes.getStorageInfo().isStored()) {
                    state = State.NO_ONLINE_POOLS;
                    break;
                }
                state = State.NEED_STAGE;
                break;
            case READ_DENED:
            case NEED_P2P:
                try {
                    Partition.P2pPair p2p = poolSelector.selectPool2Pool(true);
                    state = State.WAITING;
                    synchronized (waitingRequests) {
                        if (addToWaitingQueue()) {
                            CellMessage cellMessage = new CellMessage(new CellPath(p2p.destination.address()), new Pool2PoolTransferMsg(p2p.source.name(), p2p.destination.name(), attributes));
                            messageSender.sendMessage(cellMessage);
                        }
                    }
                } catch (PermissionDeniedCacheException e) {
                    // p2p not allowed. Try to stage.
                    state = State.P2P_DENIED;
                } catch (CacheException e) {
                    state = State.FAILED;
                    payload = e;
                    break;
                }
                break;
            case STAGE_COMPLETE:
            case P2P_COMPLETE:
                synchronized (waitingRequests) {
                    cellMessages.forEach((dmg.cells.nucleus.CellMessage m) -> {
                        PoolMgrSelectReadPoolMsg reply = (PoolMgrSelectReadPoolMsg) m.getMessageObject();
                        reply.setReply(CacheException.OUT_OF_DATE, "File location/availability is changed, retry");
                        m.revertDirection();
                        messageSender.sendMessage(m);
                    });
                }
                state = State.DONE;
                break;
            case WAITING:
                // Request is out of wating queue. Start over.
                state = State.INIT;
                break;
        }
        // post-processing: exit, resubmit or go into wating queue
        switch (state) {
            case DONE:
            case WAITING:
            case DISPOSE:
                return;
            case NO_ONLINE_POOLS:
            case LOST_FILE:
            case FAILED:
                addToWaitingQueue();
                break;
            case INIT:
            default:
                executor.execute(this);
        }
    }

    @Override
    public String toString() {
        return String.format("%s m=%d status=%s, payload={}",
                identity,
                cellMessages.size(),
                state,
                payload == null ? "" : String.valueOf(payload));
    }

    @GuardedBy(value = "waitingRequests")
    /**
     * Add request to the waiting queue. If a request with the same identity
     * exists, then current request message will be added to the list of
     * request.
     *
     * @returns true, if new request is added.
     */
    private boolean addToWaitingQueue() {
        SelectPoolRequest r = waitingRequests.putIfAbsent(this.identity, this);
        if (r != null) {
            r.addAllCellMessages(cellMessages);
            state = State.DISPOSE;
            LOGGER.info("Merging request in wating queue: {} {}", r, this);
            return false;
        }
        LOGGER.info("Adding new request into wating queue: {}", this);
        return true;
    }

}
