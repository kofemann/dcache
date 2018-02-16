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

import com.google.common.util.concurrent.MoreExecutors;
import diskCacheV111.poolManager.PoolMonitorV5;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dcache.poolmanager.SelectPoolRequest.State.*;

/**
 *
 */
public class RequestContainer extends AbstractCellComponent implements CellMessageReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestContainer.class);

    private CellStub billing;
    private CellStub poolStub;

    private PoolSelectionUnit selectionUnit;
    private PoolMonitorV5 poolMonitor;
    private PnfsHandler pnfsHandler;
    private Executor executor;
    private final Executor fastTrack = MoreExecutors.directExecutor();
    private PartitionManager partitionManager;
    private CheckStagePermission stagePolicyDecisionPoint;
    private boolean sendHitInfo;

    /**
     * Set of requests that waiting for external event, like p2p or stage.
     */
    private final Map<String, SelectPoolRequest> waitingRequests = new HashMap<>();

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public void start() {

    }

    public void shutdown() {

    }

    public List<RestoreHandlerInfo> getRestoreHandlerInfo() {
        return Collections.emptyList();
    }

    public void poolStatusChanged(String poolName, int poolStatus) {

        if (poolStatus == PoolStatusChangedMessage.UP) {
            synchronized (waitingRequests) {
                Set<String> retriedIdentities = waitingRequests.values().stream()
                        .filter(r -> r.getState() == NO_ONLINE_POOLS)
                        .map(SelectPoolRequest::getIdentity)
                        .collect(Collectors.toSet());

                for (String s : retriedIdentities) {
                    SelectPoolRequest r = waitingRequests.remove(s);
                    if (r != null) {
                        r.setState(INIT); // start over
                        executor.execute(r);
                    }
                }
            }
        }
    }

    public void messageArrived(CellMessage envelope,
            PoolMgrSelectReadPoolMsg request)
            throws PatternSyntaxException, IOException {

        SelectPoolRequest r = new SelectPoolRequest(this.getCellEndpoint(),
                request,
                poolMonitor,
                executor,
                waitingRequests);
        r.addCellMessage(envelope);

        // try fast track first. It will resubmit into working queue, if needed.
        fastTrack.execute(r);
    }

    public void messageArrived(CellMessage envelope, PoolFetchFileMessage msg) {

        if (!msg.isReply()) {
            // whe pool and pool manager in the same cell, then we will see request as well as reply
            return;
        }

        if (msg.getReturnCode() == CacheException.HSM_DELAY_ERROR) {
            // HSM tells us to back off. just drop the message
        }

        // FIXME: identity
        synchronized (waitingRequests) {
            SelectPoolRequest r = waitingRequests.remove(msg.getPnfsId().toString());
            if (r != null) {
                if (msg.getReturnCode() == 0) {
                    r.setState(STAGE_COMPLETE);
                } else {
                    r.setState(NEED_STAGE);
                }
                executor.execute(r);
            }
        }
    }

    public void messageArrived(CellMessage envelope, Pool2PoolTransferMsg msg) {

        if (!msg.isReply()) {
            // whe pool and pool manager in the same cell, then we will see request as well as reply
            return;
        }

        // FIXME: identity
        synchronized (waitingRequests) {
            SelectPoolRequest r = waitingRequests.remove(msg.getPnfsId().toString());
            if (r != null) {
                if (msg.getReturnCode() == 0) {
                    r.setState(P2P_COMPLETE);
                } else {
                    r.setState(NEED_P2P);
                }
                executor.execute(r);
            }
        }
    }

    public void setStageConfigurationFile(String path) {
        this.stagePolicyDecisionPoint = new CheckStagePermission(path);
    }

    public void setHitInfoMessages(boolean sendHitInfo) {
        this.sendHitInfo = sendHitInfo;
    }

    public void setBilling(CellStub billing) {
        this.billing = billing;
    }

    public void setPoolStub(CellStub poolStub) {
        this.poolStub = poolStub;
    }

    public void setSelectionUnit(PoolSelectionUnit selectionUnit) {
        this.selectionUnit = selectionUnit;
    }

    public void setPoolMonitor(PoolMonitorV5 poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    public void setPnfsHandler(PnfsHandler pnfsHandler) {
        this.pnfsHandler = pnfsHandler;
    }

    public void setPartitionManager(PartitionManager partitionManager) {
        this.partitionManager = partitionManager;
    }

    public void setStagePolicyDecisionPoint(CheckStagePermission stagePolicyDecisionPoint) {
        this.stagePolicyDecisionPoint = stagePolicyDecisionPoint;
    }

    public void setSendHitInfo(boolean sendHitInfo) {
        this.sendHitInfo = sendHitInfo;
    }

}
