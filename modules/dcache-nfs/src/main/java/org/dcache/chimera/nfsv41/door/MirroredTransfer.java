package org.dcache.chimera.nfsv41.door;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.Transfer;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.Pool;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.Predicate;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.chimera.nfsv41.mover.NFS4ProtocolInfo;
import org.dcache.nfs.status.LayoutTryLaterException;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.RemotePoolMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.TimebasedCounter;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;
import static org.dcache.namespace.FileAttribute.TYPE;
import org.dcache.nfs.v4.xdr.deviceid4;
import org.dcache.pool.assumption.Assumptions;
import org.dcache.poolmanager.PoolManagerStub;
import org.dcache.util.RedirectedTransfer;

/**
 * Transfer, which maintains multiple movers on different pools for the same
 * file.
 */
public class MirroredTransfer {

    private static final Logger LOGGER
            = LoggerFactory.getLogger(MirroredTransfer.class);
    /**
     * Client transfers associated with this logical transfer.
     */
    Set<RedirectedTransfer<NFSv41Door.PoolDS>> transfers = new HashSet<>();

    /**
     * File's attributes.
     */
    private FileAttributes fileAttributes = new FileAttributes();

    /**
     * Id of the file.
     */
    private PnfsId pnfsId;

    /**
     * Namespace handler.
     */
    private PnfsHandler pnfs;

    private boolean isWrite;

    private RemotePoolMonitor poolMonitor;
    private PoolManagerStub poolManagerStub;

    private NFS4ProtocolInfo protocolInfo;

    private static final Random RANDOM = new Random();

    private CellStub poolStub;
    private String ioQueue;
    private CellAddressCore cellAddress;
    private CellStub billingStub;
    private InetSocketAddress remote;
    private boolean _isBillingNotified;

    private static final TimebasedCounter _sessionCounter = new TimebasedCounter();
    private final long _id = _sessionCounter.next();
    private final Object _session = CDC.getSession();

    public void setPnfs(PnfsHandler pnfs) {
        this.pnfs = pnfs;
    }

    public void setIsWrite(boolean isWrite) {
        this.isWrite = isWrite;
    }

    public void setPoolManagerStub(PoolManagerStub poolManagerStub) {
        this.poolManagerStub = poolManagerStub;
    }

    public void setPoolStub(CellStub poolStub) {
        this.poolStub = poolStub;
    }

    public void setPoolMonitor(RemotePoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    /**
     * Returns the PnfsId of the file to be transferred.
     */
    @Nullable
    public synchronized PnfsId getPnfsId() {
        return fileAttributes.isDefined(PNFSID) ? fileAttributes.getPnfsId() : null;
    }

    /**
     * Sets the PnfsId of the file to be transferred.
     */
    public synchronized void setPnfsId(PnfsId pnfsid) {
        fileAttributes.setPnfsId(pnfsid);
    }

    public ListenableFuture<Void> readNameSpaceEntryAsync() {
        Set<FileAttribute> attr = EnumSet.of(PNFSID, TYPE, STORAGEINFO, SIZE);
        attr.addAll(PoolMgrSelectReadPoolMsg.getRequiredAttributes());

        PnfsGetFileAttributes request;
        request = new PnfsGetFileAttributes(getPnfsId(), attr);

        request.setUpdateAtime(true);
        ListenableFuture<PnfsGetFileAttributes> reply = pnfs.requestAsync(request, pnfs.getPnfsTimeout());

        return CellStub.transformAsync(reply,
                msg -> {
                    FileAttributes attributes = msg.getFileAttributes();

                    /* We can only transfer regular files.
                     */
                    FileType type = attributes.getFileType();
                    if (type == FileType.DIR || type == FileType.SPECIAL) {
                        throw new NotFileCacheException("Not a regular file");
                    }

                    setFileAttributes(attributes);
                    return immediateFuture(null);
                });
    }

    public synchronized void setFileAttributes(FileAttributes fileAttributes) {
        this.fileAttributes = fileAttributes;
        transfers.forEach(t -> t.setFileAttributes(fileAttributes));
    }

    /**
     * Sets whether this is an upload.
     */
    public synchronized void setWrite(boolean isWrite) {
        this.isWrite = isWrite;
    }

    public synchronized boolean isWrite() {
        return isWrite;
    }

    /**
     * Selects a pools suitable for the transfer.
     */
    public synchronized void selectPools() throws LayoutTryLaterException, CacheException, InterruptedException, ExecutionException, NoRouteToCellException {

        poolMonitor.refresh();
        PoolSelectionUnit selectionUnit = poolMonitor.getPoolSelectionUnit();
        String storeUnitName = fileAttributes.getStorageInfo().getStorageClass() + "@" + fileAttributes.getStorageInfo().getHsm();

        if (isWrite()) {
            int mirrorCount = selectionUnit.getSelectionUnits().entrySet().stream()
                    .filter(e -> e.getValue().getType() == PoolSelectionUnit.UnitType.STORE)
                    .filter(e -> e.getKey().equals(storeUnitName))
                    .map(e -> e.getValue())
                    .map(StorageUnit.class::cast)
                    .mapToInt(e -> e.getRequiredCopies())
                    .findAny().orElse(1);

            PoolPreferenceLevel[] selections = selectionUnit.match(
                    isWrite() ? PoolSelectionUnit.DirectionType.WRITE : PoolSelectionUnit.DirectionType.READ,
                    protocolInfo.getSocketAddress().getAddress().getHostAddress(),
                    protocolInfo.getProtocol() + "/" + protocolInfo.getMajorVersion(),
                    fileAttributes, null);

            CostModule cm = poolMonitor.getCostModule();

            if (selections.length == 0) {
                LOGGER.warn("no write pools configured");
                throw new LayoutTryLaterException();
            }

            List<PoolInfo> pools
                    = selections[0].getPoolList().stream()
                            .map(cm::getPoolInfo)
                            .filter(Objects::nonNull)
                            .collect(toList());

            if (pools.size() < mirrorCount) {
                LOGGER.warn("no write pools available in first selection");
                throw new LayoutTryLaterException();
            }

            WeightedPool[] weightedPools = toWeightedWritePoolsArray(pools);

            for (int i = 0; i < mirrorCount; i++) {
                int index = selectWrandomIndex(weightedPools, weightedPools.length - i);
                WeightedPool weightedPool = weightedPools[index];
                Pool pool = new Pool(weightedPool.getCostInfo().getName(), weightedPool.getCostInfo().getAddress(), Assumptions.none());
                RedirectedTransfer<NFSv41Door.PoolDS> t = new RedirectedTransfer<>(pnfs, Subjects.ROOT, Restrictions.none(), Subjects.ROOT, FsPath.ROOT, _id, _session);
                t.setProtocolInfo(protocolInfo);
                t.setPoolManagerStub(poolManagerStub);
                t.setPoolStub(poolStub);
                t.setBillingStub(billingStub);
                t.setCellAddress(cellAddress);
                t.setWrite(isWrite);
                t.setFileAttributes(fileAttributes);
                transfers.add(t);
                t.setPool(pool);
                weightedPools[index] = weightedPools[weightedPools.length - i - 1];
            }
        } else {
            RedirectedTransfer<NFSv41Door.PoolDS> t = new RedirectedTransfer<>(pnfs, Subjects.ROOT, Restrictions.none(), Subjects.ROOT, FsPath.ROOT, _id, _session);
            if (fileAttributes.getLocations().isEmpty()) {
                LOGGER.warn("no read locations");
                throw new LayoutTryLaterException();
            }
            String pool = fileAttributes.getLocations().iterator().next();
            t.setPool(new Pool(pool, new CellAddressCore(pool), Assumptions.none()));
            t.setProtocolInfo(protocolInfo);
            t.setPoolManagerStub(poolManagerStub);
            t.setPoolStub(poolStub);
            t.setBillingStub(billingStub);
            t.setCellAddress(cellAddress);
            t.setFileAttributes(fileAttributes);
            t.setWrite(isWrite);
            transfers.add(t);
        }
    }

    public synchronized String getTransaction() {
        if (_session != null) {
            return _session + ":" + _id;
        } else if (cellAddress != null) {
            return "door:" + cellAddress + ":" + _id;
        } else {
            return String.valueOf(_id);
        }
    }

    private final Instant crTime = Instant.now();

    /**
     * Returns an IoDoorEntry describing the transfer. This is used by the
     * "Active Transfer" view of the HTTP monitor.
     */
    public synchronized IoDoorEntry getIoDoorEntry() {
        return new IoDoorEntry(_id,
                getPnfsId(),
                Subjects.ROOT,
                "N/A",
                "N/A",
                crTime.toEpochMilli(),
                remote.getHostString());
    }

    public synchronized void notifyBilling(int code, String error) {
        if (_isBillingNotified) {
            return;
        }

        DoorRequestInfoMessage msg = new DoorRequestInfoMessage(cellAddress);
        msg.setSubject(Subjects.ROOT);
        msg.setBillingPath("/");
        msg.setTransferPath("/");
        msg.setTransactionDuration(Duration.between(crTime, Instant.now()).toMillis());
        msg.setTransaction(getTransaction());
        String chain = remote.getAddress().getHostAddress();
        msg.setClientChain(chain);
        msg.setClient(chain);
        msg.setPnfsId(getPnfsId());
        if (fileAttributes.isDefined(SIZE)) {
            msg.setFileSize(fileAttributes.getSize());
        }
        msg.setResult(code, error);
        if (fileAttributes.isDefined(STORAGEINFO)) {
            msg.setStorageInfo(fileAttributes.getStorageInfo());
        }
        billingStub.notify(msg);

        _isBillingNotified = true;
    }

    public synchronized ListenableFuture<List<Void>> startMoverAsync() {
        List<ListenableFuture<Void>> futures = transfers.stream()
                .map(t -> t.startMoverAsync(TimeUnit.MINUTES.toMillis(5)))
                .collect(Collectors.toList());

        return Futures.allAsList(futures);
    }

    private WeightedPool[] toWeightedWritePoolsArray(Collection<PoolInfo> costInfos) {

        long totalFree = costInfos.stream()
                .map(PoolInfo::getCostInfo)
                .map(PoolCostInfo::getSpaceInfo)
                .mapToLong(c -> c.getFreeSpace() + c.getRemovableSpace())
                .sum();

        WeightedPool[] weghtedPools = costInfos.stream()
                .map(c -> new WeightedPool(c, totalFree))
                .sorted()
                .toArray(WeightedPool[]::new);

        return weghtedPools;
    }

    public synchronized void killMover(int i, String pool_disabled) {
        transfers.stream().forEach(t -> t.killMover(i, pool_disabled));
    }

    public synchronized boolean waitForMover(long timeout) throws CacheException, InterruptedException {

        while(!transfers.stream().allMatch(t-> !t.hasMover())) {
            wait(timeout);
        }

        return transfers.stream().allMatch(t-> !t.hasMover());
    }

    private static class WeightedPool implements Comparable<WeightedPool> {

        private final PoolInfo costInfo;
        private final double weight;

        public WeightedPool(PoolInfo costInfo, long total) {
            this.costInfo = costInfo;
            long spaceToUse = costInfo.getCostInfo().getSpaceInfo().getFreeSpace()
                    + costInfo.getCostInfo().getSpaceInfo().getRemovableSpace();
            this.weight = (double) spaceToUse / total;
        }

        public PoolInfo getCostInfo() {
            return costInfo;
        }

        public double getWeight() {
            return weight;
        }

        @Override
        public int compareTo(WeightedPool other) {
            return Double.compare(this.getWeight(), other.getWeight());
        }
    }

    private int selectWrandomIndex(WeightedPool[] weightedPools, int max) {
        double selection = RANDOM.nextDouble();
        double total = 0;
        int i = 0;
        for (i = 0; (i < max) && (total <= selection); i++) {
            total += weightedPools[i].getWeight();
        }
        return i - 1;
    }

    private class DifferentHost implements Predicate<PoolInfo> {

        private final String host;

        DifferentHost(String host) {
            this.host = host;
        }

        @Override
        public boolean test(PoolInfo t) {
            String hostname = t.getHostName();
            return !host.equals(hostname);
        }
    }

    public void setProtocolInfo(NFS4ProtocolInfo protocolInfo) {
        this.protocolInfo = protocolInfo;
    }

    /**
     * Returns a ProtocolInfo suitable for selecting a pool. By default the
     * protocol info set with setProtocolInfo is returned.
     */
    protected ProtocolInfo getProtocolInfoForPoolManager() {
        return protocolInfo;
    }

    /**
     * Returns a ProtocolInfo suitable for starting a mover. By default the
     * protocol info set with setProtocolInfo is returned.
     */
    protected ProtocolInfo getProtocolInfoForPool() {
        return protocolInfo;
    }

    protected synchronized deviceid4[] waitForRedirect(long NFS_REQUEST_BLOCKING) throws InterruptedException {

        checkState(!transfers.isEmpty());

        while (!transfers.stream().allMatch(t -> t.getRedirect() != null)) {
            System.out.println( "redirected: "  + transfers.stream().filter(t -> t.getRedirect() != null).count());
            wait(NFS_REQUEST_BLOCKING);
        }

         deviceid4[] mirros = transfers.stream()
                .map(RedirectedTransfer::getRedirect)
                .map(NFSv41Door.PoolDS::getDeviceId)
                .toArray(deviceid4[]::new);
         System.out.println(Arrays.toString(mirros));
         return mirros;
    }

    public synchronized void redirect(NFSv41Door.PoolDS device, String poolName, long id) {

        System.out.println("redirect: " + device + " , expecting " + transfers.size());

        transfers.stream()
                .peek(t -> System.out.println("ID: " + t.getId() + " " + id))
                .filter(t -> t.getPool().getName().equals(device.getName()))
                .findAny()
                .ifPresent(t -> t.redirect(device));
        notifyAll();
    }

    public synchronized void finished(DoorTransferFinishedMessage transferFinishedMessage) {
        transfers.stream()
                .filter(t -> t.getPool().getName().equals(transferFinishedMessage.getPoolName()))
                .forEach(t -> t.finished(transferFinishedMessage));
        notifyAll();
    }

    public synchronized boolean hasMover() {
        return transfers.stream().filter(Transfer::hasMover).findAny().isPresent();
    }

    public void setCellAddress(CellAddressCore cellAddress) {
        this.cellAddress = cellAddress;
    }

    public void setBillingStub(CellStub _billingStub) {
        this.billingStub = _billingStub;
    }

    public void setClientAddress(InetSocketAddress remote) {
        this.remote = remote;
    }

    public void setIoQueue(String ioQueue) {
        this.ioQueue = ioQueue;
    }

    public void setKafkaSender(Consumer<DoorRequestInfoMessage> _kafkaSender) {

    }
}
