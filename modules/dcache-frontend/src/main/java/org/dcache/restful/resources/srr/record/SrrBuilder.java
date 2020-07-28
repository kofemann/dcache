package org.dcache.restful.resources.srr.record;

import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginBrokerSubscriber;
import org.dcache.cells.CellStub;
import org.dcache.namespace.PnfsTopDirectoriesMessage;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.Version;
import org.dcache.vehicles.FileAttributes;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SrrBuilder {


    private Map<String, String> group2vo;
    // info provider properties
    private String name;
    private String id;
    private String architecture;
    private String quality;


    private CellStub spaceManager;

    private PoolMonitor remotePoolMonitor;

    private CellStub pnfsmanager;

    private LoginBrokerSubscriber loginBrokerSubscriber;

    private boolean spaceReservationEnabled;

    private SrrBuilder() {
    }

    public static SrrBuilder builder() {
        return new SrrBuilder();
    }

    public SrrBuilder withLoginBroker(LoginBrokerSubscriber loginBroker) {
        this.loginBrokerSubscriber = loginBroker;
        return this;
    }

    public SrrBuilder withSpaceManagerStub(CellStub stub) {
        this.spaceManager = stub;
        return this;
    }

    public SrrBuilder withNamespace(CellStub stub) {
        this.pnfsmanager = stub;
        return this;
    }

    public SrrBuilder withSpaceManagerEnaled(boolean isEnabled) {
        this.spaceReservationEnabled = isEnabled;
        return this;
    }

    public SrrBuilder withId(String id) {
        this.id = id;
        return this;
    }

    public SrrBuilder withPoolMonitor(PoolMonitor poolMonitor) {
        this.remotePoolMonitor = poolMonitor;
        return this;
    }

    public SrrBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public SrrBuilder withQuality(String quality) {
        this.quality = quality;
        return this;
    }

    public SrrBuilder withArchitecture(String architecture) {
        this.architecture = architecture;
        return this;
    }

    public SrrBuilder withGroupVoMapping(Map<String, String> group2vo) {
        this.group2vo = group2vo;
        return this;
    }

    public SrrRecord generate() throws InterruptedException, CacheException, NoRouteToCellException {

        Online onlineCapacity = new Online()
                .withTotalsize(totalSpace())
                .withUsedsize(usedSpace());

        List<Storageendpoint> storageendpoints = collectEndpoint();
        List<Storageshare> storageshares = collectShares();

        if (spaceReservationEnabled) {
            List<Storageshare> spacetokens = collectSpaceTokens();
            storageshares.addAll(spacetokens);
            onlineCapacity.setReservedsize(collectSpaceReserved());
        }

        Storagecapacity storagecapacity = new Storagecapacity()
                .withOnline(onlineCapacity);

        Storageservice storageservice = new Storageservice()
                .withId(id)
                .withName(name)
                .withImplementation("dCache")
                .withServicetype(architecture)
                .withQualitylevel(Storageservice.Qualitylevel.fromValue(quality))
                .withImplementationversion(Version.of(this).getVersion())
                .withLatestupdate(Instant.now().getEpochSecond())
                .withStorageendpoints(storageendpoints)
                .withStorageshares(storageshares)
                .withStoragecapacity(storagecapacity);

        SrrRecord record = new SrrRecord();
        record.setStorageservice(storageservice);

        return record;
    }

    private long totalSpace() {
        return remotePoolMonitor.getCostModule().getPoolCostInfos().stream()
                .mapToLong(p -> p.getSpaceInfo().getTotalSpace())
                .sum();
    }

    private long usedSpace() {
        return remotePoolMonitor.getCostModule().getPoolCostInfos().stream()
                .mapToLong(p -> p.getSpaceInfo().getTotalSpace() - p.getSpaceInfo().getFreeSpace() - p.getSpaceInfo().getRemovableSpace())
                .sum();
    }

    private List<Storageshare> collectSpaceTokens() throws CacheException, NoRouteToCellException, InterruptedException {

        long now = Instant.now().getEpochSecond();
        return spaceManager.sendAndWait(new GetSpaceTokensMessage()).getSpaceTokenSet().stream()
                .map(space -> {
                    Storageshare share = new Storageshare()
                            .withName(space.getDescription())
                            .withTotalsize(space.getSizeInBytes())
                            .withUsedsize(space.getUsedSizeInBytes())
                            .withTimestamp(now)
                            .withVos(Collections.singletonList(space.getVoGroup()))
                            .withAssignedendpoints(Collections.singletonList("all"))
                            .withAccesslatency(Storageshare.Accesslatency.fromValue(space.getAccessLatency()))
                            .withRetentionpolicy(Storageshare.Retentionpolicy.fromValue(space.getRetentionPolicy()));

                    return share;
                }).collect(Collectors.toList());
    }

    private long collectSpaceReserved() throws CacheException, NoRouteToCellException, InterruptedException {
        return spaceManager.sendAndWait(new GetSpaceTokensMessage()).getSpaceTokenSet().stream()
                .mapToLong(Space::getSizeInBytes)
                .sum();
    }

    private List<Storageshare> collectShares() throws CacheException, NoRouteToCellException, InterruptedException {

        PnfsTopDirectoriesMessage topDirectoriesMessage = pnfsmanager.sendAndWait(new PnfsTopDirectoriesMessage());

        Map<String, Storageshare> storageshares = new HashMap<>();
        long now = Instant.now().getEpochSecond();

        for (Map.Entry<String, FileAttributes> d : topDirectoriesMessage.getTopDirs().entrySet()) {

            FileAttributes attribute = d.getValue();
            String storageUnit = attribute.getStorageClass() + "@" + attribute.getHsm();
            String shareName = attribute.getStorageClass().toUpperCase().replace(':', '-');

            if (storageshares.containsKey(shareName)) {
                continue;
            }

            PoolPreferenceLevel[] l = remotePoolMonitor.getPoolSelectionUnit().match(
                    PoolSelectionUnit.DirectionType.WRITE,
                    "0.0.0.0",
                    "*/*",
                    d.getValue(),
                    null);

            if (l.length == 0) {
                // no pools online or under space manager control
                continue;
            }

            CostModule costModule = remotePoolMonitor.getCostModule();
            long totalSpace = 0;
            long usedSpace = 0;

            for (PoolPreferenceLevel sl : l) {
                List<String> pools = sl.getPoolList();

                totalSpace += pools.stream()
                        .map(costModule::getPoolCostInfo)
                        .mapToLong(p -> p.getSpaceInfo().getTotalSpace())
                        .sum();

                usedSpace += pools.stream()
                        .map(costModule::getPoolCostInfo)
                        .mapToLong(p -> p.getSpaceInfo().getTotalSpace() - p.getSpaceInfo().getFreeSpace() - p.getSpaceInfo().getRemovableSpace())
                        .sum();

                usedSpace = pools.stream()
                        .mapToLong(p -> {
                            ListenableFuture<String> reply = pnfsmanager.send(new CellPath(p), "rep ls -s", String.class);
                            try {
                                String out = reply.get();
                                List<String> stat = Splitter.on('\n')
                                        .omitEmptyStrings()
                                        .trimResults()
                                        .splitToList(out);

                                return stat.stream()
                                        .filter(o -> o.startsWith(storageUnit))
                                        .map(o -> o.split("\\s+"))
                                        .mapToLong(o -> Long.parseLong(o[3]) + Long.parseLong(o[5]))
                                        .sum();
                            } catch (InterruptedException e) {
                                return 0;
                            } catch(ExecutionException e) {
                                System.out.println("Error: " + e.getCause().toString());
                                return 0;
                            }
                        })
                        .sum();

            }

            String vos = group2vo.getOrDefault(Integer.toString(d.getValue().getGroup()), "gid:" + d.getValue().getGroup());


            Storageshare share = new Storageshare()
                    .withName(shareName)
                    .withTotalsize(totalSpace)
                    .withUsedsize(usedSpace)
                    .withTimestamp(now)
                    .withVos(Collections.singletonList(vos))
                    .withAssignedendpoints(Collections.singletonList("all"))
                    .withAccesslatency(Storageshare.Accesslatency.fromValue(d.getValue().getAccessLatency()))
                    .withRetentionpolicy(Storageshare.Retentionpolicy.fromValue(d.getValue().getRetentionPolicy()));
            storageshares.put(shareName, share);
        }
        return new ArrayList<>(storageshares.values());
    }

    private List<Storageendpoint> collectEndpoint() {
        return loginBrokerSubscriber.doors().stream().map(d -> {
                    Storageendpoint endpoint = new Storageendpoint()
                            .withName(id + "#" + d.getProtocolFamily() + "@" + d.getAddresses().get(0).getCanonicalHostName() + "-" + d.getPort())
                            .withInterfacetype(d.getProtocolFamily())
                            .withInterfaceversion(d.getProtocolVersion())
                            .withEndpointurl(d.getProtocolFamily() + "://" + d.getAddresses().get(0).getCanonicalHostName() + ":" + d.getPort() + d.getRoot())
                            .withAssignedshares(Collections.singletonList("all"));

                    return endpoint;
                }
        ).collect(Collectors.toList());
    }

}
