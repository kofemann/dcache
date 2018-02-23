package org.dcache.poolmanager;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.dcache.vehicles.FileAttributes;
import org.junit.Test;
import org.junit.Before;

import org.dcache.poolmanager.SelectPoolRequest;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;
import static org.dcache.poolmanager.SelectPoolRequest.State.*;
import static org.junit.Assert.*;
import org.mockito.ArgumentCaptor;
import static org.mockito.BDDMockito.given;

/**
 *
 */
public class SelectPoolRequestTest {

    PoolMgrSelectReadPoolMsg msg;
    SelectPoolRequest request;
    CellEndpoint cellEndpoint;
    PoolMonitor poolMonitor;
    ExecutorService executorService;
    Map<String, SelectPoolRequest> waitingRequests;
    Set<SelectPoolRequest> activeRequests;

    FileAttributes fileAttributes;
    PoolSelector poolSelector;
    PoolCostInfo costInfo;
    SelectedPool selectedPool;

    @Before
    public void setUp() {
        cellEndpoint = mock(CellEndpoint.class);
        poolMonitor = mock(PoolMonitor.class);
        poolSelector = mock(PoolSelector.class);
        costInfo = mock(PoolCostInfo.class);
        selectedPool = mock(SelectedPool.class);
        activeRequests = new HashSet<>();
        waitingRequests = new HashMap<>();

        executorService = new ExecutorService() {
            @Override
            public void shutdown() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public List<Runnable> shutdownNow() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean isShutdown() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean isTerminated() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public <T> Future<T> submit(Callable<T> task) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public <T> Future<T> submit(Runnable task, T result) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public Future<?> submit(Runnable task) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void execute(Runnable command) {
                activeRequests.add((SelectPoolRequest) command);
            }
        };
        given(poolMonitor.getPoolSelector(any(), any(), any())).willReturn(poolSelector);
    }

    @Test
    public void shouldReplyWithPoolWhenAvailable() throws CacheException {

        given(selectedPool.name()).willReturn("pool-A");
        given(selectedPool.address()).willReturn(new CellAddressCore("pool-A"));
        given(poolSelector.selectReadPool()).willReturn(selectedPool);

        whenDiskFile("00000D6AB182056D4A7C97F556D2C55622E3");
        hasLocation("pool-A");
        requested();
        fromClient("some-door@some-domain");
        afterExecution();

        assertEquals(DONE, request.getState());
        assertEquals("pool-A", msg.getPoolName());
    }

    @Test
    public void shouldChangeStateToNoLocationsForflineFile() throws CacheException {

        whenTapeFile("00000D6AB182056D4A7C97F556D2C55622E3");

        requested();
        fromClient("some-door@some-domain");
        afterExecution();

        assertEquals(NO_LOCATIONS, request.getState());
        assertRescheduled();
    }

    @Test
    public void shouldTriggerStateForOfflineFile() throws CacheException {

        whenTapeFile("00000D6AB182056D4A7C97F556D2C55622E3");
        requested();
        fromClient("some-door@some-domain");
        afterExecution();

        assertEquals(NO_LOCATIONS, request.getState());
        assertRescheduled();

        given(selectedPool.name()).willReturn("hsm-pool-A");
        given(selectedPool.address()).willReturn(new CellAddressCore("hsm-pool-A"));
        given(poolSelector.selectStagePool(any())).willReturn(selectedPool);

        afterExecution();
        assertEquals(NEED_STAGE, request.getState());

        afterExecution();
        assertEquals(WAITING, request.getState());

        verify(cellEndpoint, times(1)).sendMessage(anyObject(), anyVararg());
    }

    @Test
    public void shouldTriggerStateIfCantRead() throws CacheException {

        given(poolSelector.selectReadPool()).willThrow(PermissionDeniedCacheException.class);

        whenDiskFile("00000D6AB182056D4A7C97F556D2C55622E3");
        hasLocation("pool-A");
        requested();
        fromClient("some-door@some-domain");
        afterExecution();

        assertEquals(READ_DENED, request.getState());

    }

    @Test
    public void shouldRetryOnStageComplete() throws CacheException {

        whenTapeFile("00000D6AB182056D4A7C97F556D2C55622E3");
        requested();
        fromClient("some-door@some-domain");
        request.setState(STAGE_COMPLETE);
        afterExecution();

        assertEquals(DONE, request.getState());

        ArgumentCaptor<CellMessage> message = ArgumentCaptor.forClass(CellMessage.class);
        verify(cellEndpoint, times(1)).sendMessage(message.capture(), anyVararg());
        assertEquals(CacheException.OUT_OF_DATE, ((Message)(message.getValue().getMessageObject())).getReturnCode());
    }

    @Test
    public void shouldRetryOnP2PComplete() throws CacheException {

        whenTapeFile("00000D6AB182056D4A7C97F556D2C55622E3");
        requested();
        fromClient("some-door@some-domain");
        request.setState(P2P_COMPLETE);
        afterExecution();

        assertEquals(DONE, request.getState());

        ArgumentCaptor<CellMessage> message = ArgumentCaptor.forClass(CellMessage.class);
        verify(cellEndpoint, times(1)).sendMessage(message.capture(), anyVararg());
        assertEquals(CacheException.OUT_OF_DATE, ((Message) (message.getValue().getMessageObject())).getReturnCode());
    }

    FileAttributes aFile(String id) {
        fileAttributes = new FileAttributes();
        fileAttributes.setPnfsId(new PnfsId(id));
        fileAttributes.setLocations(Collections.emptyList());
        fileAttributes.setStorageInfo(GenericStorageInfo.valueOf("raw:data@dcache", ""));
        fileAttributes.setHsm("s3");
        fileAttributes.setStorageClass("raw:data");
        fileAttributes.setCacheClass("");
        fileAttributes.setSize(1);
        fileAttributes.setChecksums(Collections.emptySet());
        fileAttributes.setFlags(Collections.emptyMap());
        return fileAttributes;
    }

    void whenDiskFile(String id) {
        fileAttributes = aFile(id);
        fileAttributes.setAccessLatency(AccessLatency.ONLINE);
        fileAttributes.setRetentionPolicy(RetentionPolicy.REPLICA);
    }

    void whenTapeFile(String id) {
        fileAttributes = aFile(id);
        fileAttributes.getStorageInfo().addLocation(URI.create("s3://s3/data/objid"));
        fileAttributes.setAccessLatency(AccessLatency.NEARLINE);
        fileAttributes.setRetentionPolicy(RetentionPolicy.CUSTODIAL);
    }

    void hasLocation(String... locations) {
        fileAttributes.setLocations(Arrays.asList(locations));
    }

    private void requested() {
        ProtocolInfo protocolInfo = mock(IpProtocolInfo.class);
        msg = new PoolMgrReplicateFileMsg(fileAttributes, protocolInfo);
        request = new SelectPoolRequest(cellEndpoint, msg, poolMonitor, executorService, waitingRequests);
    }

    private void fromClient(String client) {
        CellMessage m = new CellMessage(new CellPath("PoolManager@local"), msg);
        m.getSourcePath().add(client);
        request.addCellMessage(m);
    }

    private void afterExecution() {
        try {
            request.run();
        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        }
    }

    private void assertRescheduled() {
        assertTrue(activeRequests.contains(request));
    }
}
