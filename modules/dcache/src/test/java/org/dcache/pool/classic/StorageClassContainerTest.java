package org.dcache.pool.classic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.chimera.InodeId;
import org.dcache.namespace.FileType;
import org.dcache.pool.nearline.HsmSet;
import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;
import org.junit.Before;
import org.junit.Test;

public class StorageClassContainerTest {

    // fake clock to monotonically increase creation times
    private final AtomicLong birth = new AtomicLong();

    // NOTE: hsm type and name must be lowercase!
    public static final String HSM_A = "hsm-a";
    public static final String HSM_B = "hsm-b";

    private StorageClassContainer scc;

    private NearlineStorageHandler nearlineStorageHandler;
    private NearlineStorage nearlineStorageA;
    private NearlineStorage nearlineStorageB;

    @Before
    public void setUp() throws Exception {

        HsmSet hsmSet = mock(HsmSet.class);

        nearlineStorageA = mock(NearlineStorage.class);
        when(hsmSet.getNearlineStorageByName(eq(HSM_A))).thenReturn(nearlineStorageA);
        when(hsmSet.getNearlineStorageByType(eq(HSM_A))).thenReturn(nearlineStorageA);

        nearlineStorageB = mock(NearlineStorage.class);
        when(hsmSet.getNearlineStorageByName(eq(HSM_B))).thenReturn(nearlineStorageB);
        when(hsmSet.getNearlineStorageByType(eq(HSM_B))).thenReturn(nearlineStorageB);

        nearlineStorageHandler = new NearlineStorageHandler();
        nearlineStorageHandler.setHsmSet(hsmSet);
        scc = new StorageClassContainer();
        scc.setNearlineStorageHandler(nearlineStorageHandler);
    }

    @Test
    public void testTemplatePropagation() throws CacheException {

        var template1 = scc.defineStorageClass("osm1", "*");
        template1.setOpen(false);
        template1.setExpiration(TimeUnit.SECONDS.toMillis(1));
        template1.setMaxSize(2);
        template1.setPending(3);

        var template2 = scc.defineStorageClass("osm2", "*");
        template2.setOpen(true);
        template2.setExpiration(TimeUnit.SECONDS.toMillis(4));
        template2.setMaxSize(5);
        template2.setPending(6);

        var ce1 = givenCacheEntry("osm1", "test:tape");
        scc.addCacheEntry(ce1);

        var ce2 = givenCacheEntry("osm2", "test:tape");
        scc.addCacheEntry(ce2);

        StorageClassInfo sci1 = scc.getStorageClassInfo("osm1", "test:tape");
        assertMatchTemplate(template1, sci1);

        StorageClassInfo sci2 = scc.getStorageClassInfo("osm2", "test:tape");
        assertMatchTemplate(template2, sci2);

    }

    @Test
    public void testAddOne() throws CacheException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        assertThat(scc.getStorageClassCount(), is(1));
    }

    @Test
    public void testAddMultipleSameClass() throws CacheException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        assertThat(scc.getStorageClassCount(), is(1));
    }

    @Test
    public void testAddMultipleDifferentClass() throws CacheException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        scc.addCacheEntry(givenCacheEntry(HSM_B, "a:b"));
        assertThat(scc.getStorageClassCount(), is(2));
    }

    @Test
    public void testAddMultipleDifferentHSM() throws CacheException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        scc.addCacheEntry(givenCacheEntry(HSM_B, "a:b"));
        assertThat(scc.getStorageClassCount(), is(2));
    }

    @Test

    public void testFistInFifo() throws CacheException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        scc.addCacheEntry(givenCacheEntry(HSM_B, "a:b"));
        scc.flushAll(1, 1_000, MoverRequestScheduler.Order.FIFO);
        verify(nearlineStorageA).flush(any());
    }

    @Test
    public void testLastInLifo() throws CacheException {

        scc.addCacheEntry(givenCacheEntry(HSM_A, "a:b"));
        scc.addCacheEntry(givenCacheEntry(HSM_B, "a:b"));
        scc.flushAll(1, 1_000, MoverRequestScheduler.Order.LIFO);
        verify(nearlineStorageB).flush(any());
    }

    private void assertMatchTemplate(StorageClassInfo template, StorageClassInfo actual) {
        assertEquals(template.getExpiration(), actual.getExpiration());
        assertEquals(template.getMaxSize(), actual.getMaxSize());
        assertEquals(template.getPending(), actual.getPending());
        assertEquals(template.isOpen(), actual.isOpen());
    }


    private CacheEntry givenCacheEntry(String hsm, String storageClass) {

        long ts = birth.incrementAndGet();
        var fileAttributes = FileAttributes.of()
              .fileType(FileType.REGULAR)
              .size(ThreadLocalRandom.current().nextLong(1_000_000))
              .storageClass(storageClass)
              .hsm(hsm)
              .pnfsId(InodeId.newID(0))
              .creationTime(ts)
              .accessTime(ts)
              .modificationTime(ts)
              .build();

        return new CacheEntry() {

            @Override
            public PnfsId getPnfsId() {
                return fileAttributes.getPnfsId();
            }

            @Override
            public long getReplicaSize() {
                return fileAttributes.getSize();
            }

            @Override
            public FileAttributes getFileAttributes() {
                return fileAttributes;
            }

            @Override
            public ReplicaState getState() {
                return ReplicaState.PRECIOUS;
            }

            @Override
            public long getCreationTime() {
                return fileAttributes.getCreationTime();
            }

            @Override
            public long getLastAccessTime() {
                return fileAttributes.getAccessTime();
            }

            @Override
            public int getLinkCount() {
                return 1;
            }

            @Override
            public boolean isSticky() {
                return false;
            }

            @Override
            public Collection<StickyRecord> getStickyRecords() {
                return Collections.emptySet();
            }
        };
    }
}