package org.dcache.services.billing.spi;

import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.StorageInfoMessage;

/**
 * Service provider interface for message recorder.
 *
 * An implementation of this interface
 * get called to record billing record.
 */
public interface BillingRecorder {

    /**
     * Record {@code DoorRequestInfoMessage} message.
     * @param doorRequestInfo to record
     */
    void record(DoorRequestInfoMessage doorRequestInfo);

    /**
     * Record {@code MoverInfoMessage} message.
     * @param moverInfo to record
     */
    void record(MoverInfoMessage moverInfo);

    /**
     * Record {@code PoolHitInfoMessage} message.
     * @param poolHitInfo to record
     */
    void record(PoolHitInfoMessage poolHitInfo);

    /**
     * Record {@code StorageInfoMessage} message.
     * @param storeInfo to record
     */
    void record(StorageInfoMessage storeInfo);
}
