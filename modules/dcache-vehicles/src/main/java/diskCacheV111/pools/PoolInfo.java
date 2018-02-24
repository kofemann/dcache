package diskCacheV111.pools;

import java.util.Collection;

/**
 *
 */
public interface PoolInfo {

    PoolCostInfo.NamedPoolQueueInfo getMoverQueue(String name);

    double moverCostFactor();

    Collection<PoolCostInfo.NamedPoolQueueInfo> movers();

    String name();

    PoolCostInfo.PoolQueueInfo p2PClient();

    PoolCostInfo.PoolQueueInfo p2pServer();

    PoolCostInfo.PoolQueueInfo restore();

    PoolCostInfo.PoolSpaceInfo space();

    PoolCostInfo.PoolQueueInfo store();

}
