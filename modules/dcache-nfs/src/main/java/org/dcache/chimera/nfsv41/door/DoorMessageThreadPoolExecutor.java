package org.dcache.chimera.nfsv41.door;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author tigran
 */
public class DoorMessageThreadPoolExecutor extends ThreadPoolExecutor {

    public DoorMessageThreadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
                new ThreadFactoryBuilder().setNameFormat("door-message-processor-%d").build());
    }

}
