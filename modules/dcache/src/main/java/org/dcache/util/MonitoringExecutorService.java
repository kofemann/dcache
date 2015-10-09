package org.dcache.util;

import com.google.common.util.concurrent.ForwardingExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An executor service which logs time spent in queue and/or execution.
 *
 * @since 2.14
 */
public class MonitoringExecutorService extends ForwardingExecutorService {

    private final static Logger _log = LoggerFactory.getLogger(MonitoringExecutorService.class);
    private final ExecutorService inner;
    private final long maxQueueTime;
    private final BlockingQueue<Runnable> queue;
    private final int maxQueueSize;

    /**
     * Create new QueueTimeMonitoringExecutorService which will delegate all
     * method calls to another ExecutorService and log execution and queuing
     * times if specified duration is excided.
     *
     * @param inner backing delegate instance.
     * @param maxQueueTime duration in milliseconds.
     */
    public MonitoringExecutorService(ExecutorService inner, long maxQueueTime,
            BlockingQueue<Runnable> queue, int maxQueueSize) {
        this.inner = inner;
        this.maxQueueTime = maxQueueTime;
        this.queue = queue;
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    protected ExecutorService delegate() {
        return inner;
    }

    @Override
    public void execute(final Runnable command) {
        final long submitionTime = System.currentTimeMillis();
        final Runnable task = () -> {
            final long startTime = System.currentTimeMillis();
            final long queueTime = startTime - submitionTime;
            try {
                command.run();
            }finally {
                final long finishingTime = System.currentTimeMillis();
                final long executionDuration = finishingTime - startTime ;
                final long totalTime = finishingTime - submitionTime;
                if (totalTime > maxQueueTime) {
                    _log.warn("Slow task {} : queue: {} ms, execution: {} ms, total: {} ms",
                            command, queueTime, executionDuration, totalTime);
                }
            }
        };
        super.execute(task);
        int queueSize = queue.size();
        if (queueSize > maxQueueSize) {
            _log.warn("Queue size excided - current: {}, max: {}",
                    queueSize, maxQueueSize);
        }
    }
}
