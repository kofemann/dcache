package org.dcache.pool.movers;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limit.WindowedLimit;
import com.netflix.concurrency.limits.limiter.AbstractPartitionedLimiter;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

import java.util.concurrent.Callable;
import java.util.function.Function;

public class PoolRateLimiterFactory implements FactoryBean<Limiter>, CellCommandListener {


    private static final Logger LOGGER = LoggerFactory.getLogger(PoolRateLimiterFactory.class);

    private final Limit limit = WindowedLimit.newBuilder()
                    .build(Gradient2Limit.newBuilder()
                            .build());

    private static class ProtocolRateLimiterBuilder extends AbstractPartitionedLimiter.Builder<ProtocolRateLimiterBuilder, String> {
        @Override
        protected ProtocolRateLimiterBuilder self() {
            return this;
        }

        public ProtocolRateLimiterBuilder partitionByProtocol() {
            return partitionResolver(Function.identity());
        }
    }

    @Override
    public Class<?> getObjectType() {
        return Limiter.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public Limiter<String> getObject() throws Exception {

        limit.notifyOnChange(i -> LOGGER.warn("NFS mover IO rate limit changed to {}", i));

        return new ProtocolRateLimiterBuilder()
                .named("per-protocol-rate-limiter")
                .partitionByProtocol()
                .partition("xrootd", 0.4)
                .partition("nfs", 0.6)
                .limit(limit)
                .build();
    }


    @Command(name = "client rate",
            hint = "show nfs IO rate limits",
            description = "Display current IO rate limit")
    public class NfsRateLimitCommand implements Callable<String> {

        @Override
        public String call() {
            return new StringBuilder()
                    .append("Limiter: ").append(limit.getLimit()).append('\n')
                    .toString();
        }
    }
}
