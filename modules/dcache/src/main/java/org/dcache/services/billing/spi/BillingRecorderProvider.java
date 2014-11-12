package org.dcache.services.billing.spi;

import java.util.Properties;

/**
 * A factory to create {@code BillingReceiver}. The implemented classes will be
 * loaded with Service Provider Interface.
 */
public interface BillingRecorderProvider {

    /**
     * The unique name identifying this provider.
     *
     * The name is used by dCache admins to configure desired providers.
     *
     * @return provider name.
     */
    String getName();

    /**
     * Create {@code BillingReceiver} for this provider.
     * @param properties dCache configuration properties
     * @return new instance of {@code BillingReceiver}
     */
    BillingRecorder createRecorder(Properties properties);
}
