package org.dcache.services.billing.recorder.file;

import java.util.Properties;
import org.dcache.services.billing.spi.BillingRecorder;
import org.dcache.services.billing.spi.BillingRecorderProvider;

public class FileRecorderProvider implements BillingRecorderProvider {

    private final String NAME = "file";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public BillingRecorder createRecorder(Properties environment) {
        return new FileRecorder(environment);
    }

}
