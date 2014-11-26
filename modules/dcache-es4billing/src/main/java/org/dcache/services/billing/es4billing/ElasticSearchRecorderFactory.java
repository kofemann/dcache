/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.services.billing.es4billing;

import java.util.Properties;
import org.dcache.services.billing.spi.BillingRecorderProvider;

public class ElasticSearchRecorderFactory implements BillingRecorderProvider{
    private final String NAME = "elasticsearch";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ElasticSearchRecorder createRecorder(Properties environment) {
        return new ElasticSearchRecorder(environment);
    }
}
