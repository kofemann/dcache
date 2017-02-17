package org.dcache.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.zookeeper.ZookeeperDiscoveryProperties;
import com.hazelcast.zookeeper.ZookeeperDiscoveryStrategyFactory;
import org.apache.curator.framework.CuratorFramework;
import org.dcache.cells.CuratorFrameworkAware;

/**
 * Hazelcast cluster's member service.
 */
public class HazelcastService implements CuratorFrameworkAware {

    private CuratorFramework client;

    @Override
    public void setCuratorFramework(CuratorFramework client) {
        this.client = client;
    }

    public void start() {
        client.getZookeeperClient().getCurrentConnectionString();
        String zookeeperURL = client.getZookeeperClient().getCurrentConnectionString();

        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(new ZookeeperDiscoveryStrategyFactory());
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.ZOOKEEPER_URL.key(), zookeeperURL);
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.ZOOKEEPER_PATH.key(), "/dcache/discovery/hazelcast");
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.GROUP.key(), "dcache");


        /*
             * Embedded Hazelcast cluster member initialization
         */
        // we initialize through XML config to pic-up cache configurations
        Config config = new XmlConfigBuilder().build();

        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.setProperty(GroupProperty.DISCOVERY_SPI_ENABLED, "true");
        config.setProperty(GroupProperty.ENABLE_JMX, "true");
        config.setProperty(GroupProperty.ENABLE_JMX_DETAILED, "true");
        config.setProperty(GroupProperty.LOGGING_TYPE, "slf4j");
        config.setLiteMember(false);
        config.setInstanceName("dcache-ns");

        config.getNetworkConfig().getJoin().getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);

        // initialize server
        Hazelcast.getOrCreateHazelcastInstance(config);

        /*
             * Embedded Hazelcast client initialization
         */
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(GroupProperty.DISCOVERY_SPI_ENABLED, "true");
        clientConfig.setProperty(GroupProperty.ENABLE_JMX, "true");
        clientConfig.setProperty(GroupProperty.ENABLE_JMX_DETAILED, "true");
        clientConfig.setProperty(GroupProperty.LOGGING_TYPE, "slf4j");
        clientConfig.setInstanceName("dcache-ns");

        clientConfig.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);

        try {
            // initialize client
            HazelcastClient.newHazelcastClient(clientConfig);
        } catch (DuplicateInstanceNameException e) {
            // client already initialized
            // NOP
        }
    }

    public void shutdown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

}
