package org.dcache.chimera.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.zookeeper.ZookeeperDiscoveryProperties;
import com.hazelcast.zookeeper.ZookeeperDiscoveryStrategyFactory;
import org.apache.curator.framework.CuratorFramework;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.zookeeper.CuratorFrameworkAware;
import java.io.InputStream;
import java.net.URL;

/**
 * Hazelcast cluster's member service.
 */
public class HazelcastService implements CuratorFrameworkAware, CellIdentityAware {

    private CuratorFramework client;
    private CellAddressCore address;

    @Override
    public void setCuratorFramework(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        this.address = address;
    }

    public void start() {
        client.getZookeeperClient().getCurrentConnectionString();
        String zookeeperURL = client.getZookeeperClient().getCurrentConnectionString();

        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(new ZookeeperDiscoveryStrategyFactory());
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.ZOOKEEPER_URL.key(), zookeeperURL);
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.ZOOKEEPER_PATH.key(), "/dcache/discovery/hazelcast");
        discoveryStrategyConfig.addProperty(ZookeeperDiscoveryProperties.GROUP.key(), "dcache-namespace");

        URL url = Config.class.getClassLoader().getResource("org/dcache/chimera/hazelcast/hazelcast.xml");
        if (url == null) {
            throw new RuntimeException("Can't find hazelcast configuration");
        }

        InputStream in = Config.class.getClassLoader().getResourceAsStream("org/dcache/chimera/hazelcast/hazelcast.xml");
        if (in == null) {
            throw new RuntimeException("Can't load hazelcast configuration");
        }

        /*
         * Embedded Hazelcast cluster member initialization
         */
        // we initialize through XML config to pic-up cache configurations
        Config config = new XmlConfigBuilder(in).build();
        config.setConfigurationUrl(url); // required to keep hazelcast happy (NPE)
        config.setProperty(GroupProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        config.getNetworkConfig().getJoin().getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.setLiteMember(false);

        // use domain name only to enforce single cache server per JVM
        config.setInstanceName(address.getCellDomainName());

        // initialize server
        Hazelcast.getOrCreateHazelcastInstance(config);

        /*
         * Embedded Hazelcast client initialization
         */
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);
        clientConfig.setInstanceName(address.toString());

        // initialize client
        HazelcastClient.newHazelcastClient(clientConfig);
    }

    public void shutdown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

}