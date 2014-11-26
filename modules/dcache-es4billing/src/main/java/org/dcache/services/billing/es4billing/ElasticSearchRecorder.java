package org.dcache.services.billing.es4billing;

import com.google.common.base.Splitter;
import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PnfsFileInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.StorageInfoMessage;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.text.SimpleDateFormat;
import org.dcache.services.billing.spi.BillingRecorder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.xcontent.XContentFactory;

public class ElasticSearchRecorder implements BillingRecorder {

    private final static String CONFIG_HOSTS_OPTION = "billing.es.hosts";
    private final static String CONFIG_TYPE_OPTION = "billing.es.type";
    private final static String CONFIG_INDEX_OPTION = "billing.es.index";
    private final static String CONFIG_TEMPLATE_OPTION = "billing.es.template";

    private final static int ES_DEFAULT_PORT = 9300;

    private final static String DEFAULT_HOSTS = "localhost";
    private final static String DEFAULT_TYPE = "dcache-billing";
    private final static String DEFAULT_TEMPLATE = "billing";
    private final static String DEFAULT_INDEX = "dcache-billing-%{YYYY.MM.dd}";

    private final SimpleDateFormat indexDateFormat;
    private final Client esClient;
    private final String record_type;
    private final String indexPrefix;

    public ElasticSearchRecorder(Properties properties) {
        String hosts = properties.getProperty(CONFIG_HOSTS_OPTION, DEFAULT_HOSTS);
        String index = properties.getProperty(CONFIG_INDEX_OPTION, DEFAULT_INDEX);
        record_type = properties.getProperty(CONFIG_TYPE_OPTION, DEFAULT_TYPE);
        String template = properties.getProperty(CONFIG_TEMPLATE_OPTION, DEFAULT_TEMPLATE);

        int i = index.indexOf('%');
        if (i == -1 ) {
            indexPrefix = index;
            indexDateFormat = new SimpleDateFormat("");
        } else {
            indexPrefix = index.substring(0, i);
            indexDateFormat = new SimpleDateFormat(index.substring(i+2, index.length()-1));
        }

        esClient = createElasticSearchClient(hosts);
        initTemplate(indexPrefix, template, record_type);
    }

    public static Client createElasticSearchClient(String hosts) {
        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", true)
                .build();

        Splitter splitter = Splitter.on(',')
                .trimResults()
                .omitEmptyStrings();

        TransportClient transportClient = new TransportClient(settings);
        for (String host : splitter.split(hosts)) {
            HostAndPort hostAndPort = HostAndPort.fromString(host);
            transportClient.addTransportAddress(
                    new InetSocketTransportAddress(
                            hostAndPort.getHostText(),
                            hostAndPort.getPortOrDefault(ES_DEFAULT_PORT)
                    )
            );
        }
        return transportClient;
    }

    private void initTemplate(String index, String template, String type) {
        try {

            GetIndexTemplatesResponse response = esClient.admin().indices().prepareGetTemplates(template).get();
            if (response.getIndexTemplates().isEmpty()) {
                esClient.admin().indices().preparePutTemplate(template)
                        .setTemplate(index + "*")
                        .setOrder(0)
                        .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", "5s"))
                        .addMapping(type,
                                XContentFactory.jsonBuilder().startObject().startObject(type).startObject("properties")
                                .startObject("pool_name").field("type", "string").field("index", "not_analyzed").endObject()
                                .startObject("sunit").field("type", "string").field("index", "not_analyzed").endObject()
                                // for bompatibility with logstash parsed data
                                .startObject("pool_name.raw").field("type", "string").field("index", "not_analyzed").endObject()
                                .startObject("sunit.raw").field("type", "string").field("index", "not_analyzed").endObject()
                                .endObject().endObject().endObject())
                        .get();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void record(DoorRequestInfoMessage doorRequestInfo) {
        // NOP
    }

    @Override
    public void record(MoverInfoMessage moverInfo) {

        try {
            XContentBuilder builder = toJsonBuilder(moverInfo, record_type);
            esClient.prepareIndex(getIndex(moverInfo), record_type)
                    .setSource(builder).execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void record(PoolHitInfoMessage poolHitInfo) {
        // NOP
    }

    @Override
    public void record(StorageInfoMessage storeInfo) {
        // NOP
    }

    private String getIndex (InfoMessage msg) {
        return  indexPrefix + indexDateFormat.format(new Date(msg.getTimestamp()));
    }

    private static XContentBuilder toJsonBuilder(MoverInfoMessage msg, String type) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        builder = initInfoMessageFields(builder, msg, type);
        builder = initPnfsFileInfoMessageFields(builder, msg, type);

        InetSocketAddress remoteHost = ((IpProtocolInfo)msg.getProtocolInfo()).getSocketAddress();
        builder.field("transfer_time", msg.getConnectionTime())
                .field("transfer_size", msg.getDataTransferred())
                .field("initiator", msg.getInitiator())
                .field("pool_name", msg.getCellName())
                .field("pool_name.raw", msg.getCellName())
                .field("is_write", msg.isFileCreated() ? "write" : "read")
                .field("proto", msg.getProtocolInfo().getVersionString())
                .field("remote_host", remoteHost.getAddress().getHostAddress())
                .field("remote_port", remoteHost.getPort())
                .field("p2p", msg.isP2P());

        return builder.endObject();
    }

    private static XContentBuilder initPnfsFileInfoMessageFields(XContentBuilder builder, PnfsFileInfoMessage msg, String type) throws IOException {

        builder.field("pnfsid", msg.getPnfsId())
                .field("file_path", msg.getBillingPath())
                .field("size", msg.getFileSize())
                .field("sunit", msg.getStorageInfo().getStorageClass() + "@" + msg.getStorageInfo().getHsm())
                .field("sunit.raw", msg.getStorageInfo().getStorageClass() + "@" + msg.getStorageInfo().getHsm());

        return builder;
    }

    private static XContentBuilder initInfoMessageFields(XContentBuilder builder, InfoMessage msg, String type) throws IOException {

        builder.field("@timestamp", new Date(msg.getTimestamp()))
                .field("@version", 1)
                .field("host", "localhost")
                .field("type", type)
                .field("cellName", msg.getCellName())
                .field("cellType", msg.getCellType())
                .field("error_code", msg.getResultCode());

        XContentBuilder subjectBuilder = builder.startArray("subject");
        for(Principal principal : msg.getSubject().getPrincipals()) {
            subjectBuilder.value(principal.toString());
        }
        subjectBuilder.endArray();

        if (msg.getResultCode() != 0) {
            subjectBuilder.field("error_msg", msg.getMessage());
        }
        return subjectBuilder;
    }
}
