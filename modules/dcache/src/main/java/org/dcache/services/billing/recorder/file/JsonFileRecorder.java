package org.dcache.services.billing.recorder.file;

import com.google.common.io.Files;
import org.json.JSONObject;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PnfsFileInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfoMessage;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.dcache.services.billing.spi.BillingRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonFileRecorder implements BillingRecorder {

    private final Logger LOG = LoggerFactory.getLogger(JsonFileRecorder.class);
    private final static DateTimeFormatter DATE_FORMATER = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.systemDefault());
    private final static String OUT_FILE = "billing.json.file";
    private final static String PRETTY_PRINT = "billing.json.pretty-print";

    private final File outFile;
    private final int indent;

    public JsonFileRecorder(Properties environment) {
        outFile = new File(environment.getProperty(OUT_FILE));
        indent = Boolean.valueOf(environment.getProperty(PRETTY_PRINT)) ? 1 : 0;
    }

    @Override
    public void record(DoorRequestInfoMessage doorRequestInfo) {
        JSONObject json = toJsonString(doorRequestInfo);

        recordIntoFile(json);
    }

    @Override
    public void record(MoverInfoMessage moverInfo) {
        JSONObject json = toJsonString(moverInfo);

        recordIntoFile(json);
    }

    private void recordIntoFile(JSONObject json) {
        try {
            Files.append(json.toString(indent) + "\n", outFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("Failed to write billing record: {}", e.getMessage());
        }
    }

    @Override
    public void record(PoolHitInfoMessage poolHitInfo) {
        JSONObject json = toJsonString(poolHitInfo);

        recordIntoFile(json);
    }

    @Override
    public void record(StorageInfoMessage storeInfo) {
        JSONObject json = toJsonString(storeInfo);

        recordIntoFile(json);
    }

    private JSONObject toJsonString(StorageInfoMessage msg) {
        JSONObject json = newInfoMessageJson(msg);
        json = addPnfsFileInfo(msg, json);

        json.put("transfer_size", msg.getFileSize())
                .put("pool_name", msg.getCellName())
                .put("queue_time", msg.getTimeQueued());

        return json;
    }

    private JSONObject toJsonString(PoolHitInfoMessage msg) {
        JSONObject json = newInfoMessageJson(msg);
        json = addPnfsFileInfo(msg, json);

        InetSocketAddress remoteHost = ((IpProtocolInfo) msg.getProtocolInfo()).getSocketAddress();
        json.put("cached", msg.getFileCached())
                .put("proto", msg.getProtocolInfo().getVersionString())
                .put("remote_host", remoteHost.getAddress().getHostAddress())
                .put("remote_port", remoteHost.getPort());

        return json;
    }

    private JSONObject toJsonString(DoorRequestInfoMessage msg) {
        JSONObject json = newInfoMessageJson(msg);
        json = addPnfsFileInfo(msg, json);

        json.put("transfer_time", msg.getTransactionDuration())
                .put("queue_time", msg.getTimeQueued())
                .put("transfer_path", msg.getTransferPath())
                .put("request_client", msg.getClient());

        return json;
    }

    private JSONObject toJsonString(MoverInfoMessage msg) {
        JSONObject json = newInfoMessageJson(msg);
        json = addPnfsFileInfo(msg, json);

        InetSocketAddress remoteHost = ((IpProtocolInfo) msg.getProtocolInfo()).getSocketAddress();
        json.put("transfer_time", msg.getConnectionTime())
                .put("transfer_size", msg.getDataTransferred())
                .put("initiator", msg.getInitiator())
                .put("pool_name", msg.getCellName())
                .put("is_write", msg.isFileCreated() ? "write" : "read")
                .put("proto", msg.getProtocolInfo().getVersionString())
                .put("remote_host", remoteHost.getAddress().getHostAddress())
                .put("remote_port", remoteHost.getPort())
                .put("p2p", msg.isP2P());

        return json;
    }

    private JSONObject newInfoMessageJson(InfoMessage msg) {
        JSONObject json = new JSONObject();

        json.put("timestamp", DATE_FORMATER.format(Instant.ofEpochMilli(msg.getTimestamp())));
        json.put("bill_type", msg.getMessageType());
        json.put("cell_name", msg.getCellName());
        json.put("cell_type", msg.getCellType());
        json.put("subject", msg.getSubject().getPrincipals().stream().map(Object::toString).toArray());
        json.put("error_code", msg.getResultCode());
        if (msg.getResultCode() != 0) {
            json.put("error_message", msg.getMessage());
        }

        return json;
    }

    private JSONObject addPnfsFileInfo(PnfsFileInfoMessage msg, JSONObject json) {
        json.put("pnfsid", msg.getPnfsId())
                .put("file_path", msg.getBillingPath())
                .put("size", msg.getFileSize());

        StorageInfo si = msg.getStorageInfo();
        if (si != null) {
                json.put("sunit", msg.getStorageInfo().getStorageClass() + "@" + msg.getStorageInfo().getHsm());
        }
        return json;
    }
}
