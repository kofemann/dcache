package org.dcache.services.billing.recorder.file;

import com.google.common.base.CaseFormat;
import com.google.common.io.Files;
import diskCacheV111.cells.DateRenderer;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.StorageInfoMessage;
import diskCacheV111.vehicles.StringTemplateInfoMessageVisitor;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.dcache.services.billing.spi.BillingRecorder;
import org.dcache.util.Slf4jSTErrorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.compiler.STException;

public class FileRecorder implements BillingRecorder {

    private final Logger LOG = LoggerFactory.getLogger(FileRecorder.class);
    public static final String FORMAT_PREFIX = "billing.text.format.";
    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private final static DateTimeFormatter _fileNameFormat
            = DateTimeFormatter.ofPattern("yyyy.MM.dd");

    private final static DateTimeFormatter _directoryNameFormat
            = DateTimeFormatter.ofPattern("yyyy'" + File.separator + "'MM");

    private final STGroup _templateGroup = new STGroup('$', '$');
    private final Map<String, String> _formats = new HashMap<>();

    private File _currentDbFile;
    private final File _logsDir;
    private final boolean _flatTextDir;

    public FileRecorder(Properties environment) {
        _templateGroup.registerRenderer(Date.class, new DateRenderer());
        _templateGroup.setListener(new Slf4jSTErrorListener(LOG));

        environment.stringPropertyNames().forEach((key) -> {
            if (key.startsWith(FORMAT_PREFIX)) {
                String clazz = CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_CAMEL, key.substring(FORMAT_PREFIX.length()));
                _formats.put(clazz, environment.getProperty(key));
            }
        });

        _logsDir = new File(environment.getProperty("billing.text.dir"));
        if (!_logsDir.isDirectory()) {
            throw new IllegalArgumentException("No such directory: " + _logsDir);
        }
        if (!_logsDir.canWrite()) {
            throw new IllegalArgumentException("Directory not writable: " + _logsDir);
        }

        _flatTextDir = Boolean.valueOf(environment.getProperty("billing.text.flat-dir"));
    }

    @Override
    public void record(DoorRequestInfoMessage doorRequestInfo) {
        store(doorRequestInfo);
    }

    @Override
    public void record(MoverInfoMessage moverInfo) {
        store(moverInfo);
    }

    @Override
    public void record(PoolHitInfoMessage poolHitInfo) {
        store(poolHitInfo);
    }

    @Override
    public void record(StorageInfoMessage storeInfo) {
        store(storeInfo);
    }

    private void store(InfoMessage info) {

        String output = getFormattedMessage(info);
        if (output.isEmpty()) {
            return;
        }

        String ext = getFilenameExtension(new Date(info.getTimestamp()));
        logInfo(output, ext);
        if (info.getResultCode() != 0) {
            logError(output, ext);
        }

    }

    private String getFormattedMessage(InfoMessage msg) {
        String format = _formats.get(msg.getClass().getSimpleName());
        if (format == null) {
            return msg.toString();
        } else {
            try {
                ST template = new ST(_templateGroup, format);
                msg.accept(new StringTemplateInfoMessageVisitor(template));
                return template.render();
            } catch (STException e) {
                LOG.error("Unable to render format '{}'. Falling back to internal default.", format);
                return msg.toString();
            }
        }
    }

    private String getFilenameExtension(Date dateOfEvent) {
        if (_flatTextDir) {
            _currentDbFile = _logsDir;
            return _fileNameFormat.format(dateOfEvent.toInstant());
        } else {
            LocalDate now = LocalDate.now();
            _currentDbFile
                    = new File(_logsDir, _directoryNameFormat.format(now));
            if (!_currentDbFile.exists() && !_currentDbFile.mkdirs()) {
                LOG.error("Failed to create directory {}", _currentDbFile);
            }
            return _fileNameFormat.format(now);
        }
    }

    private void logInfo(String output, String ext) {
        File outputFile = new File(_currentDbFile, "billing-" + ext);
        try {
            Files.append(output + "\n", outputFile, UTF8);
        } catch (IOException e) {
            LOG.warn("Can't write billing [{}] : {}", outputFile,
                    e.toString());
        }
    }

    private void logError(String output, String ext) {
        File errorFile = new File(_currentDbFile, "billing-error-" + ext);
        try {
            Files.append(output + "\n", errorFile, UTF8);
        } catch (IOException e) {
            LOG.warn("Can't write billing-error : {}", e.toString());
        }
    }
}
