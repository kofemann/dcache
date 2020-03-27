package org.dcache.namespace;

import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsMessage;
import org.dcache.vehicles.FileAttributes;

import java.io.Serializable;
import java.util.Map;

public class PnfsTopDirectoriesMessage extends PnfsMessage implements Serializable {

    Map<String, FileAttributes> topDirs;

    public PnfsTopDirectoriesMessage() {
        super();
        super.setReplyRequired(true);
    }
    public Map<String, FileAttributes> getTopDirs() {
        return topDirs;
    }

    public void setTopDirs(Map<String, FileAttributes> topDirs) {
        this.topDirs = topDirs;
    }
}
