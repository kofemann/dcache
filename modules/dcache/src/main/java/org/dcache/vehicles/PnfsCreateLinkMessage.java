package org.dcache.vehicles;

import diskCacheV111.vehicles.PnfsCreateEntryMessage;

/**
 * A message to create a symbolic link
 */
public class PnfsCreateLinkMessage extends PnfsCreateEntryMessage {

    private final String _destination;
    public PnfsCreateLinkMessage(String path, String dest, int uid, int gid, int mode) {
        super(path, uid, gid, mode);
        _destination = dest;
    }

    public String getDestination() {
        return _destination;
    }
}
