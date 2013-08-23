package org.dcache.vehicles;

import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;

/**
 * 
 */
public class PoolModifyFileMessage extends PoolIoFileMessage {

    public PoolModifyFileMessage(String pool, ProtocolInfo protocolInfo, FileAttributes fileAttributes) {
        super(pool, protocolInfo, fileAttributes);
    }

}
