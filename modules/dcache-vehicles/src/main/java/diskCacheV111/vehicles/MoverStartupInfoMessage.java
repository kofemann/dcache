package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAddressCore;

import java.net.InetSocketAddress;
import java.util.Optional;

public class MoverStartupInfoMessage extends PnfsFileInfoMessage {

    private ProtocolInfo _protocolInfo;
    private boolean _fileCreated;
    private String _initiator = "<undefined>";
    private boolean _isP2p;

    private static final long serialVersionUID = -16358687982035237L;
    private String _transferPath;

    public MoverStartupInfoMessage(CellAddressCore address, PnfsId pnfsId) {
        super("transferStart", "pool", address, pnfsId);
    }

    public void setFileCreated(boolean created) {
        _fileCreated = created;
    }

    public void setInitiator(String transaction) {
        _initiator = transaction;
    }

    public void setP2P(boolean isP2p) {
        _isP2p = isP2p;
    }

    public String getInitiator() {
        return _initiator;
    }

    public boolean isFileCreated() {
        return _fileCreated;
    }

    public boolean isP2P() {
        return _isP2p;
    }

    public ProtocolInfo getProtocolInfo() {
        return _protocolInfo;
    }

    public String getTransferPath() {
        return _transferPath != null ? _transferPath : getBillingPath();
    }

    public void setTransferPath(String path) {
        _transferPath = path;
    }

    public void setProtocolInfo(ProtocolInfo protocolInfo) {
        _protocolInfo = protocolInfo;
    }

    @Override
    public String toString() {
        return "MoverInfoMessage{" +
              ", protocolInfo=" + _protocolInfo +
              ", fileCreated=" + _fileCreated +
              ", initiator='" + _initiator + '\'' +
              ", isP2p=" + _isP2p +
              ", transferPath='" + _transferPath + '\'' +
              "} " + super.toString();
    }

    @Override
    public void accept(InfoMessageVisitor visitor) {
        visitor.visit(this);
    }
}
