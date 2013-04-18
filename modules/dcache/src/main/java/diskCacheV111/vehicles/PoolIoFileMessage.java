package diskCacheV111.vehicles;

import com.google.common.base.Optional;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.EnumSet;

import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.io.Serializable;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;

public class PoolIoFileMessage extends PoolMessage {

    private FileAttributes _fileAttributes;

    @Deprecated // Remove in 2.7
    private StorageInfo  _storageInfo;

    private ProtocolInfo _protocolInfo;

    @Deprecated // Remove in 2.7
    private PnfsId       _pnfsId;

    private boolean      _isPool2Pool;
    private String       _ioQueueName;
    private int          _moverId;
    private String       _initiator = "<undefined>";
    private boolean      _forceSourceMode;
    private Optional<? extends Serializable> _payload;

    private static final long serialVersionUID = -6549886547049510754L;

    public PoolIoFileMessage( String pool ,
                              ProtocolInfo protocolInfo ,
                              FileAttributes fileAttributes   ){
       super( pool ) ;

        checkNotNull(fileAttributes);
        checkArgument(fileAttributes.isDefined(
                EnumSet.of(STORAGEINFO, PNFSID)));

       _fileAttributes = fileAttributes;
       _storageInfo  = StorageInfos.extractFrom(fileAttributes);
       _protocolInfo = protocolInfo ;
       _pnfsId       = fileAttributes.getPnfsId();
       _payload = Optional.absent();
    }

    public PoolIoFileMessage( String pool ,
                              PnfsId pnfsId ,
                              ProtocolInfo protocolInfo  ){
       super( pool ) ;
       _protocolInfo = protocolInfo ;
       _pnfsId       = pnfsId ;
        _fileAttributes = new FileAttributes();
        _fileAttributes.setPnfsId(pnfsId);
        _payload = Optional.absent();
    }
    public PnfsId       getPnfsId(){ return _fileAttributes.getPnfsId(); }
    public ProtocolInfo getProtocolInfo(){ return _protocolInfo ; }

    public boolean isPool2Pool(){ return _isPool2Pool ; }
    public void setPool2Pool(){ _isPool2Pool = true ; }

    public void setIoQueueName( String ioQueueName ){
       _ioQueueName = ioQueueName ;
    }
    public String getIoQueueName(){
       return _ioQueueName ;
    }
    /**
     * Getter for property moverId.
     * @return Value of property moverId.
     */
    public int getMoverId() {
        return _moverId;
    }

    /**
     * Setter for property moverId.
     * @param moverId New value of property moverId.
     */
    public void setMoverId(int moverId) {
        this._moverId = moverId;
    }


    public void setInitiator(String initiator) {
        _initiator = initiator;
    }

    public String getInitiator() {
        return _initiator;
    }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    public void setAttachment(Optional<? extends Serializable> attachement) {
        _payload = attachement;
    }

    public Optional<? extends Serializable> getAttachemt() {
        return _payload;
    }

    private void readObject(ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_fileAttributes == null) {
            _fileAttributes = new FileAttributes();
            if (_storageInfo != null) {
                StorageInfos.injectInto(_storageInfo, _fileAttributes);
            }
            _fileAttributes.setPnfsId(_pnfsId);
        }
        if(_payload == null) {
            _payload = Optional.absent();
        }
    }

    public void setForceSourceMode(boolean forceSourceMode)
    {
        _forceSourceMode = forceSourceMode;
    }

    public boolean isForceSourceMode()
    {
        return _forceSourceMode;
    }
}
