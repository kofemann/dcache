package diskCacheV111.vehicles;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

import diskCacheV111.util.PnfsId;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.namespace.FileAttribute.*;

public class PoolMgrGetPoolMsg extends PoolManagerMessage
{
    private static final long serialVersionUID = 8907604668091102254L;

    private final FileAttributes _fileAttributes;

    /**
     * A {@code Collection} of pools that satisfy the query.
     */
    private Collection<Pool> _pools;

    public PoolMgrGetPoolMsg(FileAttributes fileAttributes)
    {
        checkArgument(fileAttributes.isDefined(getRequiredAttributes()), "Required attributes are missing.");

	_fileAttributes = fileAttributes;
	setReplyRequired(true);
    }

    @Nonnull
    public FileAttributes getFileAttributes()
    {
	return _fileAttributes;
    }

    @Nonnull
    public StorageInfo getStorageInfo()
    {
	return _fileAttributes.getStorageInfo();
    }

    @Nonnull
    public PnfsId getPnfsId()
    {
	return _fileAttributes.getPnfsId();
    }

    /**
     * Get a {@code Collection} of pools that satisfy the query.
     * @return collection of pools that satisfy the query.
     */
    public Collection<Pool> getPools()
    {
	return Collections.unmodifiableCollection(_pools);
    }

    @Deprecated
    // for bakward compatibility
    public Pool getPool() {
        return _pools == null ? null : _pools.stream().findAny().orElse(null);
    }

    /**
     * Set a {@code Collection} of pools that satisfy the query.
     * @param pools collection of pools that satisfy the query.
     */
    public void setPools(Collection<Pool> pools)
    {
	_pools = pools;
    }

    @Override
    public String toString()
    {
        if (getReturnCode() == 0) {
            return "PnfsId=" + getPnfsId()
                    + ";StorageInfo=" + getStorageInfo() + ";"
                    + ((_pools == null) ? "" : _pools);
        } else {
            return super.toString();
        }
    }

    public static EnumSet<FileAttribute> getRequiredAttributes()
    {
        return EnumSet.of(PNFSID, STORAGEINFO, STORAGECLASS, HSM);
    }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}
