package org.dcache.namespace;

import javax.security.auth.Subject;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import dmg.util.CollectionFactory;

import org.dcache.acl.enums.AccessType;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.acl.enums.AccessType.*;

/**
 * PermissionHandler which delegates calls to a chain of permission
 * handler. For each policy decision, the permission handler delegates
 * the call to each chained permission handler until the first one
 * which returns a result different from ACCESS_UNDEFINED. That result
 * will be returned. If all chained permission handlers return
 * ACCESS_UNDEFINED, then this permission handler also returns
 * ACCESS_UNDEFINED.
 */
public class ChainedPermissionHandler implements PermissionHandler
{
    private final List<PermissionHandler> _chain =
        CollectionFactory.newArrayList();

    public ChainedPermissionHandler()
    {
    }

    public ChainedPermissionHandler(List<PermissionHandler> chain)
    {
        _chain.addAll(chain);
    }

    public ChainedPermissionHandler(PermissionHandler ... chain)
    {
        _chain.addAll(Arrays.asList(chain));
    }

    public void setChain(List<PermissionHandler> chain)
    {
        _chain.clear();
        _chain.addAll(chain);
    }

    @Override
    public Set<FileAttribute> getRequiredAttributes()
    {
        Set<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
        for (PermissionHandler handler: _chain) {
            attributes.addAll(handler.getRequiredAttributes());
        }
        return attributes;
    }

    @Override
    public AccessType canReadFile(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canReadFile(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canWriteFile(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canWriteFile(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canCreateSubDir(Subject subject, FileAttributes parentAttr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canCreateSubDir(subject, parentAttr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canCreateFile(Subject subject, FileAttributes parentAttr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canCreateFile(subject, parentAttr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canDeleteFile(Subject subject,
                                    FileAttributes parentAttr,
                                    FileAttributes childAttr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canDeleteFile(subject, parentAttr,
                                                   childAttr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canDeleteDir(Subject subject,
                                   FileAttributes parentAttr,
                                   FileAttributes childAttr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canDeleteDir(subject, parentAttr,
                                                  childAttr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canRename(Subject subject,
                                FileAttributes existingParentAttr,
                                FileAttributes newParentAttr,
                                boolean isDirectory)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canRename(subject, existingParentAttr,
                                               newParentAttr, isDirectory);
            if (res != null && res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canListDir(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canListDir(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canLookup(Subject subject, FileAttributes attr)
    {
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canLookup(subject, attr);
            if (res != null && res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return AccessType.ACCESS_UNDEFINED;
    }

    private AccessType canSetAttribute(Subject subject,
                                       FileAttributes attrs,
                                       FileAttribute attribute)
    {
        Set<FileAttribute> set = Collections.singleton(attribute);
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canSetAttributes(subject,
                                                      attrs,
                                                      set);
            if (res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return ACCESS_UNDEFINED;
    }

    private AccessType canGetAttribute(Subject subject,
                                       FileAttributes attrs,
                                       FileAttribute attribute)
    {
        Set<FileAttribute> set = Collections.singleton(attribute);
        for (PermissionHandler handler: _chain) {
            AccessType res = handler.canGetAttributes(subject,
                                                      attrs,
                                                      set);
            if (res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canSetAttributes(Subject subject,
                                       FileAttributes attrs,
                                       Set<FileAttribute> attributes)
    {
        for (FileAttribute attribute: attributes) {
            AccessType res =
                canSetAttribute(subject, attrs, attribute);
            if (res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return ACCESS_UNDEFINED;
    }

    @Override
    public AccessType canGetAttributes(Subject subject,
                                       FileAttributes attrs,
                                       Set<FileAttribute> attributes)
    {
        for (FileAttribute attribute: attributes) {
            AccessType res =
                canGetAttribute(subject, attrs, attribute);
            if (res != AccessType.ACCESS_UNDEFINED) {
                return res;
            }
        }
        return ACCESS_UNDEFINED;
    }
}
