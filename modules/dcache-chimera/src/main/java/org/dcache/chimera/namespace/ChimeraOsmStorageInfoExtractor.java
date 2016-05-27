package org.dcache.chimera.namespace;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.OSMStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.StorageGenericLocation;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;


public class ChimeraOsmStorageInfoExtractor extends ChimeraHsmStorageInfoExtractor {

    private final LoadingCache<ExtendedInode, StorageInfo> siCache = CacheBuilder
            .newBuilder()
            .expireAfterWrite(3, TimeUnit.MINUTES)
            .maximumSize(1024)
            .build(
                new CacheLoader<ExtendedInode, StorageInfo>() {
                    @Override
                    public StorageInfo load(ExtendedInode k) throws Exception {
                        return getDirStorageInfo(k);
                    }
            });

    public ChimeraOsmStorageInfoExtractor(AccessLatency defaultAL,
                                          RetentionPolicy defaultRP) {
        super(defaultAL,defaultRP);
    }

    @Override
    public StorageInfo getFileStorageInfo(ExtendedInode inode) throws CacheException {

        OSMStorageInfo info;

        try {
            Stat stat = inode.statCache();
            ExtendedInode level2 = inode.getLevel(2);

            boolean isNew = (stat.getSize() == 0) && (!level2.exists());

            if (!isNew) {
                ImmutableList<String> locations = inode.getLocations(StorageGenericLocation.TAPE);

                if (locations.isEmpty()) {
                    info = (OSMStorageInfo) siCache.get(inode);
                } else {
                    InodeStorageInformation inodeStorageInfo = inode.getStorageInfo();

                    info = new OSMStorageInfo(inodeStorageInfo.storageGroup(),
                            inodeStorageInfo.storageSubGroup());

                    for (String location : locations) {
                        try {
                            info.addLocation(new URI(location));
                        } catch (URISyntaxException e) {
                            // bad URI
                        }
                    }
                }
            } else {
                info = (OSMStorageInfo) siCache.get(inode);
            }

            info.setIsNew(isNew);

        } catch (ExecutionException e) {
            Throwables.propagateIfPossible(e, CacheException.class);
            throw new CacheException("Failed to fetch strage info", e);
        } catch (ChimeraFsException e) {
            throw new CacheException(e.getMessage());
        }
        return info;
    }

    @Override
    public StorageInfo getDirStorageInfo(ExtendedInode inode) throws CacheException {
        ExtendedInode dirInode;
        if (!inode.isDirectory()) {
            dirInode = inode.getParent();
        }
        else {
            dirInode = inode;
        }
        HashMap<String, String> hash = new HashMap<>();
        String store = null;
        ImmutableList<String> OSMTemplate = dirInode.getTag("OSMTemplate");
        if (!OSMTemplate.isEmpty()) {
            for (String line: OSMTemplate) {
                StringTokenizer st = new StringTokenizer(line);
                if (st.countTokens() < 2) {
                    continue;
                }
                hash.put(st.nextToken().intern(), st.nextToken());
            }
            store = hash.get("StoreName");
            if (store == null) {
                throw new CacheException(37, "StoreName not found in template");
            }
        }

        ImmutableList<String> sGroup = dirInode.getTag("sGroup");
        String group = getFirstLine(sGroup).transform(String::intern).orNull();
        OSMStorageInfo info = new OSMStorageInfo(store, group);
        info.addKeys(hash);
        return info;
    }

}
