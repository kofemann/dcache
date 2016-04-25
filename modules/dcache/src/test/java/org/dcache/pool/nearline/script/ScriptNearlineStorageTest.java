package org.dcache.pool.nearline.script;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.vehicles.FileAttributes;

import static org.hamcrest.Matchers.hasItemInArray;
import static org.junit.Assert.assertThat;

public class ScriptNearlineStorageTest {

    ScriptNearlineStorage storage;

    @Before
    public void setup() {
        storage = new ScriptNearlineStorage("dcache", "dcache");
        Map propertiesMap = new HashMap<>();
        propertiesMap.put("command", "/path/to/shell/script.sh");
        propertiesMap.put("c:gets", "10");
        propertiesMap.put("c:puts", "30");
        storage.configure(propertiesMap);
    }

    @After
    public void tearDown() {
        storage = null;
    }

    @Test
    public void testGetFlushCommand() {
        FileAttributes fileAttributes = createFileAttributes();
        URI someFile = new File("/some/file/path").toURI();

        assertThat(storage.getFlushCommand(someFile, fileAttributes), hasItemInArray("put"));
        assertThat(storage.getFlushCommand(someFile, fileAttributes), hasItemInArray("-si=size=0;new=true;stored=true;sClass=testStorageClass;cClass=-;hsm=testHsm;accessLatency=NEARLINE;retentionPolicy=CUSTODIAL;dcache://dcache/?store=ttf&group=ard_sinbad&bfid=000084C5FEC6E440422EBB1E0558EB7CF0CC:000019E436CD246146C1A47305309A50DC6E;"));
    }

    @Test
    public void testGetFetchCommand() {
        FileAttributes fileAttributes = createFileAttributes();
        URI someFile = new File("/some/file/path").toURI();

        assertThat(storage.getFetchCommand(someFile, fileAttributes), hasItemInArray("get"));
        assertThat(storage.getFetchCommand(someFile, fileAttributes), hasItemInArray("-si=size=0;new=true;stored=true;sClass=testStorageClass;cClass=-;hsm=testHsm;accessLatency=NEARLINE;retentionPolicy=CUSTODIAL;dcache://dcache/?store=ttf&group=ard_sinbad&bfid=000084C5FEC6E440422EBB1E0558EB7CF0CC:000019E436CD246146C1A47305309A50DC6E;"));
        assertThat(storage.getFetchCommand(someFile, fileAttributes), hasItemInArray("-uri=dcache://dcache/?store=ttf&group=ard_sinbad&bfid=000084C5FEC6E440422EBB1E0558EB7CF0CC:000019E436CD246146C1A47305309A50DC6E"));
    }

    @Test
    public void testRemoveFetchCommand() {
        assertThat(storage.getRemoveCommand(URI.create("proto://some/sub/dir")), hasItemInArray("remove"));
        assertThat(storage.getRemoveCommand(URI.create("proto://some/sub/dir")), hasItemInArray("-uri=proto://some/sub/dir"));
    }

    @Test
    public void testGetChecksumCommand() {
        FileAttributes fileAttributes = createFileAttributes();
        URI someFile = new File("/some/file/path").toURI();

        assertThat(storage.getChecksumCommand(someFile, fileAttributes), hasItemInArray("checksum"));
    }

    private FileAttributes createFileAttributes() {
        FileAttributes fileAttributes = new FileAttributes();
        fileAttributes.setPnfsId(new PnfsId("000019E436CD246146C1A47305309A50DC6E"));
        StorageInfo storageInfo = new GenericStorageInfo("testHsm", "testStorageClass");
        storageInfo.addLocation(URI.create("dcache://dcache/?store=ttf&group=ard_sinbad&bfid=000084C5FEC6E440422EBB1E0558EB7CF0CC:000019E436CD246146C1A47305309A50DC6E"));
        fileAttributes.setStorageInfo(storageInfo);
        return fileAttributes;
    }
}
