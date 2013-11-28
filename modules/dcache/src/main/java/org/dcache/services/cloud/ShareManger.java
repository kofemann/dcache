package org.dcache.services.cloud;

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;

import diskCacheV111.util.PnfsHandler;
import dmg.cells.nucleus.DelayedReply;
import dmg.util.Args;

import java.io.File;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Callable;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellCommandListener;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.springframework.beans.factory.annotation.Required;

public class ShareManger implements CellCommandListener {

    private static final Set<FileAttribute> REQUIRED_ATTRS
            = EnumSet.of(FileAttribute.PNFSID);

    private String _shareBase;
    private PnfsHandler _pnfs;
    private ListDirectoryHandler _listDirectoryHandler;

    @Required
    public void setShareBase(String shareBase) {
        _shareBase = shareBase;
    }

    @Required
    public void setPnfs(PnfsHandler pnfs) {
        _pnfs = pnfs;
    }

    @Required
    public void setListDirectoryHandler(ListDirectoryHandler listDirectoryHandler) {
        _listDirectoryHandler = listDirectoryHandler;
    }

    public final static String hh_share_$_1 = " share <path> # make a public link for a file";
    public DelayedReply ac_share_$_1(Args args) {
        final String path = args.argv(0);
        CommandCall command = new CommandCall(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return share(path);
            }
        });
        command.execute();
        return command;
    }

    public final static String hh_unshare_$_1 = " unshare <path> # remove the public link for a file if exists";
    public DelayedReply ac_unshare_$_1(Args args) {
        final String path = args.argv(0);

        CommandCall command = new CommandCall( new Callable<String>() {
            @Override
            public String call() throws Exception {
                return unshare(path);
            }
        });
        command.execute();

        return command;
    }

    public final static String hh_get_share_$_1 = " get share <path> # get the public link for a file if exists";
    public DelayedReply ac_get_share_$_1(Args args) {
        final String path = args.argv(0);

        CommandCall command = new CommandCall(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return getShare(path);
            }
        });
        command.execute();

        return command;
    }

    public String getShare(String path) throws CacheException, InterruptedException {
        String publicLink;
        FileAttributes fileAttributes;

        fileAttributes = _pnfs.getFileAttributes(path, REQUIRED_ATTRS);
        String fileId = fileAttributes.getPnfsId().toString();

        final String linkBase = _shareBase + '/' + fileId;
        try {
            _pnfs.getFileAttributes(linkBase, EnumSet.noneOf(FileAttribute.class));
            try (DirectoryStream directoryStream = _listDirectoryHandler.
                    list(Subjects.ROOT, new FsPath(linkBase), null, Range.<Integer>all())) {
                String fname = directoryStream.iterator().next().getName();
                publicLink = linkBase + "/" + fname;
            }

        } catch (FileNotFoundCacheException e) {
            return null;
        }

        return publicLink;
    }

    public String share(String path) throws CacheException, InterruptedException {
        String publicLink;
        FileAttributes fileAttributes;

        fileAttributes = _pnfs.getFileAttributes(path, REQUIRED_ATTRS);
        String fileId = fileAttributes.getPnfsId().toString();

        final String linkBase = _shareBase + '/' + fileId;
        try {
            _pnfs.createDirectories(new FsPath(linkBase));
            File f = new File(path);
            String fname = f.getName();
            publicLink = linkBase + "/" + fname;
            _pnfs.createLink(publicLink, path, 0, 0, 0644);

        } catch (FileExistsCacheException e) {
            try (DirectoryStream directoryStream = _listDirectoryHandler.
                    list(Subjects.ROOT, new FsPath(linkBase), null, Range.<Integer>all())) {
                String fname = directoryStream.iterator().next().getName();
                publicLink = linkBase + "/" + fname;
            }
        }

        return publicLink;
    }

    public String unshare(String path) throws CacheException, InterruptedException {

        FileAttributes fileAttributes;

        fileAttributes = _pnfs.getFileAttributes(path, REQUIRED_ATTRS);
        String fileId = fileAttributes.getPnfsId().toString();

        final String linkBase = _shareBase + '/' + fileId;

        try (DirectoryStream directoryStream = _listDirectoryHandler.
                list(Subjects.ROOT, new FsPath(linkBase), null, Range.<Integer>all())) {
            for(DirectoryEntry e: directoryStream) {
                String fname = e.getName();
                String publicLink = linkBase + "/" + fname;
                _pnfs.deletePnfsEntry(publicLink);
            }
            _pnfs.deletePnfsEntry(linkBase);
        }

        return "";
    }

    private class CommandCall extends DelayedReply implements Runnable {

        private final Callable<String> call;

        public CommandCall(Callable<String> call) {
            this.call = call;
        }

        @Override
        public void run() {
            try {
                reply(call.call());
            } catch (Exception e) {
                reply(e);
            }
        }

        public void execute() {
             new Thread(this, "CommandCall").start();
        }
    }
}
