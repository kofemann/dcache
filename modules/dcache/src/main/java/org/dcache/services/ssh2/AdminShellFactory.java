package org.dcache.services.ssh2;

import diskCacheV111.admin.UserAdminShell;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import java.io.File;
import org.apache.sshd.server.channel.ChannelSession;
import org.apache.sshd.server.command.Command;
import org.apache.sshd.server.shell.ShellFactory;
import org.dcache.cells.CellStub;
import org.dcache.util.list.ListDirectoryHandler;
import org.springframework.beans.factory.annotation.Autowired;

public class AdminShellFactory implements ShellFactory, CellMessageSender {

    private CellEndpoint _endpoint;
    private File _historyFile;
    private int _historySize;
    private boolean _useColor;
    private CellStub _pnfsManager;
    private CellStub _poolManager;
    private CellStub _acm;
    private String _prompt;
    private ListDirectoryHandler _list;

    @Autowired
    public void setHistoryFile(File historyFile) {
        _historyFile = historyFile;
    }

    @Autowired
    public void setHistorySize(int size) {
        _historySize = size;
    }

    @Autowired
    public void setUseColor(boolean useColor) {
        _useColor = useColor;
    }

    @Autowired
    public void setPnfsManager(CellStub stub) {
        _pnfsManager = stub;
    }

    @Autowired
    public void setPoolManager(CellStub stub) {
        _poolManager = stub;
    }

    @Autowired
    public void setAcm(CellStub stub) {
        _acm = stub;
    }

    @Autowired
    public void setPrompt(String prompt) {
        _prompt = prompt;
    }

    @Autowired
    public void setListHandler(ListDirectoryHandler list) {
        _list = list;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        _endpoint = endpoint;
    }

    @Override
    public Command createShell(ChannelSession channelSession) {
        return new ShellCommand(_historyFile, _historySize, _useColor, createAdminShell());
    }

    private UserAdminShell createAdminShell() {
        UserAdminShell shell = new UserAdminShell(_prompt);
        shell.setCellEndpoint(_endpoint);
        shell.setPnfsManager(_pnfsManager);
        shell.setPoolManager(_poolManager);
        shell.setAcm(_acm);
        shell.setListHandler(_list);
        return shell;
    }
}
