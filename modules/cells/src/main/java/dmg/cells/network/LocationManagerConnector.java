package dmg.cells.network;

import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Random;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.services.ZooKeeperPaths;
import dmg.util.DummyStreamEngine;
import dmg.util.StreamEngine;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.Args;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.curator.utils.ZKPaths.makePath;

public class LocationManagerConnector
    extends CellAdapter
    implements Runnable
{
    private final static Logger _log =
        LoggerFactory.getLogger("org.dcache.cells.network");

    private final String _domain;
    private final Thread _thread;
    private String _status = "disconnected";
    private int _retries;

    public LocationManagerConnector(String cellName, String args)
    {
        super(cellName, "System", args);

        Args a = getArgs();
        _domain = a.getOpt("domain");
        start();

        _thread = getNucleus().newThread(this, "TunnelConnector");
        _thread.start();
    }

    private synchronized void setStatus(String s)
    {
        _status = s;
    }

    private synchronized String getStatus()
    {
        return _status;
    }

    private String whereIs(String domain)
        throws IOException, InterruptedException
    {
        try {
            return new String(getZkClient().getData().forPath(makePath(ZooKeeperPaths.zkLmLocations, domain)), UTF_8);
        } catch (Exception e) {
            throw new IOException(e.getCause().getMessage(), e);
        }
    }

    private StreamEngine connect(String domain)
        throws IOException, InterruptedException
    {
        setStatus("Locating " + domain);
        HostAndPort location = HostAndPort.fromString(whereIs(domain));

        InetSocketAddress address =
            new InetSocketAddress(location.getHostText(), location.getPort());

        setStatus("Connecting to " + address);
        Socket socket;
        try {
            socket = SocketChannel.open(address).socket();
        } catch (UnsupportedAddressTypeException e) {
            throw new IOException("Unsupported address type: " + address, e);
        } catch (UnresolvedAddressException e) {
            throw new IOException("Unable to resolve " + address, e);
        } catch (IOException e) {
            throw new IOException("Failed to connect to " + address + ": " + e.toString(), e);
        }
        socket.setKeepAlive(true);

        _log.info("Using clear text channel");
        return new DummyStreamEngine(socket);
    }

    @Override
    public void run()
    {
        /* Thread for creating the tunnel. There is a grace period of
         * 4 to 30 seconds between connection attempts. The thread is
         * terminated by interrupting it.
         */
        Args args = getArgs();
        String name = getCellName() + "*";
        Random random = new Random();
        try {
            while (true) {
                try {
                    _retries++;
                    LocationMgrTunnel tunnel =
                        new LocationMgrTunnel(name, connect(_domain), args);
                    _retries = 0;
                    setStatus("Connected");
                    tunnel.join();
                } catch (InterruptedIOException e) {
                    throw e;
                } catch (IOException e) {
                    _log.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.LOCATION_MANAGER_FAILURE,
                                                           name,
                                                           _domain,
                                                           e.getMessage()),
                              "Failed to connect to " + _domain + ": "
                                                      + e.getMessage());
                }

                setStatus("Sleeping");
                long sleep = random.nextInt(26000) + 4000;
                _log.warn("Sleeping " + (sleep / 1000) + " seconds");
                Thread.sleep(sleep);
            }
        } catch (InterruptedIOException | InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String toString()
    {
        return getStatus();
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("Location manager connector : " + getCellName());
        pw.println("Status   : " + getStatus());
        pw.println("Retries  : " + _retries);
    }

    @Override
    public void cleanUp()
    {
        _thread.interrupt();
    }
}
