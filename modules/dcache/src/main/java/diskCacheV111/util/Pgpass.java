//
// $Id: Pgpass.java,v 1.2 2005-08-19 23:45:26 timur Exp $
//

package diskCacheV111.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;

/**
 *
 * @author  Vladimir Podstavkov
 */
public class Pgpass {

    private String _pwdfile;
    private String _hostname;
    private String _port;
    private String _database;
    private String _username;

    public Pgpass(String pwdfile) {
        _pwdfile = pwdfile;
    }

    private String process(String line, String hostname, String port, String database, String username) {
        if (line.charAt(0) != '#') {
//         System.out.println("process: "+line);
            String[] sa = line.split(":");
//         for (int i = 0; i < sa.length; i++) {
//             System.out.print(sa[i]+",");
//         }
//         System.out.println();
            if (sa[0].equals("*") || sa[0].equals(hostname)) {
                if (sa[1].equals("*") || sa[1].equals(port)) {
                    if (sa[2].equals("*") || sa[2].equals(database)) {
                        if (sa[3].equals("*") || sa[3].equals(username)) {
                            return sa[4];
                        }
                    }
                }
            }
        }
        return null;
    }

    private boolean parseUrl(String url) {
        // -jdbcUrl=jdbc:postgresql:database
        // -jdbcUrl=jdbc:postgresql://host/database
        // -jdbcUrl=jdbc:postgresql://host:port/database
        String[] r = url.split("/");
        _hostname = "localhost";
        _port = "5432";
        if (r.length==1) {
            String[] r1 = r[0].split(":");
            _database = r1[r1.length-1];
        } else if (r.length==4) {
            _database = r[r.length-1];
            String[] r1 = r[2].split(":");
            _hostname = r1[0];
            if (r1.length==2) {
                _port = r1[1];
            } else if (r1.length > 2) {
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    public String getPgpass(String hostname, String port, String database, String username) {

        Path path = new File(_pwdfile).toPath();
        FileSystem fs = path.getFileSystem();
        PosixFileAttributeView posixFileAttributeView = fs.provider().getFileAttributeView(path, PosixFileAttributeView.class);

        try {
            PosixFileAttributes posixFileAttributes = posixFileAttributeView.readAttributes();

            for(PosixFilePermission perrmission: posixFileAttributes.permissions()) {
                if (perrmission != PosixFilePermission.OWNER_READ || perrmission != PosixFilePermission.OWNER_WRITE) {
                    System.out.println("Protection for '" + _pwdfile + "' must be '600'");
                    return null;
                }
            }

            /*
             * Here we can read and parse the password file
             */
            String r = null;
            try (BufferedReader in = new BufferedReader(new FileReader(_pwdfile));) {
                do {
                    String line = in.readLine();
                    if (line == null) {
                        break;
                    }
                    r = process(line, hostname, port, database, username);
                } while (r != null);
            }
            return r;
        } catch (IOException ex) {
            System.out.println("Cannot read pgpwd file "+_pwdfile + " : " + ex.getMessage());
        }
        return null;
    }

    public String getPgpass(String url, String username) {
        if (parseUrl(url)) {
            return getPgpass(_hostname, _port, _database, username);
        }
        return null;
    }

    public String getHostname() {
        return _hostname;
    }

    public String getPort() {
        return _port;
    }

    public String getDatabase() {
        return _database;
    }

    public static String getPassword(String file,
                                     String url, String user, String password)
    {
        if (file != null && file.trim().length() > 0) {
            Pgpass pgpass = new Pgpass(file);
            return pgpass.getPgpass(url, user);
        }
        return password;
    }
}
