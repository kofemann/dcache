package org.dcache.auth.gplazma;

import org.dcache.auth.*;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.gplazma.plugins.Ldap2Plugin;
import org.dcache.gplazma.plugins.ldap2utils.Ldap2Exception;
import org.junit.Before;
import org.junit.Test;

import javax.naming.NamingException;
import java.io.IOException;
import java.net.URL;
import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by btwe on 11/27/14.
 */
public class Ldap2PluginTest {

    private Ldap2Plugin plugin_;

    /**
     * create the configuration properties and the plugin instance
     *
     * @throws IOException
     * @throws NamingException
     * @throws Ldap2Exception
     */
    @Before
    public void setup() throws IOException, NamingException, Ldap2Exception {
        Properties testenv = new Properties();
        URL res = Ldap2PluginTest.class.getResource("/jade01.properties");
        testenv.put("gplazma.ldap2.config_file", res.getFile());

        // just put the pointer to the .properties file into the env and go ..
        plugin_ = new Ldap2Plugin(testenv);
    }

    @Test
    public void auth_map_session() throws AuthenticationException {
        HashSet<Object> pubCred = new HashSet<Object>();
        HashSet<Object> privCred = new HashSet<Object>();
        HashSet<Principal> identified = new HashSet<Principal>();
        HashSet<Object> attribs = new HashSet<Object>();

        privCred.add(new PasswordCredential("btwe.ldap", "btwe.ldap.123"));
        this.plugin_.authenticate(pubCred, privCred, identified);
        this.plugin_.map(identified);
        this.plugin_.session(identified, attribs);
        this.plugin_.account(identified);
    }

    @Test(expected = AuthenticationException.class)
    public void simple_authentication_wrongpw() throws AuthenticationException {
        HashSet<Object> pubCred = new HashSet<Object>();
        HashSet<Object> privCred = new HashSet<Object>();
        HashSet<Principal> identified = new HashSet<Principal>();

        privCred.add(new PasswordCredential("btwe.ldap", "wrong.pw"));

        this.plugin_.authenticate(pubCred, privCred, identified);
    }

    @Test(expected = AuthenticationException.class)
    public void simple_authentication_wronguid() throws AuthenticationException {
        HashSet<Object> pubCred = new HashSet<Object>();
        HashSet<Object> privCred = new HashSet<Object>();
        HashSet<Principal> identified = new HashSet<Principal>();

        privCred.add(new PasswordCredential("jiddet.nit", "wrong.pw"));

        this.plugin_.authenticate(pubCred, privCred, identified);
    }

    @Test
    public void identity_map_user() throws NoSuchPrincipalException {
        UserNamePrincipal unp = new UserNamePrincipal("btwe.ldap");
        Principal up = this.plugin_.map(unp);
        if (up instanceof UidPrincipal
                && ((UidPrincipal)up).getUid() == 11010001) {
            return;
        }
        throw new NoSuchPrincipalException("test failed to map uid to uidnumber");
    }

    @Test
    public void identity_reversemap_user() throws NoSuchPrincipalException {
        long uidNubmerLong = 11010001;
        UidPrincipal up = new UidPrincipal(uidNubmerLong);
        Set<Principal> resultPrincipals = this.plugin_.reverseMap(up);
        // get first entry in resultSet
        Principal result = resultPrincipals.iterator().next();
        if( result instanceof UserNamePrincipal &&
                ((UserNamePrincipal)result).getName().equals("btwe.ldap")) {
            return;
        }
        throw new NoSuchPrincipalException("test failed to map uidnumber to uid");
    }

    @Test
    public void identity_map_group() throws NoSuchPrincipalException {
        GroupNamePrincipal gnp = new GroupNamePrincipal("smhb.ldap");
        Principal gp = this.plugin_.map(gnp);
        if (gp instanceof GidPrincipal
                && ((GidPrincipal)gp).getGid() == 11020001 ) {
            return;
        }
        throw new NoSuchPrincipalException("test failed to map gid to gidnumber");
    }

    @Test
    public void identity_reversemap_group() throws NoSuchPrincipalException {
        long gidNubmerLong = 11020001;
        GidPrincipal gp = new GidPrincipal(gidNubmerLong, false);
        Set<Principal> resultPrincipals = this.plugin_.reverseMap(gp);
        // get first entry in resultSet
        Principal result = resultPrincipals.iterator().next();
        if( result instanceof GroupNamePrincipal &&
                ((GroupNamePrincipal)result).getName().equals("smhb.ldap")) {
            return;
        }
        throw new NoSuchPrincipalException("test failed to map gidnumber to gid");
    }

}

