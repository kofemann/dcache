package org.dcache.gplazma.plugins.ldap2utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Created by btwe on 12/2/14.
 */
public class Ldap2Configuration extends Properties {


    public static final String URL = "url";
    public static final String BASE = "base";
    public static final String AUTHENTICATION = "authentication";
    public static final String BASE_PASSWD = "base_passwd";
    public static final String BASE_GROUP = "base_group";
    public static final String BINDDN = "binddn";
    public static final String BINDPW = "bindpw";
    public static final String MAP_OBJECTCLASSES = "map_objectclasses";
    public static final String MAP_ATTRIBUTES = "map_attributes";
    public static final String DCACHEACCOUNT_ATTRIBUTES = "dcacheaccount_attributes";

    public static final String DCACHEACCOUNT_ROOTDIR = "root";
    public static final String DCACHEACCOUNT_READONLY = "readOnly";


    private static final String[] MANDATORY_KEYS = {
            URL,
            BASE,
            BASE_PASSWD,
            BASE_GROUP,
            MAP_OBJECTCLASSES,
            MAP_ATTRIBUTES,
            DCACHEACCOUNT_ATTRIBUTES,
            } ;

    private static final Logger _log = LoggerFactory.getLogger(Ldap2Configuration.class);

    private Ldap2SchemaMap schemamap_;

    public Ldap2Configuration(String properties_file) throws IOException, Ldap2Exception {
        // first, load default values
        InputStream inp = this.getClass().getResourceAsStream("/Ldap2DefaultMapping.properties");
        if( inp == null){
            throw new Ldap2Exception("Could not find default resources file");
        }
        this.load(new BufferedInputStream(inp));

        // create schemamap from default configuration
        this.schemamap_ = new Ldap2SchemaMap(this);

        // second, load configuration from properties file
        _log.debug(String.format("Loading configuration from %s.", properties_file));
        this.load(new BufferedInputStream(new FileInputStream(properties_file)));

        // run check for mandatory environment entries
        this.verify_configuration();

        // update schemamap from custom configuration
        this.schemamap_.update_map(this);

        // last, convert part of the configuration to comply with JNDI environment
        this.convert_environment_for_jndi();

        _log.debug(String.format("JNDI Environment: %s", this.toString()));
    }

    private void verify_configuration() throws Ldap2Exception {
        for( String mandatory_key: MANDATORY_KEYS) {
            if( this.getProperty(mandatory_key) == null ) {
                throw new Ldap2Exception(
                        String.format("Mandatory configuration key not found in environment. '%s'", mandatory_key) );
            }
        }
    }

    public String mapAttributeName(String rfc2307) {
        String ret = this.schemamap_.getProperty(rfc2307);
        if( ret == null ) {
            return rfc2307;
        } else {
            return ret;
        }
    }

    /**
     * Uses the LdapJndiConfigMap to amp configuration stanzas from
     * property files into jndi environment variables
     */
    private void convert_environment_for_jndi() {
        _log.debug("Converting properties to jndi environment.");
        // we need this
        this.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        // go an translate existing configuration to comply with jndi
        Enumeration<Object> entries = this.keys();
        while(entries.hasMoreElements()) {
            String cur_key = (String) entries.nextElement();
            String cur_val = this.getProperty(cur_key);

            String mapped_key = Ldap2JndiConfigMap.JNDI_KEYMAP.get(cur_key);
            if( mapped_key != null ) {
                this.remove(cur_key);
                this.setProperty(mapped_key, cur_val);
            }
        }
    }
}
