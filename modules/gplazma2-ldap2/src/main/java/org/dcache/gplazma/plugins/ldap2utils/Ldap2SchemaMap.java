package org.dcache.gplazma.plugins.ldap2utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by btwe on 12/2/14.
 */
public class Ldap2SchemaMap extends Properties {

    private static final Logger _log = LoggerFactory.getLogger(Ldap2SchemaMap.class);

    public static final String POSIXACCOUNT="posixAccount";
    public static final String POSIXGROUP="posixGroup";
    public static final String UID="uid";
    public static final String GID="gid";
    public static final String CN="cn";
    public static final String UIDNUMBER="uidNumber";
    public static final String GIDNUMBER="gidNumber";
    public static final String HOMEDIRECTORY="homedirectory";
    public static final String MEMBERUID="memberUid";
    public static final String X_DCACHEACCOUNT="x-dcacheAccount";
    public static final String X_DCACHE_ROOTDIR ="x-dcache-root";
    public static final String X_DCACHE_READONLY ="x-dcache-readOnly";
    public static final String X_DCACHE_BANNED="x-dcache-banned";

    public Ldap2SchemaMap(Properties config) throws Ldap2Exception {
        this.update_map(config);
    }

    private void process(Properties config, String env_key ) throws Ldap2Exception {
        String env_value = config.getProperty(env_key);
        if( env_value == null ) {
            throw new Ldap2Exception(
                    String.format("key '%s' not found in environment.", env_key));
        }

        String[] mappings = env_value.split(",");
        for( String cur_maping: mappings ) {
            String[] s = cur_maping.split("=");
            _log.debug(String.format("Applying to schema map, '%s' => '%s'",
                    s[0], s[1]));
            this.put(s[0], s[1]);
        }

    }

    public void update_map(Properties config) throws Ldap2Exception {
        this.process(config, Ldap2Configuration.MAP_OBJECTCLASSES);
        this.process(config, Ldap2Configuration.MAP_ATTRIBUTES);
        _log.debug("rfc2307+schema extension map: " + this.toString());
    }
}
