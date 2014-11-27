package org.dcache.gplazma.plugins.ldap2utils;

import javax.naming.Context;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by btwe on 12/2/14.
 */
public class Ldap2JndiConfigMap extends HashMap<String, String> {


    public static final Map<String, String> JNDI_KEYMAP;

    static {
        JNDI_KEYMAP = new HashMap<String, String>();
        JNDI_KEYMAP.put("url", Context.PROVIDER_URL);
        JNDI_KEYMAP.put("binddn", Context.SECURITY_PRINCIPAL);
        JNDI_KEYMAP.put("bindpw", Context.SECURITY_CREDENTIALS);
        JNDI_KEYMAP.put("authentication", Context.SECURITY_AUTHENTICATION);
    }
}
