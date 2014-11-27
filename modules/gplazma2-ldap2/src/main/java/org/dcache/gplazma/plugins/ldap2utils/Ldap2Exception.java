package org.dcache.gplazma.plugins.ldap2utils;

/**
 * Created by btwe on 12/2/14.
 */
public class Ldap2Exception extends Exception {
    public Ldap2Exception(String message) {
       super(message);
    }
    public Ldap2Exception(Throwable cause) {
        super(cause);
    }
}
