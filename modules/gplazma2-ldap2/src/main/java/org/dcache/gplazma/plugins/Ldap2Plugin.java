package org.dcache.gplazma.plugins;

import org.dcache.auth.*;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.NoSuchPrincipalException;
import org.dcache.gplazma.plugins.ldap2utils.Ldap2Configuration;
import org.dcache.gplazma.plugins.ldap2utils.Ldap2Exception;
import org.dcache.gplazma.plugins.ldap2utils.Ldap2SchemaMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * gPlazma plug-in
 *
 * Plugin Types:
 *      - Authentication
 *      - Mapping
 *      - Session
 *      - Account
 *      - Identity
 *
 * Features:
 *      - support for multiple LDAP servers for distinct user management domains
 *      - anonymous and authenticated binding
 *      - ldap, ldaps, ldapi
 *      - support for schema extension related to dcache attributes (fs root read-write)
 *      - ...
 *
 * Session:
 *      - conform to rfc2307 NIS in LDAP to find uids, gids, homedirs, ...
 *      - posixAccount
 *      - posixGroup
 *
 * Authentication:
 *      - is performed via binding to ldap (ldap server is then authenticating the user)
 *
 * Mapping:
 *      - defined translation between received principals to dcache principals
 *
 * Identity:
 *      - ...
 *
 * Account:
 *      - ...
 *
 *
 * Configuration:
 *
 *  - Add ldap2 plugin to gplazma2.conf
 *     """
 *     auth sufficient ldap2 conf_file="x/y/z/ldap_site_a.conf"
 *     auth sufficient ldap2 conf_file="x/y/z/ldap_site_b.conf"
 *     ...
 *     """
 *
 *  - conf_file:
 *
 *   url:
 *          Space separated string of urls pointing to ldap servers
 *   authentication:
 *          String:  "simple", "sasl", "ssl"
 *   basedn:
 *          String, dn of the entry with read access to the rfc2307 tree
 *          If missing, anonymous access is assumed
 *   basepw:
 *          String, contains the password to bind with
 *   base:
 *          String, dn of the base of the queried ldap database
 *   base_passwd:
 *          String, dn of the base, where the posixAccount table is
 *   base_group:
 *          String, dn of the base, where the groupAccount table is
 *   dcacheaccount_attributes:
 *          String,
 *              Option 0: comma separated entries with default values
 *                  fsroot=/,root=/,read-write=read=write
 *              Option 1: "ldap"
 *                  values for dcache attributes will be fetched from ldap entry
 *                  see map_attributes
 *
 *   map_objectclasses:
 *           String of comma separated entries.
 *           One entry defines a mapping:    RFC2307Key=MappedKey
 *           Default: posixAccount=posixAccount,groupAccount=groupAccount
 *           Example: posixAcconut=x-site-dcacheAccount
 *   map_attributes:
 *           same as map_objectclass, but for attributes
 *           Attributes are extended to support dcache specific attributes:
 *           x-dcache-banned, x-dcache-root, x-dcache-readAccess
 *
 *           Example: homedirectory=x-mysite-dachehome, x-dcache-readOnly=x-mysite-dcacheReadOnly
 *
 e
 * See all defaults in resources: Ldap2DefaultMapping.properties
 *
 *
 * @since 2.11.1
 * @author b.tweddell@fz-juelich.de (Jülich Supercomputing Centre)
 */
public class Ldap2Plugin implements
        GPlazmaAuthenticationPlugin,
        GPlazmaSessionPlugin,
        GPlazmaAccountPlugin,
        GPlazmaMappingPlugin,
        GPlazmaIdentityPlugin {

    private static final Logger _log = LoggerFactory.getLogger(Ldap2Plugin.class);

    private static final String config_file_ = "gplazma.ldap2.config_file";

    private InitialLdapContext ldapctx_;

    /**
     * Create a LDAP2 identity plugin.
     */

    protected Ldap2Configuration configuration_;

    public Ldap2Plugin(Properties dcache_env) throws Ldap2Exception, NamingException, IOException {
        _log.info("Loading Ldap2 plugin");
        // check for configuration file (properties files)
        if (!dcache_env.containsKey(this.config_file_)) {
            throw new Ldap2Exception(
                    String.format("Configuration failure: %s not found in environment. Should be placed in gplazma.conf",
                            this.config_file_));
        }

        // fetch and build configuration
        this.configuration_ = new Ldap2Configuration((String) dcache_env.get(this.config_file_));

        // finally create the context
        this.ldapctx_ = new InitialLdapContext(this.configuration_, null);

    }


    protected NamingEnumeration<SearchResult> getGidEntries_byGidnumber(Long gidnumber) throws Ldap2Exception {
        NamingEnumeration<SearchResult> ret = null;
        // fetch posixAccout entry for uid with a gid
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        String searchUidFilter = String.format(
                "(&(objectclass=%s)(%s=%d))",
                this.configuration_.mapAttributeName(Ldap2SchemaMap.POSIXGROUP),
                this.configuration_.mapAttributeName(Ldap2SchemaMap.GIDNUMBER),
                gidnumber
        );
        String searchBasePasswd = this.configuration_.getProperty(Ldap2Configuration.BASE_GROUP);
        try {
            _log.debug(String.format("Fetching entry: base=%s, filter=%s",
                    searchBasePasswd,
                    searchUidFilter));
            NamingEnumeration<SearchResult> answers = this.ldapctx_.search(
                    searchBasePasswd,
                    searchUidFilter,
                    searchControls
            );
            ret = answers;
        } catch (NamingException e) {
            throw new Ldap2Exception(e);
        }

        return ret;

    }
    protected Attributes getGidEntry_byCn(String cn) throws Ldap2Exception {
        Attributes ret = null;
        // fetch posixAccout entry for uid with a gid
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        String searchUidFilter = String.format(
                "(&(objectclass=%s)(%s=%s))",
                this.configuration_.mapAttributeName(Ldap2SchemaMap.POSIXGROUP),
                this.configuration_.mapAttributeName(Ldap2SchemaMap.CN),
                cn
        );
        String searchBasePasswd = this.configuration_.getProperty(Ldap2Configuration.BASE_GROUP);
        try {
            _log.debug(String.format("Fetching entry: base=%s, filter=%s",
                    searchBasePasswd,
                    searchUidFilter));
            NamingEnumeration<SearchResult> answers = this.ldapctx_.search(
                    searchBasePasswd,
                    searchUidFilter,
                    searchControls
            );
            ret = answers.next().getAttributes();
        } catch (NamingException e) {
            throw new Ldap2Exception(e);
        }

        return ret;

    }


    protected NamingEnumeration<SearchResult> getUidEntries_byUidnumber(Long uidnumber) throws Ldap2Exception {
        NamingEnumeration<SearchResult> ret = null;
        // fetch posixAccount entry
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        String searchUidFilter = String.format(
                "(&(objectclass=%s)(%s=%d))",
                this.configuration_.mapAttributeName(Ldap2SchemaMap.POSIXACCOUNT),
                this.configuration_.mapAttributeName(Ldap2SchemaMap.UIDNUMBER),
                uidnumber
        );
        String searchBasePasswd = this.configuration_.getProperty(Ldap2Configuration.BASE_PASSWD);
        try {
            // find posixaccount entry for uid and add uid and gid to the principals set
            _log.debug(String.format("Fetching entry: base=%s, filter=%s",
                    searchBasePasswd,
                    searchUidFilter));
            NamingEnumeration<SearchResult> answers = this.ldapctx_.search(
                    searchBasePasswd,
                    searchUidFilter,
                    searchControls
            );
            ret = answers;
        } catch (NamingException e) {
            throw new Ldap2Exception(e);
        }

        return ret;
    }


    protected Attributes getUidEntry_byUid(String uid) throws Ldap2Exception {
        Attributes ret = null;
        // fetch posixAccount entry for uid with a gid
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        String searchUidFilter = String.format(
                "(&(objectclass=%s)(%s=%s))",
                this.configuration_.mapAttributeName(Ldap2SchemaMap.POSIXACCOUNT),
                this.configuration_.mapAttributeName(Ldap2SchemaMap.UID),
                uid
        );
        String searchBasePasswd = this.configuration_.getProperty(Ldap2Configuration.BASE_PASSWD);
        NamingEnumeration<SearchResult> answers = null;
        try {
            // find posixaccount entry for uid and add uid and gid to the principals set
            _log.debug(String.format("Searching entry: base=%s, filter=%s",
                    searchBasePasswd,
                    searchUidFilter));
            answers = this.ldapctx_.search(
                    searchBasePasswd,
                    searchUidFilter,
                    searchControls
            );
        } catch (NamingException e) {
            throw new Ldap2Exception(e);
        }
        // be sure to have at least one answer
        // hasMore throws NamingException if enumeration is empty
        try {
            SearchResult firstAnswer;
            if (  answers.hasMore() ) {
                firstAnswer = answers.next();
            } else {
                throw new Ldap2Exception("uid not found in ldap db");
            }
            ret = firstAnswer.getAttributes();
        } catch (NamingException e) {
            e.printStackTrace();
        }

        return ret;
    }

    protected NamingEnumeration<SearchResult> getGidEntries_byMemberUid(String memberUid) throws Ldap2Exception {
        // search for posixGroup where the uid is listed in a memberUid attribute
        SearchControls searchControls = new SearchControls();
        String searchGidFilter = String.format(
                "(&(objectclass=%s)(%s=%s))",
                this.configuration_.mapAttributeName(Ldap2SchemaMap.POSIXGROUP),
                this.configuration_.mapAttributeName(Ldap2SchemaMap.MEMBERUID),
                memberUid
        );
        String searchBaseGroup = this.configuration_.getProperty(Ldap2Configuration.BASE_GROUP);
        _log.debug(String.format("Fetching entry: base=%s, filter=%s",
                searchBaseGroup,
                searchGidFilter));

        NamingEnumeration<SearchResult> answers;
        try {
            answers = this.ldapctx_.search(
                    searchBaseGroup,
                    searchGidFilter,
                    searchControls
            );
        } catch (NamingException e) {
            throw new Ldap2Exception(e);
        }

        return answers;
    }



    @Override
    public void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals) throws AuthenticationException {
        // in order to authenticate a user a bind operation is performed
        // thus the ldap server decides about an successful login

        _log.debug(String.format(
                "Try to authenticate: pub='%s', pirv='%s', identified='%s'",
                publicCredentials.toString(),
                privateCredentials.toString(),
                identifiedPrincipals.toString()
        ));

        // make a local copy of the ldap2_env in order to change the credentials for the connect+bind operation
        Properties intermediate_env_ = new Properties();
        intermediate_env_.putAll(this.configuration_);

        // get username and password from privateCredentials
        PasswordCredential cred = null;
        for(Object entry : privateCredentials) {
            if(entry instanceof PasswordCredential) {
                cred = (PasswordCredential) entry;
                // just take care about the first one found
                break;
            }
        }
        if(cred == null) {
            throw new AuthenticationException("no password credential found");
        }

        // build fqdn of user like this: ${map_attribute_uid}=${username},${base_group}
        String fqdn_of_user = String.format("%s=%s,%s",
                this.configuration_.mapAttributeName(Ldap2SchemaMap.UID),
                cred.getUsername(),
                this.configuration_.getProperty(Ldap2Configuration.BASE_PASSWD));

        intermediate_env_.put(Context.SECURITY_PRINCIPAL, fqdn_of_user);
        intermediate_env_.put(Context.SECURITY_CREDENTIALS, cred.getPassword());

        _log.debug(String.format("Authenticating with env: %s", intermediate_env_.toString()));

        try {
            LdapContext ctx = new InitialLdapContext(intermediate_env_, null);
            ctx.close();
        } catch (NamingException e) {
            throw new AuthenticationException(e);
        }

        // if we reach this line, authentication is granted by ldap server,
        // otherwise NamingException would have been raised before
        // add Principal to the return value
        identifiedPrincipals.add(new UserNamePrincipal(cred.getUsername()));
    }


    @Override
    public void session(Set<Principal> authorizedPrincipals, Set<Object> attrib) throws AuthenticationException {
        _log.debug(String.format("Start session for principals %s", authorizedPrincipals.toString()));

        // get first entry in principal set, which is a UserNamePrincipal
        Principal usernamePrincipal = null;
        for (Principal principal : authorizedPrincipals ) {
            if (principal instanceof UserNamePrincipal) {
                usernamePrincipal = principal;
                break;
            }
        }
        if(usernamePrincipal == null) {
            throw new AuthenticationException("no username principal found");
        }
        _log.debug(String.format("Creating session for username %s...", usernamePrincipal.toString()));

        // fetch entry for uid from ldap
        Attributes uidEntry = null;
        try {
            uidEntry = this.getUidEntry_byUid(usernamePrincipal.getName());
        } catch (Ldap2Exception e) {
            throw new AuthenticationException(
                    String.format("username %s not found in ldap database",  usernamePrincipal.getName()));
        }
        // read attr homeDirectory from entry
        HomeDirectory homedir = null;
        Attribute homedirectory_attr = null;
        homedirectory_attr = uidEntry.get(this.configuration_.mapAttributeName(Ldap2SchemaMap.HOMEDIRECTORY));
        if(homedirectory_attr == null) {
            throw new AuthenticationException(
                    String.format("Entry for username %s does not contain attribute %s",
                            usernamePrincipal.getName(),
                            this.configuration_.mapAttributeName(Ldap2SchemaMap.HOMEDIRECTORY)) );
        }
        try {
            homedir = new HomeDirectory((String) homedirectory_attr.get());
        } catch (NamingException e) {
            throw new AuthenticationException(e);
        }

        // try to configure RootDirectory and ReadOnly for this usernamePrincipal
        RootDirectory rootdir = null;
        ReadOnly readonly = null;

        if( this.configuration_.getProperty(Ldap2Configuration.DCACHEACCOUNT_ATTRIBUTES).equalsIgnoreCase("ldap") ) {
            _log.debug("Try to fetch additional attributes (root,readonly) from ldap");

            Attribute rootdir_attr = uidEntry.get(this.configuration_.mapAttributeName(Ldap2SchemaMap.X_DCACHE_ROOTDIR));
            if( rootdir_attr != null ) {
                // we got an attribute resp rootdir
                try {
                    rootdir = new RootDirectory((String)rootdir_attr.get());
                } catch (NamingException e) {
                    throw new AuthenticationException( String.format("Attribute %s from entry for %s contains no value",
                            this.configuration_.getProperty(Ldap2SchemaMap.X_DCACHE_ROOTDIR),
                            usernamePrincipal.getName() ));
                }
            }

            Attribute readonly_attr = uidEntry.get(this.configuration_.mapAttributeName(Ldap2SchemaMap.X_DCACHE_READONLY));
            if ( readonly_attr != null ) {
                // we got an attribute resp readonly
                try {
                    readonly = new ReadOnly((String)readonly_attr.get());
                } catch (NamingException e) {
                    throw new AuthenticationException( String.format("Attribute %s from entry for %s contains no value",
                            this.configuration_.mapAttributeName(Ldap2SchemaMap.X_DCACHE_READONLY),
                            usernamePrincipal.getName() ));
                }
            }

        } else {
            // fetch default values from env
            //dcacheaccount_attributes = root=/,readOnly=false,banned=false
            String default_attributes_str = this.configuration_.getProperty(Ldap2Configuration.DCACHEACCOUNT_ATTRIBUTES);
            if (default_attributes_str == null) {
                throw new AuthenticationException(String.format("Configuration variable not found: %s",
                        Ldap2Configuration.DCACHEACCOUNT_ATTRIBUTES));
            }
            String[] default_attributes = default_attributes_str.split(",");
            for (String cur_conf : default_attributes) {
                String[] kv = cur_conf.split("=");
                if (kv[0].startsWith(Ldap2Configuration.DCACHEACCOUNT_ROOTDIR)) {
                    rootdir = new RootDirectory(kv[1]);
                } else if (kv[0].startsWith(Ldap2Configuration.DCACHEACCOUNT_READONLY)) {
                    readonly = new ReadOnly(kv[1]);
                }
            }
        }

        attrib.add(homedir);
        attrib.add(rootdir);
        attrib.add(readonly);

        _log.debug(String.format("Session contains the following attributes: %s", attrib.toString()));
    }


    @Override
    public void map(Set<Principal> principals) throws AuthenticationException {
        _log.debug(String.format("Start mapping of principals %s", principals.toString()));
        // get all username principals from arg
        Principal usernamePrincipal = null;
        for (Principal principal : principals) {
            if (principal instanceof UserNamePrincipal) {
                usernamePrincipal = principal;
                // break here, because only one username principal is expected
                break;
            }
        }
        if(usernamePrincipal == null) {
            throw new AuthenticationException("no username principal found");
        }
        _log.debug(String.format("Create mapping for username %s...", usernamePrincipal.toString()));

        // fetch posixAccout entry for uid with a gid
        try {
            Attributes attributes = this.getUidEntry_byUid(usernamePrincipal.getName());

            Attribute uidnumber = attributes.get(this.configuration_.mapAttributeName(Ldap2SchemaMap.UIDNUMBER));
            if( uidnumber != null ) {
                UidPrincipal addup = new UidPrincipal(Long.parseLong((String) uidnumber.get()));
                _log.debug(String.format("Adding uidprincipal %s to map", addup.toString()));
                principals.add(addup);
            }

            Attribute gidnumber = attributes.get(this.configuration_.mapAttributeName(Ldap2SchemaMap.GIDNUMBER));
            if( gidnumber != null ) {
                GidPrincipal addgp = new GidPrincipal(Long.parseLong((String) gidnumber.get()), true);
                _log.debug(String.format("Adding gidprincipal %s to map", addgp.toString()));
                principals.add(addgp);
            }

        } catch (Ldap2Exception e) {
            throw new AuthenticationException(e);
        } catch (NamingException e) {
            throw new AuthenticationException(e);
        }

        // fetch all secondary groups with affiliation to uid
        NamingEnumeration<SearchResult> answers = null;
        try {
            answers = this.getGidEntries_byMemberUid(usernamePrincipal.getName());
            // iterate over all groups found and add gidnumber to principals
            while ( answers.hasMore() ) {
                Attributes attributes = answers.next().getAttributes();
                Attribute gidnumber = attributes.get(this.configuration_.mapAttributeName(Ldap2SchemaMap.GIDNUMBER));
                if (gidnumber != null) {
                    GidPrincipal addgp = new GidPrincipal(Long.parseLong((String) gidnumber.get()), false);
                    _log.debug(String.format("Adding gidprincipal %s to map", addgp.toString()));
                    principals.add(addgp);
                }
            }
        } catch (Ldap2Exception e1) {
            throw new AuthenticationException(e1);
        } catch (NamingException e) {
            throw new AuthenticationException(e);
        }

        _log.debug(String.format("identified principals after mapping are %s", principals.toString() ));
    }


    @Override
    public void account(Set<Principal> authorizedPrincipals) throws AuthenticationException {
         _log.debug(String.format("Start account for principals %s", authorizedPrincipals.toString()));
        // get first entry in principal set, which is a UserNamePrincipal
        Principal usernamePrincipal = null;
        for (Principal principal : authorizedPrincipals ) {
            if (principal instanceof UserNamePrincipal) {
                usernamePrincipal = principal;
                break;
            }
        }
        if(usernamePrincipal == null) {
            throw new AuthenticationException("no username principal found");
        }
        _log.debug(String.format("Create account for username %s...", usernamePrincipal.toString()));

        // fetch entry for uid from ldap
        Attributes uidEntry = null;
        try {
            uidEntry = this.getUidEntry_byUid(usernamePrincipal.getName());
        } catch (Ldap2Exception e) {
            throw new AuthenticationException(
                    String.format("username %s not found in ldap database",  usernamePrincipal.getName()));
        }
        // read attr banned from entry
        Attribute banned_attr = null;
        banned_attr = uidEntry.get(this.configuration_.mapAttributeName(Ldap2SchemaMap.X_DCACHE_BANNED));
        if(banned_attr == null) {
            _log.info(String.format("Ldap entry for username %s does not contain attribute %s. Assuming no banning.",
                    usernamePrincipal,
                    this.configuration_.getProperty(Ldap2SchemaMap.X_DCACHE_BANNED)));
        } else {
            try {
                String banned_str = (String) banned_attr.get();
                if (Boolean.parseBoolean(banned_str)) {
                    throw new AuthenticationException(String.format("Username %s is banned", usernamePrincipal.getName()));
                }
            } catch (NamingException e) {
            }
        }
    }


    @Override
    public Principal map(Principal principal) throws NoSuchPrincipalException {
        String name = principal.getName();
        try {
            if (principal instanceof UserNamePrincipal) {
                _log.debug("Trying to map uid {}", name);
                Attributes userAttr = this.getUidEntry_byUid(name);
                Attribute uidNumberAttr = userAttr.get(this.configuration_.mapAttributeName(Ldap2SchemaMap.UIDNUMBER));
                String uidnumber = (String) uidNumberAttr.get();
                UidPrincipal up = new UidPrincipal(uidnumber);
                return up;
            } else if (principal instanceof GroupNamePrincipal) {
                _log.debug("Trying to map gid {}", name);
                Attributes groupAttr = this.getGidEntry_byCn(name);
                Attribute gidNumberAttr = groupAttr.get(this.configuration_.mapAttributeName(Ldap2SchemaMap.GIDNUMBER));
                String gidnumber = (String) gidNumberAttr.get();
                GidPrincipal gp = new GidPrincipal(gidnumber, false);
                return gp;
            }
        } catch (NamingException e) {
            _log.debug("Failed to get mapping: {}", e.toString());
        } catch (Ldap2Exception e) {
            _log.debug("Failed to get mapping: {}", e.toString());
        }
        throw new NoSuchPrincipalException(principal);
    }

    @Override
    public Set<Principal> reverseMap(Principal principal) throws NoSuchPrincipalException {
        Set<Principal> ret = new HashSet<>();
        try {
            if (principal instanceof GidPrincipal) {
                long gidNumber = ((GidPrincipal)principal).getGid();
                _log.debug("Trying to reverse map gidnumber {}", gidNumber);
                NamingEnumeration<SearchResult> ne = this.getGidEntries_byGidnumber(gidNumber);
                while(ne.hasMore()) {
                    SearchResult result = ne.next();
                    Attributes groupAttrs = result.getAttributes();
                    Attribute gidAttr = groupAttrs.get(
                            this.configuration_.mapAttributeName(Ldap2SchemaMap.CN)
                    );
                    String adderName = (String)gidAttr.get();
                    Principal adder = new GroupNamePrincipal(adderName);
                    ret.add(adder);
                }
            } else if (principal instanceof UidPrincipal) {
                long uidNumber = ((UidPrincipal)principal).getUid();
                _log.debug("Trying to reverse map uidnumber {}", uidNumber);
                NamingEnumeration<SearchResult> ne = this.getUidEntries_byUidnumber(uidNumber);
                while(ne.hasMore()) {
                    SearchResult result = ne.next();
                    Attributes userAttrs = result.getAttributes();
                    Attribute uidAttr = userAttrs.get(
                            this.configuration_.mapAttributeName(Ldap2SchemaMap.UID)
                    );
                    String adderName = (String)uidAttr.get();
                    Principal adder = new UserNamePrincipal(adderName);
                    ret.add(adder);
                }
            }
            return ret;
        } catch (NamingException | Ldap2Exception e) {
            throw new NoSuchPrincipalException(e.getMessage());
        }
    }

}
