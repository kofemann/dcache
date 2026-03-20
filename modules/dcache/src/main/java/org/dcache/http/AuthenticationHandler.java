package org.dcache.http;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static java.util.Arrays.asList;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.DesiredRole;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.util.CertificateFactories;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.security.SPNEGOUserPrincipal;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationHandler extends Handler.Wrapper {

    private final static Logger LOG = LoggerFactory.getLogger(AuthenticationHandler.class);
    public static final String X509_CERTIFICATE_ATTRIBUTE =
          "javax.servlet.request.X509Certificate";
    public static final String DCACHE_SUBJECT_ATTRIBUTE =
          "org.dcache.subject";
    public static final String DCACHE_RESTRICTION_ATTRIBUTE =
          "org.dcache.restriction";
    public static final String DCACHE_LOGIN_ATTRIBUTES =
          "org.dcache.login";
    private static final String AUTH_HANDLER_ATTRIBUTE = "org.dcache.authentication-handler";
    private static final String X509_SUBJECT_ATTRIBUTE = "org.dcache.x509-subject";
    public static final String BEARER_TOKEN_QUERY_KEY = "authz";

    private static final InetAddress UNKNOWN_ADDRESS = InetAddresses.forString("0.0.0.0");

    private String _realm;
    private Restriction _doorRestriction;
    private boolean _isBasicAuthenticationEnabled;
    private boolean _isSpnegoAuthenticationEnabled;
    private LoginStrategy _loginStrategy;
    private boolean _acceptBearerTokenUnencrypted;

    private CertificateFactory _cf = CertificateFactories.newX509CertificateFactory();

    public static Set<LoginAttribute> getLoginAttributes(HttpServletRequest request) {
        return (Set<LoginAttribute>) request.getAttribute(DCACHE_LOGIN_ATTRIBUTES);
    }

    public static Set<LoginAttribute> getLoginAttributes(Request request) {
        return (Set<LoginAttribute>) request.getAttribute(DCACHE_LOGIN_ATTRIBUTES);
    }
    /**
     * Provide the identity of the user, based solely on their X.509 certificate (if supplied).  If
     * the user was already authenticated based on their X.509 certificate then this method simply
     * returned that existing Subject, otherwise the X.509 certificate chain is authenticated and
     * that result returned.
     *
     * @param request the HTTP request to process
     * @return a Subject based on the users X.509 certificate or null if the user provided no X.509
     * certificate
     * @throws PermissionDeniedCacheException if the user could not be logged in
     * @throws CacheException                 some other problem with the X.509-based login
     * @throws IllegalArgumentException       X.509 authentication is not supported
     */
    public static Subject getX509Identity(Request request)
          throws CacheException {
        Subject dCacheUser = Subject.getSubject(AccessController.getContext());
        if (dCacheUser != null && Subjects.getDn(dCacheUser) != null) {
            return dCacheUser;
        }

        if (!(request.getAttribute(X509_CERTIFICATE_ATTRIBUTE) instanceof X509Certificate[])) {
            return null;
        }

        Object existingX509Subject = request.getAttribute(X509_SUBJECT_ATTRIBUTE);
        if (existingX509Subject instanceof Subject) {
            return (Subject) existingX509Subject;
        }

        AuthenticationHandler handler = (AuthenticationHandler) request.getAttribute(
              AUTH_HANDLER_ATTRIBUTE);
        Subject x509Subject = handler.x509Login(request);
        request.setAttribute(X509_SUBJECT_ATTRIBUTE, x509Subject);
        return x509Subject;
    }

    private Subject x509Login(Request request) throws CacheException {
        Subject suppliedIdentity = new Subject();
        addX509ChainToSubject(request, suppliedIdentity);
        addOriginToSubject(request, suppliedIdentity);
        return _loginStrategy.login(suppliedIdentity).getSubject();
    }

    @Override
    public boolean handle(Request request, Response servletResponse, Callback callback)
          throws IOException, ServletException {

            AuthHandlerResponse response = new AuthHandlerResponse(servletResponse, request);
            try {
                Subject suppliedIdentity = new Subject();
                addX509ChainToSubject(request, suppliedIdentity);
                addOriginToSubject(request, suppliedIdentity);
                addAuthCredentialsToSubject(request, suppliedIdentity);
                addSpnegoCredentialsToSubject(request, suppliedIdentity);
                addQueryBearerTokenToSubject(request, suppliedIdentity);
                addDesiredRolesToSubject(request, suppliedIdentity);

                LoginReply login = _loginStrategy.login(suppliedIdentity);
                Subject authnIdentity = login.getSubject();
                Restriction restriction = Restrictions.concat(_doorRestriction,
                      login.getRestriction());

                request.setAttribute(DCACHE_SUBJECT_ATTRIBUTE, authnIdentity);
                request.setAttribute(DCACHE_RESTRICTION_ATTRIBUTE, restriction);
                request.setAttribute(DCACHE_LOGIN_ATTRIBUTES, login.getLoginAttributes());
                request.setAttribute(AUTH_HANDLER_ATTRIBUTE, this);

                /* Process the request as the authenticated user.*/
                Exception problem = Subject.doAs(authnIdentity,
                      (PrivilegedAction<Exception>) () -> {
                          try {
                              AuthenticationHandler.super.handle(request, response, callback);
                          } catch (Exception e) {
                              return e;
                          }
                          return null;
                      });
                if (problem != null) {
                    Throwables.throwIfInstanceOf(problem, IOException.class);
                    Throwables.throwIfInstanceOf(problem, ServletException.class);
                    throw new RuntimeException(problem);
                }
            } catch (PermissionDeniedCacheException e) {
                LOG.info("Login failed for {} on {}: {}", request.getMethod(),
                      request.getHttpURI().getPath(), e.getMessage());
                Response.writeError(request, response, callback, HttpServletResponse.SC_UNAUTHORIZED);
                return true;
            } catch (CacheException e) {
                LOG.error("Internal server error: {}", e.getMessage());
                Response.writeError(request, response, callback, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                return true;
            }
        return false;
    }

    private void addSpnegoCredentialsToSubject(Request request, Subject subject) {
        if (_isSpnegoAuthenticationEnabled) {
            var spnegoAuth = Request.getAuthenticationState(request).getUserPrincipal();
            if (spnegoAuth instanceof SPNEGOUserPrincipal sup) {
                subject.getPrincipals().add(new KerberosPrincipal(sup.getName()));
            }
        }
    }

    private void addX509ChainToSubject(Request request, Subject subject)
          throws CacheException {
        Object object = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        if (object instanceof X509Certificate[]) {
            try {
                subject.getPublicCredentials()
                      .add(_cf.generateCertPath(asList((X509Certificate[]) object)));
            } catch (CertificateException e) {
                throw new CacheException(
                      "Failed to generate X.509 certificate path: " + e.getMessage(), e);
            }
        }
    }

    private void addQueryBearerTokenToSubject(Request request, Subject subject)
          throws PermissionDeniedCacheException {
        List<String> bearerTokens = Request.extractQueryParameters(request).getValues(BEARER_TOKEN_QUERY_KEY);
        if (bearerTokens != null) {
            if (!_acceptBearerTokenUnencrypted && !request.isSecure()) {
                throw new PermissionDeniedCacheException(
                      "not allowed to send pre-authorized URL unencrypted");
            }
            Set<Object> credentials = subject.getPrivateCredentials();
            bearerTokens.stream()
                  .map(BearerTokenCredential::new)
                  .forEach(credentials::add);
        }
    }

    private void addXForwardForAddresses(ImmutableList.Builder<InetAddress> addresses,
          Request request) {
        String xff = nullToEmpty(request.getHeaders().get(HttpHeader.X_FORWARDED_FOR));
        List<String> ids = newArrayList(
              Splitter.on(',').trimResults().omitEmptyStrings().split(xff));
        reverse(ids).stream().
              map(id -> {
                  try {
                      return InetAddresses.forString(id);
                  } catch (IllegalArgumentException e) {
                      LOG.warn("Fail to parse \"{}\" in X-Forwarded-For " +
                            "header \"{}\": {}", id, xff, e.getMessage());
                      return UNKNOWN_ADDRESS;
                  }
              }).
              forEach(addresses::add);
    }

    private void addOriginToSubject(Request request, Subject subject) {
        ImmutableList.Builder<InetAddress> addresses = ImmutableList.builder();

        String address = Request.getRemoteAddr(request);
        try {
            addresses.add(InetAddress.getByName(address));
        } catch (UnknownHostException e) {
            LOG.warn("Failed to resolve {}: {}", address, e.getMessage());
            return;
        }

        // REVISIT: although RFC 7239 specifies a more powerful format, it
        // is currently not widely used; whereas X-Forward-For header, while not
        // standardised is the de facto standard and widely supported.
        addXForwardForAddresses(addresses, request);

        subject.getPrincipals().add(new Origin(addresses.build()));
    }


    private void addAuthCredentialsToSubject(Request request, Subject subject)
          throws PermissionDeniedCacheException {
        Optional<AuthInfo> optional = parseAuthenticationHeader(request);
        if (optional.isPresent()) {
            AuthInfo info = optional.get();
            switch (info.getScheme()) {
                case HttpServletRequest.BASIC_AUTH:
                    if (!_isBasicAuthenticationEnabled) {
                        return;
                    }

                    try {
                        byte[] bytes = Base64.getDecoder()
                              .decode(info.getData().getBytes(StandardCharsets.US_ASCII));
                        String credential = new String(bytes, StandardCharsets.UTF_8);
                        int colon = credential.indexOf(":");
                        if (colon >= 0) {
                            String user = credential.substring(0, colon);
                            int lastHash = user.lastIndexOf('#');
                            if (lastHash != -1 && lastHash < (user.length() - 1)) {
                                Splitter.on(',')
                                      .trimResults()
                                      .omitEmptyStrings()
                                      .split(user.substring(lastHash + 1))
                                      .forEach(
                                            r -> subject.getPrincipals().add(new DesiredRole(r)));
                                user = user.substring(0, lastHash);
                            }
                            String password = credential.substring(colon + 1);
                            subject.getPrivateCredentials()
                                  .add(new PasswordCredential(user, password));
                        } else {
                            subject.getPrincipals().add(new LoginNamePrincipal(credential));
                        }
                    } catch (IllegalArgumentException e) {
                        LOG.warn(
                              "Authentication Data in the header received is not Base64 encoded {}",
                              request.getHeaders().get("Authorization"));
                    }
                    break;
                case "BEARER":
                    if (!_acceptBearerTokenUnencrypted && !request.isSecure()) {
                        throw new PermissionDeniedCacheException(
                              "not allowed to send bearer token unencrypted");
                    }

                    try {
                        subject.getPrivateCredentials()
                              .add(new BearerTokenCredential(info.getData()));
                    } catch (IllegalArgumentException e) {
                        LOG.info("Bearer Token in invalid {}",
                              request.getHeaders().get("Authorization"));
                    }
                    break;
                default:
                    LOG.debug("Unknown authentication scheme {}", info.getScheme());
            }
        }
    }

    public String getRealm() {
        return _realm;
    }

    /**
     * Sets the HTTP realm used for basic authentication.
     */
    public void setRealm(String realm) {
        _realm = realm;
    }

    /**
     * Specifies whether the door is read only.
     */
    public void setReadOnly(boolean isReadOnly) {
        _doorRestriction = isReadOnly ? Restrictions.readOnly() : Restrictions.none();
    }

    public void setEnableBasicAuthentication(boolean isEnabled) {
        _isBasicAuthenticationEnabled = isEnabled;
    }

    public void setEnableSpnegoAuthentication(boolean isEnabled) {
        _isSpnegoAuthenticationEnabled = isEnabled;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy) {
        _loginStrategy = loginStrategy;
    }

    public void setAcceptBearerTokenUnencrypted(boolean value) {
        _acceptBearerTokenUnencrypted = value;
    }

    private class AuthHandlerResponse extends Response.Wrapper {

        private final boolean suppressWWWAuthenticate;

        public AuthHandlerResponse(Response response, Request request) {
            super(request, response);
            suppressWWWAuthenticate = request.getHeaders().contains("Suppress-WWW-Authenticate");
        }

        @Override
        public void setStatus(int code) {
            addAuthenticationChallenges(code);
            super.setStatus(code);
        }

        private void addAuthenticationChallenges(int code) {
            if (code == HttpServletResponse.SC_UNAUTHORIZED) {
                if (_isSpnegoAuthenticationEnabled) {
                    // Firefox always defaults to the first available authentication mechanism
                    // Conversely, Chrome and Safari choose the strongest mechanism
                    getHeaders().put(HttpHeader.WWW_AUTHENTICATE,
                          HttpHeader.NEGOTIATE.asString());
                    getHeaders().add(HttpHeader.WWW_AUTHENTICATE,
                          "Basic realm=\"" + getRealm() + "\"");
                } else {
                    if (suppressWWWAuthenticate) {
                        getHeaders().remove(HttpHeader.WWW_AUTHENTICATE);
                        getHeaders().add("Suppress-WWW-Authenticate", "suppressed");
                    } else {
                        getHeaders().put(HttpHeader.WWW_AUTHENTICATE.asString(),
                              "Basic realm=\"" + getRealm() + "\"");
                    }
                }
            }
        }
    }


    private class AuthInfo {

        private final String _scheme;
        private final String _data;

        AuthInfo(String scheme, String data) {
            _scheme = scheme;
            _data = data;
        }

        public String getScheme() {
            return _scheme;
        }

        public String getData() {
            return _data;
        }
    }

    private Optional<AuthInfo> parseAuthenticationHeader(Request request) {
        String header = request.getHeaders().get("Authorization");
        if (header == null) {
            LOG.debug("No credentials found in Authorization header");
            return Optional.empty();
        }
        String authScheme;
        String authData;

        if (header.length() == 0) {
            LOG.debug("Credentials in Authorization header are not-null, but are empty");
            return Optional.empty();
        }
        int space = header.indexOf(" ");

        if (space < 0) {
            authScheme = header.toUpperCase();
            authData = "";
        } else {
            authScheme = header.substring(0, space).toUpperCase();
            authData = header.substring(space + 1);
        }
        return Optional.of(new AuthInfo(authScheme, authData));
    }

    private void addDesiredRolesToSubject(Request request, Subject subject) {
        String header = request.getHeaders().get("Roles");
        if (header == null) {
            LOG.debug("No roles header found");
            return;
        }

        if (header.length() == 0) {
            LOG.debug("Desired roles in roles header are not-null, but are empty");
            return;
        }

        int space = header.indexOf(" ");
        String data = space >= 0 ? header.substring(space + 1) : header;

        Splitter.on(',')
              .trimResults()
              .omitEmptyStrings()
              .split(data)
              .forEach(
                    r -> {
                        subject.getPrincipals().add(new DesiredRole(r));
                    });
    }
}
