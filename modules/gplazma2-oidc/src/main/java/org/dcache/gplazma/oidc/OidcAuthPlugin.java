package org.dcache.gplazma.oidc;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.net.InternetDomainName;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.OAuthProviderPrincipal;
import org.dcache.auth.Origin;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.oidc.helpers.JsonHttpClient;
import org.dcache.gplazma.oidc.jwt.OfflineJwtVerification;
import org.dcache.gplazma.oidc.profiles.OidcProfileFactory;
import org.dcache.gplazma.oidc.userinfo.QueryUserInfoEndpoint;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.util.JsonWebToken;
import org.dcache.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.gplazma.oidc.PropertiesUtils.asInt;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

public class OidcAuthPlugin implements GPlazmaAuthenticationPlugin {

    private final static Logger LOG = LoggerFactory.getLogger(OidcAuthPlugin.class);

    private final static String HTTP_CONCURRENT_ACCESS = "gplazma.oidc.http.total-concurrent-requests";
    private final static String HTTP_PER_ROUTE_CONCURRENT_ACCESS = "gplazma.oidc.http.per-route-concurrent-requests";
    private final static String HTTP_TIMEOUT = "gplazma.oidc.http.timeout";
    private final static String HTTP_TIMEOUT_UNIT = "gplazma.oidc.http.timeout.unit";
    private final static String OIDC_ALLOWED_AUDIENCES = "gplazma.oidc.audience-targets";
    private final static String OIDC_HOSTNAMES = "gplazma.oidc.hostnames";
    private final static String OIDC_PROVIDER_PREFIX = "gplazma.oidc.provider!";

    private final static String SUPPRESS_AUDIENCE_TOKEN = "audience";

    private static final String DEFAULT_PROFILE_NAME = "oidc";
    private static final Map<String,ProfileFactory> PROFILES = Map.of("oidc", new OidcProfileFactory());

    private final TokenProcessor tokenProcessor;
    private final Set<String> audienceTargets;

    public OidcAuthPlugin(Properties properties) {
        this(properties, buildProcessor(properties));
    }

    @VisibleForTesting
    OidcAuthPlugin(Properties properties, TokenProcessor processor) {
        tokenProcessor = processor;

        String targets = properties.getProperty(OIDC_ALLOWED_AUDIENCES);
        audienceTargets = Set.copyOf(Splitter.on(' ').trimResults().splitToList(targets));
    }

    private static IdentityProvider createIdentityProvider(String name, String description) {
        checkArgument(!name.isEmpty(), "Empty name not allowed");

        Args args = new Args(description);
        checkArgument(args.argc() >= 1, "Missing URI");

        Profile profile = buildProfile(args);

        String endpoint = args.argv(0);
        try {
            URI issuer = new URI(endpoint);

            List<String> suppress = args.getOptions("suppress").stream()
                .flatMap(v -> Splitter.on(',').trimResults().splitToList(v).stream())
                .collect(Collectors.toList());
            IdentityProvider idp = new IdentityProvider(name, issuer, profile, suppress);
            if (idp.isSuppressed(SUPPRESS_AUDIENCE_TOKEN)) {
                LOG.warn("Audience (\"aud\") checking is suppressed for OP {}.  This makes dCache "
                    + "compatible with behaviour before version 8.2.0, but it also violates RFC "
                    + "\"MUST\" requirements and may have security implications.",
                    idp.getName());
            }
            return idp;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                  "Invalid endpoint " + endpoint + ": " + e.getMessage());
        }
    }

    private static Profile buildProfile(Args args) {
        String profileName = args.getOption("profile", DEFAULT_PROFILE_NAME);

        ProfileFactory factory = PROFILES.get(profileName);
        checkArgument(factory != null, "profile '%s' is not supported", profileName);

        return factory.create(args.removeOptions("suppress").optionsAsMap());
    }

    @Override
    public void stop() {
        tokenProcessor.shutdown();
    }

    private static TokenProcessor buildProcessor(Properties properties) {
        JsonHttpClient client = buildClientFromProperties(properties);
        Set<IdentityProvider> providers = new HashSet<>();
        providers.addAll(buildHosts(properties));
        providers.addAll(buildProviders(properties));
        checkArgument(!providers.isEmpty(), "No OIDC providers configured");

        var queryUserInfo = new QueryUserInfoEndpoint(properties, client, providers);
        var offlineVerification = new OfflineJwtVerification(properties, client.getClient(), providers);

        return ChainedTokenProcessor
                .tryWith(offlineVerification)
                .andThenTryWith(queryUserInfo);
    }

    @VisibleForTesting
    static JsonHttpClient buildClientFromProperties(Properties properties) {
        int soTimeout = (int) TimeUnit.valueOf(properties.getProperty(HTTP_TIMEOUT_UNIT))
              .toMillis(asInt(properties, HTTP_TIMEOUT));

        return new JsonHttpClient(asInt(properties, HTTP_CONCURRENT_ACCESS),
              asInt(properties, HTTP_PER_ROUTE_CONCURRENT_ACCESS),
              soTimeout);
    }

    @VisibleForTesting
    static Set<IdentityProvider> buildHosts(Properties properties) {
        String oidcHostnamesProperty = properties.getProperty(OIDC_HOSTNAMES);
        checkArgument(oidcHostnamesProperty != null, OIDC_HOSTNAMES + " not defined");

        Map<Boolean, Set<String>> validHosts = Arrays.stream(oidcHostnamesProperty.split("\\s+"))
              .filter(not(String::isEmpty))
              .collect(
                    Collectors.groupingBy(InternetDomainName::isValid,
                          Collectors.toSet())
              );

        Set<String> badHosts = validHosts.get(Boolean.FALSE);
        checkArgument(badHosts == null, "Invalid hosts in %s: %s",
              OIDC_HOSTNAMES, Joiner.on(", ").join(nullToEmpty(badHosts)));

        Set<String> goodHosts = validHosts.get(Boolean.TRUE);
        return goodHosts == null
              ? Collections.emptySet()
              : goodHosts.stream()
                    .map(h -> createIdentityProvider(h, "https://" + h + "/"))
                    .collect(Collectors.toSet());
    }

    @VisibleForTesting
    static Set<IdentityProvider> buildProviders(Properties properties) {
        return properties.stringPropertyNames().stream()
              .filter(n -> n.startsWith(OIDC_PROVIDER_PREFIX))
              .map(n -> {
                  try {
                      String name = n.substring(OIDC_PROVIDER_PREFIX.length());
                      return createIdentityProvider(name, properties.getProperty(n));
                  } catch (IllegalArgumentException e) {
                      throw new IllegalArgumentException(
                            "Bad OIDC provider " + n + ": " + e.getMessage());
                  }
              })
              .collect(Collectors.toSet());
    }

    private static <T> Predicate<T> not(Predicate<T> t) {
        return t.negate();
    }

    @Override
    public void authenticate(Set<Object> publicCredentials,
          Set<Object> privateCredentials,
          Set<Principal> identifiedPrincipals)
          throws AuthenticationException {

        String token = null;
        for (Object credential : privateCredentials) {
            if (credential instanceof BearerTokenCredential) {
                checkAuthentication(token == null, "Multiple bearer tokens");

                token = ((BearerTokenCredential) credential).getToken();
                LOG.debug("Found bearer token: {}", token);
            }
        }

        checkAuthentication(token != null, "No bearer token in the credentials");
        checkValid(token);

        try {
            ExtractResult result = tokenProcessor.extract(token);
            checkAuthentication(!result.claims().isEmpty(), "processing token yielded no claims");
            checkAudience(result, identifiedPrincipals);

            var idp = result.idp();
            identifiedPrincipals.add(new OAuthProviderPrincipal(idp.getName()));

            Profile profile = idp.getProfile();
            var principals = profile.processClaims(idp, result.claims());
            identifiedPrincipals.addAll(principals);
        } catch (UnableToProcess e) {
            throw new AuthenticationException("Unable to process token: " + e.getMessage());
        }
    }

    private static void checkValid(String token) throws AuthenticationException {
        if (JsonWebToken.isCompatibleFormat(token)) {
            try {
                JsonWebToken jwt = new JsonWebToken(token);

                Instant now = Instant.now();

                Optional<Instant> exp = jwt.getPayloadInstant("exp");
                checkAuthentication(!exp.isPresent() || now.isBefore(exp.get()),
                      "expired");

                Optional<Instant> nbf = jwt.getPayloadInstant("nbf");
                checkAuthentication(!nbf.isPresent() || now.isAfter(nbf.get()),
                      "not yet valid");
            } catch (IOException e) {
                LOG.debug("Failed to parse token: {}", e.toString());
            }
        }
    }

    private void checkAudience(ExtractResult result, Set<Principal> principals)
            throws AuthenticationException {
        var claims = result.claims();
        var audClaim = claims.get("aud");

        if (audClaim == null) {
            return;
        }

        var idp = result.idp();
        boolean suppressAudience = idp.isSuppressed(SUPPRESS_AUDIENCE_TOKEN);

        if (audClaim.isArray()) {
            List<String> audClaimAsList = new ArrayList<>(audClaim.size());
            audClaim.elements().forEachRemaining(e -> audClaimAsList.add(e.textValue()));

            if (!audClaimAsList.stream().anyMatch(audienceTargets::contains)) {
                if (suppressAudience) {
                    logAudienceSuppression(audClaimAsList.toString(), idp.getName(), claims,
                        principals, "one of these audiences");
                    return;
                }
                throw new AuthenticationException("intended for " + audClaimAsList);
            }
        } else {
            String aud = audClaim.textValue();
            if (!audienceTargets.contains(aud)) {
                if (suppressAudience) {
                    logAudienceSuppression(aud, idp.getName(), claims, principals, "this audience");
                    return;
                }
                throw new AuthenticationException("intended for " + aud);
            }
        }
    }

    private static void logAudienceSuppression(String audience, String op,
            Map<String,JsonNode> claims, Set<Principal> principals, String whatToAdd) {
        Optional<String> clientIPAddress = principals.stream()
            .filter(Origin.class::isInstance)
            .map(Origin.class::cast)
            .findFirst()
            .map(Principal::getName);

        LOG.warn("Accepting token with an incompatible audience \"{}\" issued by OP {} to {}{}. "
            + "To prevent similar warnings, either add {} to the 'gplazma.oidc.audience-targets' "
            + "configuration property or update the client to use an audience value already defined "
            + "in this configuration property.", audience, op,
            clientId(claims).map(id -> "client \"" + id + "\"").orElse("an unknown client"),
            clientIPAddress.map(addr -> " connecting from " + addr).orElse(""),
            whatToAdd);
    }

    private static Optional<String> clientId(Map<String,JsonNode> claims) {
        var clientIdClaim = claims.get("client_id");
        if (clientIdClaim != null && clientIdClaim.isTextual()) {
            return Optional.of(clientIdClaim.textValue());
        }

        // Some OPs (*cough* EGI CheckIn *cough*) violate RFC 9068 by issuing JWTs without a
        // 'client_id' claim; however, they do provide an 'azp' claim.
        var azpClaim = claims.get("azp");
        if (azpClaim != null && azpClaim.isTextual()) {
            return Optional.of(azpClaim.textValue());
        }

        return Optional.empty();
    }

    private static <T> Collection<T> nullToEmpty(final Collection<T> collection) {
        return collection == null ? Collections.emptySet() : collection;
    }
}
