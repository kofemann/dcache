package org.dcache.services.httpd.handlers;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import dmg.util.HttpBasicAuthenticationException;
import dmg.util.HttpException;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.dcache.services.httpd.exceptions.OnErrorException;
import org.dcache.services.httpd.util.AliasEntry;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for parsing the request to find the correct alias type and passing the context to the
 * alias handler.
 *
 * @author arossi
 */
public class HandlerDelegator extends Handler.Abstract {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(HandlerDelegator.class);
    private static final Splitter PATH_SPLITTER
          = Splitter.on('/').omitEmptyStrings();

    private static String extractAlias(String requestURI) {
        return Iterables.getFirst(PATH_SPLITTER.split(requestURI), "<home>");
    }

    private static void handleException(Exception e, String uri,
          Response response) {
        if (e instanceof ServletException) {
            final Throwable cause = e.getCause();
            if (cause instanceof HttpException) {
                printHttpException((HttpException) cause, response);
            }
            LOGGER.warn("Problem with {}: {}", uri, e.getMessage());
        } else if (e instanceof HttpException) {
            printHttpException((HttpException) e, response);
            LOGGER.warn("Problem with {}: {}", uri, e.getMessage());
        } else if (e instanceof RuntimeException) {
            printHttpException(new HttpException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                  "Internal problem processing request"), response);
            LOGGER.warn("Bug found, please report it", e);
        } else {
            printHttpException(new HttpException(HttpServletResponse.SC_BAD_REQUEST,
                  "Bad Request : " + e), response);
            LOGGER.warn("Problem in HttpServiceCellHandler: {}", e.getMessage());
        }
    }

    private static void printHttpException(HttpException exception,
          Response response) {
        if (exception instanceof HttpBasicAuthenticationException) {
            final String realm
                  = ((HttpBasicAuthenticationException) exception).getRealm();
            response.getHeaders().add("WWW-Authenticate", "Basic realm=\""
                  + realm + "\"");
        }
        response.setStatus(exception.getErrorCode());
    }

    private final Map<String, AliasEntry> aliases = new ConcurrentHashMap<>();

    @Override
    public boolean handle(Request request, Response response, Callback callback) throws IOException {

        String uri = null;
        String alias;
        AliasEntry entry = null;

        try {
            uri = request.getHttpURI().getPath();
            alias = extractAlias(uri);

            LOGGER.debug("handle {}, {}", uri, alias);

            entry = aliases.get(alias);

            if (entry == null) {
                entry = aliases.get("<default>");
            }

            if (entry == null) {
                throw new HttpException(HttpServletResponse.SC_NOT_FOUND,
                      "Alias not found : " + alias);
            }

            LOGGER.debug("alias: {}, entry {}", alias, entry);

            /*
             * Exclusion of POST is absolute.
             */
            if (!request.getMethod().equals("GET")) {
                throw new HttpException(HttpServletResponse.SC_NOT_IMPLEMENTED,
                      "Method not implemented: " + request.getMethod());
            }

            /*
             * check for overwritten alias
             */
            final String alternate = entry.getOverwrite();
            if (alternate != null) {
                LOGGER.debug("handle, overwritten alias: {}", alternate);
                final AliasEntry overwrittenEntry = aliases.get(alternate);
                if (overwrittenEntry != null) {
                    entry = overwrittenEntry;
                }
                LOGGER.debug("handle, alias {}, entry {}", alternate, entry);
            }

            final Handler handler = entry.getHandler();
            LOGGER.debug("got handler: {}", handler);
            if (handler != null) {
                handler.handle(request, response, callback);
            }

        } catch (final Exception e) {
            if (entry != null && e.getCause() instanceof OnErrorException) {
                final String alternate = entry.getOnError();
                if (alternate != null) {
                    LOGGER.debug("handle, onError alias: {}", alternate);
                    final AliasEntry overwrittenEntry = aliases.get(alternate);
                    if (overwrittenEntry != null) {
                        entry = overwrittenEntry;
                        LOGGER.debug("handle, alias {}, entry {}", alternate,
                              entry);
                        final Handler handler = entry.getHandler();
                        if (handler != null) {
                            try {
                                handler.handle(request, response, callback);
                            } catch (final Exception t) {
                                handleException(t, uri, response);
                            }
                        }
                    } else {
                        handleException(new HttpException(HttpServletResponse.SC_NOT_FOUND,
                                    "Not found : " + entry.getSpecificString()),
                              uri, response);
                    }
                }
            } else {
                handleException(e, uri, response);
            }
        }

        LOGGER.info("Finished");
        return true;
    }

    public AliasEntry removeAlias(String name) throws InvocationTargetException {
        AliasEntry entry = aliases.remove(name);
        if (entry != null) {
            try {
                Handler handler = entry.getHandler();
                removeBean(handler);
                if (handler.isStarted()) {
                    handler.stop();
                }
            } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                throw new InvocationTargetException(e, "Handler failed to stop.");
            }
        }
        return entry;
    }

    @Override
    protected void doStart() throws Exception {
        for (AliasEntry entry : aliases.values()) {
            entry.getHandler().setServer(getServer());
        }
        super.doStart();
    }

    public void addAlias(String name, AliasEntry entry) throws InvocationTargetException {
        Handler handler = entry.getHandler();
        addBean(handler, true);
        if (isStarted() && !handler.isStarted()) {
            try {
                handler.setServer(getServer());
                handler.start();
            } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                throw new InvocationTargetException(e, "Handler failed to start.");
            }
        }
        aliases.put(name, entry);
    }

    public Map<String, AliasEntry> getAliases() {
        return Collections.unmodifiableMap(aliases);
    }

    public AliasEntry getAlias(String name) {
        return aliases.get(name);
    }
}
