package org.dcache.services.httpd.handlers;

import jakarta.servlet.ServletException;
import java.io.IOException;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes redirection from one URL to another.
 *
 * @author arossi
 */
public class RedirectHandler extends Handler.Abstract {

    private static final Logger LOGGER
          = LoggerFactory.getLogger(RedirectHandler.class);

    private final String fromContext;
    private final String toContext;

    public RedirectHandler(String fromContext, String toContext) {
        if ("<home>".equals(fromContext)) {
            fromContext = "/";
        }
        this.fromContext = fromContext;
        this.toContext = toContext;
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback) throws
          IOException, ServletException {

        String target = request.getHttpURI().getPath();
        LOGGER.debug("target: {}", target);

        if (target.contains(fromContext)) {
            StringBuilder targetUrl = new StringBuilder(target);
            int i = targetUrl.indexOf(fromContext);
            String newUrl
                  = targetUrl.replace(i, i + fromContext.length(),
                        toContext)
                  .toString();
            LOGGER.debug("redirected to: {}", newUrl);

            Response.sendRedirect(request, response, callback, newUrl);
            return true;
        }
        return false;
    }
}
