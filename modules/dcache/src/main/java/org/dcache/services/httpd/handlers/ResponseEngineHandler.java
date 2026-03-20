package org.dcache.services.httpd.handlers;

import static java.util.Objects.requireNonNull;

import dmg.util.HttpException;
import dmg.util.HttpRequest;
import dmg.util.HttpResponseEngine;
import jakarta.servlet.ServletException;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URISyntaxException;
import org.dcache.services.httpd.util.StandardHttpRequest;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;


/**
 * Wraps calls to {@link HttpResponseEngine} aliases with the Jetty handler API.
 *
 * @author arossi
 */
public class ResponseEngineHandler extends Handler.Abstract {

    private final HttpResponseEngine engine;

    public ResponseEngineHandler(HttpResponseEngine engine) {
        this.engine = engine;
    }

    @Override
    public boolean handle( Request request, Response response, Callback callback) throws IOException, ServletException {
        requireNonNull(engine);
        try {
            HttpRequest proxy = new StandardHttpRequest(request, response);
            engine.queryUrl(proxy);
            proxy.getPrintWriter().flush();
            callback.succeeded();
        } catch (HttpException e) {
            callback.failed(e);
            response.setStatus(e.getErrorCode());
        } catch (URISyntaxException e) {
            callback.failed(e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        return true;
    }
}
