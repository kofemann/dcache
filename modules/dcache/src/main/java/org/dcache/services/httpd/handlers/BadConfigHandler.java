package org.dcache.services.httpd.handlers;

import dmg.util.HttpException;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

/**
 * Handles messages having to do with alias configuration errors (in the batch file).
 *
 * @author arossi
 */
public class BadConfigHandler extends Handler.Abstract {

    private static final String BAD_CONFIG = "HTTP Server badly configured";
    private ServletException exception;

    public BadConfigHandler() {
        exception = new ServletException(new HttpException(
              HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
              BAD_CONFIG));
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback)
          throws IOException, ServletException {
        callback.failed(exception);
        throw exception;
    }

    public void setFailureMessage(String failureMessage) {
        if (failureMessage != null) {
            exception = new ServletException(new HttpException(
                  HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                  BAD_CONFIG + ": " + failureMessage + "."));
        }
    }
}
