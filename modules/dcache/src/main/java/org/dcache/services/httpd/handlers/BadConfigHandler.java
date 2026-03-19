package org.dcache.services.httpd.handlers;

import dmg.util.HttpException;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.eclipse.jetty.ee9.nested.AbstractHandler;
import org.eclipse.jetty.ee9.nested.Request;

/**
 * Handles messages having to do with alias configuration errors (in the batch file).
 *
 * @author arossi
 */
public class BadConfigHandler extends AbstractHandler {

    private static final String BAD_CONFIG = "HTTP Server badly configured";
    private ServletException exception;

    public BadConfigHandler() {
        exception = new ServletException(new HttpException(
              HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
              BAD_CONFIG));
    }

    @Override
    public void handle(String target, Request baseRequest,
          HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {
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
