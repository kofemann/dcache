package org.dcache.webdav;

import com.google.common.collect.ImmutableList;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import io.milton.http.Auth;
import io.milton.http.FileItem;
import io.milton.http.HttpManager;
import io.milton.http.RequestParseException;
import io.milton.servlet.MiltonServlet;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.Servlet;
import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.servlet.http.HttpUpgradeHandler;
import jakarta.servlet.http.Part;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.AccessController;
import java.security.Principal;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.commons.fileupload2.core.FileUploadException;
import org.dcache.auth.Subjects;
import org.dcache.util.Transfer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Jetty handler that wraps a Milton HttpManager. Makes it possible to embed Milton in Jetty
 * without using the Milton servlet.
 */
public class MiltonHandler implements Servlet, CellIdentityAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiltonHandler.class);
    private static final ImmutableList<String> ALLOWED_ORIGIN_PROTOCOL = ImmutableList.of("http",
          "https");

    private HttpManager _httpManager;
    private CellAddressCore _myAddress;

    public void setHttpManager(HttpManager httpManager) {
        _httpManager = httpManager;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        _myAddress = address;
    }

    @Override
    public void init(ServletConfig config) throws ServletException {

    }

    @Override
    public ServletConfig getServletConfig() {
        return null;
    }

    @Override
    public void service(jakarta.servlet.ServletRequest servletRequest, jakarta.servlet.ServletResponse servletResponse) throws ServletException, IOException {

        HttpServletRequest request = (HttpServletRequest)servletRequest;
        HttpServletResponse response = (HttpServletResponse)servletResponse;

        try (CDC ignored = CDC.reset(_myAddress)) {
            Transfer.initSession(false, false);
            ServletContext context = (ServletContext) ContextHandler.getCurrentContextHandler();

            if ("USERINFO".equals(request.getMethod())) {
                response.setStatus(501);
            } else {
                Subject subject = Subject.getSubject(AccessController.getContext());
                ServletRequest req = new DcacheServletRequest(request, context);
                ServletResponse resp = new DcacheServletResponse(response);

                /* Although we don't rely on the authorization tag
                 * ourselves, Milton uses it to detect that the request
                 * was preauthenticated.
                 */
                req.setAuthorization(new Auth(Subjects.getUserName(subject), subject));
                _httpManager.process(req, resp);
            }
        }
    }

    @Override
    public String getServletInfo() {
        return "";
    }

    @Override
    public void destroy() {

    }


    /**
     * Dcache specific subclass to workaround various Jetty/Milton problems.
     */
    class DcacheServletRequest extends ServletRequest {

        private final HttpServletRequest request;

        public DcacheServletRequest(HttpServletRequest request,
              ServletContext context) {
            super(request, context);
            this.request = request;
        }

        @Override
        public void parseRequestParameters(Map<String, String> params, Map<String, FileItem> files)
              throws RequestParseException {
            /*
             * io.milton.http.ResourceHandlerHelper#process calls
             * Request#parseRequestParameters and catches any
             * RequestParseException thrown.  Unfortunately, it logs this
             * with a stack-trace, but otherwise ignores such failures.
             *
             * See  https://github.com/miltonio/milton2/issues/93 for details.
             *
             * As a work-around, such exceptions are caught here and
             * converted into an unchecked exception that results in
             * the server responding with a 400 Bad Request.
             */
            try {
                super.parseRequestParameters(params, files);
            } catch (RequestParseException e) {
                // Inexplicably, Milton wraps any FileUploadException with a
                // RequestParseException containing a meaningless message.
                String message = e.getCause() instanceof FileUploadException
                      ? e.getCause().getMessage()
                      : e.getMessage();
                throw new UncheckedBadRequestException(message, e, null);
            }
        }

        /**
         * Is there content from the client that is unparsed by Jetty. This is equivalent to {@code
         * getInputStream().available() > 0} with the distinction that it does not result in the
         * "100 Continue" response that {@code getInputStream()} would normally by triggered.
         */
        public boolean isClientSendingEntity() {
            try {
                /* This is a work-around for Jetty where calling getInputStream
                 * results in Jetty immediately returning "100 Continue" to
                 * the client.  The Jetty-specific getHttpInput method
                 * provides a "back door" that returns the InputStream without
                 * triggering this behaviour.
                 */
                return request.getInputStream().available() > 0;
            } catch (IOException e) {
                LOGGER.warn("Got exception in hasContent: {}", e.toString());
                return false;
            }
        }

        @Override
        public InputStream getInputStream() {
            /* Jetty tells the client to continue uploading data as
             * soon as the input stream is retrieved by the servlet.
             * We want to redirect the request before that happens,
             * hence we query the input stream lazily.
             */
            return new InputStream() {
                private InputStream inner;

                private InputStream getRealInputStream() throws IOException {
                    if (inner == null) {
                        inner = DcacheServletRequest.super.getInputStream();
                    }
                    return inner;
                }

                @Override
                public int read() throws IOException {
                    return getRealInputStream().read();
                }

                @Override
                public int read(byte[] b) throws IOException {
                    return getRealInputStream().read(b);
                }

                @Override
                public int read(byte[] b, int off, int len)
                      throws IOException {
                    return getRealInputStream().read(b, off, len);
                }

                @Override
                public long skip(long n) throws IOException {
                    return getRealInputStream().skip(n);
                }

                @Override
                public int available() throws IOException {
                    return getRealInputStream().available();
                }

                @Override
                public void close() throws IOException {
                    getRealInputStream().close();
                }

                @Override
                public synchronized void mark(int readlimit) {
                    throw new UnsupportedOperationException("Mark is unsupported");
                }

                @Override
                public synchronized void reset() throws IOException {
                    getRealInputStream().reset();
                }

                @Override
                public boolean markSupported() {
                    return false;
                }
            };
        }
    }

    /**
     * dCache specific subclass to workaround various Jetty/Milton problems.
     */
    private class DcacheServletResponse extends ServletResponse {

        public DcacheServletResponse(HttpServletResponse r) {
            super(r);
        }

        @Override
        public void setContentLengthHeader(Long length) {
            /* If the length is unknown, Milton insists on
             * setting an empty string value for the
             * Content-Length header.
             *
             * Instead we want the Content-Length header
             * to be skipped and rely on Jetty using
             * chunked encoding.
             */
            if (length != null) {
                super.setContentLengthHeader(length);
            }
        }
    }
}
