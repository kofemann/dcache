package org.dcache.webdav;

import com.google.common.collect.ImmutableList;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import io.milton.http.Auth;
import io.milton.http.FileItem;
import io.milton.http.HttpManager;
import io.milton.http.RequestParseException;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.util.Map;
import javax.security.auth.Subject;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload2.core.FileUploadException;
import org.dcache.auth.Subjects;
import org.dcache.util.Transfer;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Jetty handler that wraps a Milton HttpManager. Makes it possible to embed Milton in Jetty
 * without using the Milton servlet.
 */
public class MiltonHandler
      extends Handler.Abstract
      implements CellIdentityAware {

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
    public boolean handle(Request request, Response response, Callback callback) throws Exception {
        try (CDC ignored = CDC.reset(_myAddress)) {
            Transfer.initSession(false, false);
            var context = request.getContext();

            if ("USERINFO".equals(request.getMethod())) {
                response.setStatus(501);
            } else {
                Subject subject = Subject.getSubject(AccessController.getContext());

                var req = new io.milton.servlet.ServletRequest((HttpServletRequest)request, (ServletContext)context);
                var resp = new io.milton.servlet.ServletResponse((HttpServletResponse)response);

                /* Although we don't rely on the authorization tag
                 * ourselves, Milton uses it to detect that the request
                 * was preauthenticated.
                 */
                req.setAuthorization(new Auth(Subjects.getUserName(subject), subject));
                _httpManager.process(req, resp);
            }
        } finally {
            callback.succeeded();
        }
        return true;
    }
}
