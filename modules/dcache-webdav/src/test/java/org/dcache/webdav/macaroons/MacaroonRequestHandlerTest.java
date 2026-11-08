/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.webdav.macaroons;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import diskCacheV111.util.FsPath;
import dmg.cells.nucleus.CellAddressCore;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Optional;
import java.util.Set;
import javax.security.auth.Subject;
import javax.servlet.http.HttpServletResponse;
import org.dcache.auth.Subjects;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.PrefixRestriction;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.http.AuthenticationHandler;
import org.dcache.http.PathMapper;
import org.dcache.macaroons.InternalErrorException;
import org.dcache.macaroons.MacaroonContext;
import org.dcache.macaroons.MacaroonProcessor;
import org.eclipse.jetty.server.Request;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MacaroonRequestHandlerTest {

    private static final FsPath USER_ROOT = FsPath.create("/data/user");
    private static final FsPath UPLOAD_DIR = FsPath.create("/upload");

    private MacaroonProcessor processor;
    private HttpServletResponse response;
    private String doorRoot;

    @Before
    public void setUp() throws Exception {
        processor = mock(MacaroonProcessor.class);
        // The minted macaroon itself is of no interest: stop once the context is built.
        when(processor.buildMacaroon(any(), any(), any()))
              .thenThrow(new InternalErrorException("not minting", null));
        response = mock(HttpServletResponse.class);
        doorRoot = "/";
    }

    @Test
    public void shouldIgnoreUploadDirForRootTarget() throws Exception {
        // What gPlazma returns for a user with a non-/ root and an absolute upload directory.
        request("/", new RootDirectory(USER_ROOT.toString()),
              new PrefixRestriction(USER_ROOT, UPLOAD_DIR));

        MacaroonContext context = mintedContext();
        assertEquals(Optional.of(USER_ROOT), context.getRoot());
        assertEquals(Optional.empty(), context.getPath());
    }

    @Test
    public void shouldUseRequestedPathWithUploadDirRestriction() throws Exception {
        request("/sub/dir", new RootDirectory(USER_ROOT.toString()),
              new PrefixRestriction(USER_ROOT, UPLOAD_DIR));

        MacaroonContext context = mintedContext();
        assertEquals(Optional.of(USER_ROOT), context.getRoot());
        assertEquals(Optional.of("/sub/dir"), context.getPath());
    }

    @Test
    public void shouldIgnoreRestrictionAboveDoorRoot() throws Exception {
        doorRoot = "/data/user/door";

        request("/", new RootDirectory(USER_ROOT.toString()),
              new PrefixRestriction(USER_ROOT, UPLOAD_DIR));

        MacaroonContext context = mintedContext();
        assertEquals(Optional.of(FsPath.create("/data/user/door")), context.getRoot());
        assertEquals(Optional.empty(), context.getPath());
    }

    @Test
    public void shouldUsePrefixBelowRootForRootTarget() throws Exception {
        request("/", new RootDirectory(USER_ROOT.toString()),
              new PrefixRestriction(FsPath.create("/data/user/project"), UPLOAD_DIR));

        assertEquals(Optional.of("/project"), mintedContext().getPath());
    }

    @Test
    public void shouldUseRequestedPathWithoutRestriction() throws Exception {
        request("/public", new RootDirectory("/"));

        MacaroonContext context = mintedContext();
        assertEquals(Optional.empty(), context.getRoot());
        assertEquals(Optional.of("/public"), context.getPath());
    }

    @Test
    public void shouldNotAddPathForRootTargetWithoutRestriction() throws Exception {
        request("/", new RootDirectory("/"));

        assertEquals(Optional.empty(), mintedContext().getPath());
    }

    @Test
    public void shouldRejectRootTargetWithMultiplePrefixesBelowRoot() throws Exception {
        request("/", new RootDirectory(USER_ROOT.toString()),
              new PrefixRestriction(FsPath.create("/data/user/a"), FsPath.create("/data/user/b")));

        assertBadRequest();
    }

    @Test
    public void shouldRejectRequestedPathOutsideRestriction() throws Exception {
        request("/other", new RootDirectory(USER_ROOT.toString()),
              new PrefixRestriction(FsPath.create("/data/user/project"), UPLOAD_DIR));

        assertBadRequest();
    }

    private void request(String target, LoginAttribute... attributes) throws Exception {
        PathMapper mapper = new PathMapper();
        mapper.setRootPath(doorRoot);

        MacaroonRequestHandler handler = new MacaroonRequestHandler();
        handler.setPathMapper(mapper);
        handler.setMacaroonProcessor(processor);
        handler.setMaximumLifetime(86_400_000L);
        handler.setDefaultLifetime(3_600_000L);
        handler.setCellAddress(new CellAddressCore("WebDAV", "dCacheDomain"));

        Request request = mock(Request.class);
        when(request.getMethod()).thenReturn("POST");
        when(request.getContentType()).thenReturn("application/macaroon-request");
        when(request.isSecure()).thenReturn(true);
        when(request.getRemoteAddr()).thenReturn("127.0.0.1");
        when(request.getReader()).thenReturn(new BufferedReader(new StringReader("{}")));
        when(request.getAttribute(AuthenticationHandler.DCACHE_LOGIN_ATTRIBUTES))
              .thenReturn(Set.of(attributes));

        Subject user = Subjects.of(1000, 1000, new int[]{1000});
        user.getPrincipals().add(new UserNamePrincipal("user"));
        Subject.callAs(user, () -> {
            handler.handle(target, request, request, response);
            return null;
        });
    }

    private MacaroonContext mintedContext() throws Exception {
        verify(response, never()).sendError(anyInt());
        ArgumentCaptor<MacaroonContext> context = ArgumentCaptor.forClass(MacaroonContext.class);
        verify(processor).buildMacaroon(any(), context.capture(), any());
        return context.getValue();
    }

    private void assertBadRequest() throws Exception {
        verify(response).setStatus(eq(HttpServletResponse.SC_BAD_REQUEST), anyString());
        verify(response, never()).sendError(anyInt());
        verify(processor, never()).buildMacaroon(any(), any(), any());
    }
}
