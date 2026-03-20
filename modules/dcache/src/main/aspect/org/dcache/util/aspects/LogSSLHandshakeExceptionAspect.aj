package org.dcache.util.aspects;

import org.eclipse.jetty.io.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;

import java.net.InetSocketAddress;

public aspect LogSSLHandshakeExceptionAspect
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);

    before(Connection c, Exception e) : withincode(void Connection.onFillable()) && this(c) && handler(Exception) && args(e) {
        if (e instanceof SSLHandshakeException) {
            InetSocketAddress remoteAddress = (InetSocketAddress)(c.getEndPoint().getRemoteSocketAddress());
            LOGGER.warn("SSL handshake with {}:{} failed: {}", remoteAddress.getAddress().getHostAddress(), remoteAddress.getPort(), e.getMessage());
        }
    }
}
