/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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

package org.dcache.http;

import dmg.cells.nucleus.CDC;
import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import eu.emi.security.authn.x509.impl.PEMCredential;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.dcache.ssl.CanlContextFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import javax.net.ssl.KeyManager;
import java.nio.file.Path;

/**
 * Netty SslContext context factory.
 */
public class OpenSslContextFactoryBean implements FactoryBean<SslContext> {

    private Path serverCertificatePath;
    private Path serverKeyPath;
    private Path serverCaPath;
    private CrlCheckingMode crlCheckingMode;
    private OCSPCheckingMode ocspCheckingMode;

    @Required
    public void setServerCertificatePath(Path serverCertificatePath) {
        this.serverCertificatePath = serverCertificatePath;
    }

    @Required
    public void setServerKeyPath(Path serverKeyPath) {
        this.serverKeyPath = serverKeyPath;
    }

    @Required
    public void setServerCaPath(Path serverCaPath) {
        this.serverCaPath = serverCaPath;
    }

    @Required
    public void setCrlCheckingMode(CrlCheckingMode crlCheckingMode) {
        this.crlCheckingMode = crlCheckingMode;
    }

    @Required
    public void setOcspCheckingMode(OCSPCheckingMode ocspCheckingMode) {
        this.ocspCheckingMode = ocspCheckingMode;
    }

    @Override
    public SslContext getObject() throws Exception {

        CanlContextFactory canlContextFactory = org.dcache.ssl.CanlContextFactory.custom()
                .withCertificateAuthorityPath(serverCaPath)
                .withCrlCheckingMode(crlCheckingMode)
                .withOcspCheckingMode(ocspCheckingMode)
                .withCertificatePath(serverCertificatePath)
                .withKeyPath(serverKeyPath)
                .withLazy(false)
                .withLoggingContext(new CDC()::restore)
                .build();

        PEMCredential serviceCredentials = new PEMCredential(
                serverKeyPath.toString(),
                serverCertificatePath.toString(),
                null);

        KeyManager keyManager = serviceCredentials.getKeyManager();

        // We don't enforce provide to let SslContextBuilder to pick OPENSSL if available and JSSE other wise.
        return SslContextBuilder.forServer(keyManager)
                .trustManager(canlContextFactory.getTrustManagers()[0])
                .build();
    }

    @Override
    public Class<?> getObjectType() {
        return SslContext.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }
}



