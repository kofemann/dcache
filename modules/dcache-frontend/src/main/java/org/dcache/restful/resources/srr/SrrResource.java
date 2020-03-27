package org.dcache.restful.resources.srr;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginBrokerSubscriber;
import io.swagger.annotations.Api;
import io.swagger.annotations.Authorization;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.restful.resources.srr.record.SrrBuilder;
import org.dcache.restful.resources.srr.record.SrrRecord;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

/**
 * RestFul API to  provide files/folders manipulation operations.
 */
@Api(value = "srr")
@Component
@Path("/srr")
public class SrrResource {

    @Context
    private HttpServletRequest request;

    private Map<String, String> group2vo;
    // info provider properties
    private String name;
    private String id;
    private String architecture;
    private String quality;

    @Inject
    @Named("spacemanager-stub")
    private CellStub spaceManager;

    @Inject
    @Named("pool-monitor")
    private PoolMonitor remotePoolMonitor;

    @Inject
    @Named("pnfs-stub")
    private CellStub namespaceStub;

    @Inject
    @Named("login-broker-source")
    private LoginBrokerSubscriber loginBrokerSubscriber;

    private boolean spaceReservationEnabled;

    public void setQuality(String quality) {
        this.quality = quality;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setSpaceReservationEnabled(boolean spaceReservationEnabled) {
        this.spaceReservationEnabled = spaceReservationEnabled;
    }

    public void setArchitecture(String architecture) {
        this.architecture = architecture;
    }

    public void setGroupMapping(String mapping) {

        if (Strings.isNullOrEmpty(mapping)) {
            group2vo = Collections.emptyMap();
        }

        group2vo = Splitter.on(',')
                .trimResults()
                .omitEmptyStrings()
                .withKeyValueSeparator('=')
                .split(mapping);

    }

    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("/")
    public SrrRecord getSrr() throws InterruptedException, CacheException, NoRouteToCellException {

        InetAddress remoteAddress  = InetAddresses.forString(request.getRemoteAddr());
        if (!remoteAddress.isLoopbackAddress()) {
            throw new ForbiddenException();
        }

        return SrrBuilder.builder()
                .withLoginBroker(loginBrokerSubscriber)
                .withNamespace(namespaceStub)
                .withPoolMonitor(remotePoolMonitor)
                .withSpaceManagerStub(spaceManager)
                .withSpaceManagerEnaled(spaceReservationEnabled)
                .withId(id)
                .withName(name)
                .withQuality(quality)
                .withArchitecture(architecture)
                .withGroupVoMapping(group2vo)
                .generate();
    }
}
