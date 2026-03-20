package org.dcache.restful.resources.doors;

import dmg.cells.services.login.LoginBrokerSubscriber;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import java.util.List;
import java.util.stream.Collectors;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import org.dcache.restful.providers.doors.Door;
import org.dcache.restful.util.HttpServletRequests;
import org.springframework.stereotype.Component;

@Component
@Path("doors")
@Tag(name = "doors", description = "Operations about doors")
@Produces(MediaType.APPLICATION_JSON)
public class DoorsResources {

    @Context
    private HttpServletRequest request;

    private final LoginBrokerSubscriber loginBrokerSubscriber;

    @Inject
    public DoorsResources(LoginBrokerSubscriber subscriber) {
        loginBrokerSubscriber = subscriber;
    }

    @GET
    @Operation(summary = "Obtain a list of available dCache doors.")
    @ApiResponses({
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public List<Door> getDoors() {
        Boolean isAdmin = HttpServletRequests.isAdmin(request);

        return loginBrokerSubscriber.doors().stream()
              .map(info -> new Door(isAdmin, info))
              .collect(Collectors.toList());
    }
}
