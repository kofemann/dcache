package org.dcache.restful.resources.doors;

import dmg.cells.services.login.LoginBrokerSubscriber;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
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
@Api(value = "doors", description = "Operations about doors",
      authorizations = {
            @Authorization("basicAuth")
      }
)
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
    @ApiOperation(value = "Obtain a list of available dCache doors.")
    @ApiResponses({
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    public List<Door> getDoors() {
        Boolean isAdmin = HttpServletRequests.isAdmin(request);

        return loginBrokerSubscriber.doors().stream()
              .map(info -> new Door(isAdmin, info))
              .collect(Collectors.toList());
    }
}
