package org.dcache.restful.qos;

import static org.dcache.qos.QoSTransitionEngine.Qos.DISK;
import static org.dcache.qos.QoSTransitionEngine.Qos.DISK_TAPE;
import static org.dcache.qos.QoSTransitionEngine.Qos.TAPE;
import static org.dcache.qos.QoSTransitionEngine.Qos.VOLATILE;

import diskCacheV111.util.PermissionDeniedCacheException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.dcache.qos.QoSTransitionEngine.Qos;
import org.dcache.restful.util.RequestUser;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * RestFul API for querying and manipulating QoS
 */
@Component
@Api(value = "qos", authorizations = {@Authorization("basicAuth")})
@Path("/qos-management/qos/")
public class QosManagement {

    private static final Logger LOGGER = LoggerFactory.getLogger(QosManagement.class);

    @Inject
    @Named("geographic-placement")
    private List<String> geographicPlacement;

    @GET
    @ApiOperation("List the available quality of services for a specific object "
          + "type.  Requires authentication.")
    @ApiResponses({
          @ApiResponse(code = 401, message = "Unauthorized."),
          @ApiResponse(code = 403, message = "Forbidden."),
          @ApiResponse(code = 404, message = "Not found."),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Path("{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getQosList(@ApiParam(value = "The kind of object to query.",
          allowableValues = "file,directory")
    @PathParam("type") String qosValue) {

        JSONObject json = new JSONObject();

        if (RequestUser.isAnonymous()) {
            throw new NotAuthorizedException("Permission denied");
        }

        if ("file".equals(qosValue)) {
            JSONArray list = new JSONArray(Arrays.asList(DISK.displayName(),
                  TAPE.displayName(),
                  DISK_TAPE.displayName(),
                  VOLATILE.displayName()));
            json.put("name", list);
        } else if ("directory".equals(qosValue.trim())) {
            JSONArray list = new JSONArray(Arrays.asList(DISK.displayName(),
                  TAPE.displayName(),
                  DISK_TAPE.displayName(),
                  VOLATILE.displayName()));
            json.put("name", list);
        } else {
            throw new NotFoundException();
        }

        json.put("status", "200");
        json.put("message", "successful");

        return json.toString();
    }


    @GET
    @ApiOperation("Provide information about a specific file quality of "
          + "services.  Requires authentication.")
    @ApiResponses({
          @ApiResponse(code = 401, message = "Unauthorized"),
          @ApiResponse(code = 403, message = "Forbidden"),
          @ApiResponse(code = 404, message = "Not found"),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Path("/file/{qos}")
    @Produces(MediaType.APPLICATION_JSON)
    public BackendCapabilityResponse getQueriedQosForFiles(
          @ApiParam("The file quality of service to query.")
          @PathParam("qos") String qosValue) {

        BackendCapabilityResponse backendCapabilityResponse
              = new BackendCapabilityResponse();

        BackendCapability backendCapability = new BackendCapability();

        try {
            if (RequestUser.isAnonymous()) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            backendCapabilityResponse.setStatus("200");
            backendCapabilityResponse.setMessage("successful");

            QoSMetadata qoSMetadata;

            switch (Qos.fromDisplayName(qosValue)) {
                case DISK:
                    qoSMetadata = new QoSMetadata("1",
                          geographicPlacement,
                          "100");
                    setBackendCapability(backendCapability, DISK.displayName(),
                          Arrays.asList(TAPE.displayName(),
                                DISK_TAPE.displayName()),
                          qoSMetadata);
                    break;
                case TAPE:
                    qoSMetadata = new QoSMetadata("1",
                          geographicPlacement,
                          "600000");
                    setBackendCapability(backendCapability, TAPE.displayName(),
                          Arrays.asList(DISK_TAPE.displayName()),
                          qoSMetadata);
                    break;
                case DISK_TAPE:
                    qoSMetadata = new QoSMetadata("2",
                          geographicPlacement,
                          "100");
                    setBackendCapability(backendCapability, DISK_TAPE.displayName(),
                          Arrays.asList(TAPE.displayName()),
                          qoSMetadata);
                    break;
                case VOLATILE:
                    qoSMetadata = new QoSMetadata("0",
                          geographicPlacement,
                          "100");
                    setBackendCapability(backendCapability, VOLATILE.displayName(),
                          Arrays.asList(DISK.displayName(),
                                TAPE.displayName(),
                                DISK_TAPE.displayName()),
                          qoSMetadata);
                    break;
                default:
                    throw new NotFoundException();
            }
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (UnsupportedOperationException e) {
            throw new BadRequestException(e);
        }

        backendCapabilityResponse.setBackendCapability(backendCapability);

        return backendCapabilityResponse;
    }


    @GET
    @ApiOperation("Provides information about a specific directory quality of "
          + "services.  Requires authentication.")
    @ApiResponses({
          @ApiResponse(code = 401, message = "Unauthorized."),
          @ApiResponse(code = 403, message = "Forbidden."),
          @ApiResponse(code = 404, message = "Not found."),
          @ApiResponse(code = 500, message = "Internal Server Error"),
    })
    @Path("/directory/{qos}")
    @Produces(MediaType.APPLICATION_JSON)
    public BackendCapabilityResponse getQueriedQosForDirectories(
          @ApiParam("The directory quality of service to query.")
          @PathParam("qos") String qosValue) {

        BackendCapabilityResponse backendCapabilityResponse
              = new BackendCapabilityResponse();

        BackendCapability backendCapability = new BackendCapability();

        try {
            if (RequestUser.isAnonymous()) {
                throw new PermissionDeniedCacheException("Permission denied");
            }

            backendCapabilityResponse.setStatus("200");
            backendCapabilityResponse.setMessage("successful");

            QoSMetadata qoSMetadata;

            switch (Qos.fromDisplayName(qosValue)) {
                case DISK:
                    qoSMetadata = new QoSMetadata("1",
                          geographicPlacement,
                          "100");
                    setBackendCapability(backendCapability, DISK.displayName(),
                          Arrays.asList(TAPE.displayName()),
                          qoSMetadata);
                    break;
                case TAPE:
                    qoSMetadata = new QoSMetadata("1",
                          geographicPlacement,
                          "600000");
                    setBackendCapability(backendCapability, TAPE.displayName(),
                          Arrays.asList(DISK.displayName()),
                          qoSMetadata);
                    break;
                case DISK_TAPE:
                    qoSMetadata = new QoSMetadata("2",
                          geographicPlacement,
                          "100");
                    setBackendCapability(backendCapability, DISK_TAPE.displayName(),
                          Collections.emptyList(),
                          qoSMetadata);
                    break;
                case VOLATILE:
                    qoSMetadata = new QoSMetadata("0",
                          geographicPlacement,
                          "100");
                    setBackendCapability(backendCapability, VOLATILE.displayName(),
                          Collections.emptyList(),
                          qoSMetadata);
                    break;
                default:
                    throw new NotFoundException();
            }
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (UnsupportedOperationException e) {
            throw new BadRequestException(e);
        }

        backendCapabilityResponse.setBackendCapability(backendCapability);
        return backendCapabilityResponse;
    }


    public void setBackendCapability(BackendCapability backendCapability,
          String name,
          List<String> transitions,
          QoSMetadata qoSMetadata) {

        backendCapability.setName(name);
        backendCapability.setTransition(transitions);
        backendCapability.setMetadata(qoSMetadata);
    }


}
