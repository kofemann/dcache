package org.dcache.restful.resources.namespace;

import static org.dcache.restful.providers.SuccessfulResponse.successfulResponse;

import com.google.common.base.Strings;
import com.google.common.collect.Range;
import diskCacheV111.util.AttributeExistsCacheException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NoAttributeCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.PnfsWriteExtendedAttributesMessage.Mode;
import diskCacheV111.vehicles.ProtocolInfo;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Exceptions;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import javax.security.auth.Subject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.http.PathMapper;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.qos.QoSTransitionEngine;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.remote.clients.RemoteQoSRequirementsClient;
import org.dcache.restful.providers.JsonFileAttributes;
import org.dcache.restful.util.HandlerBuilders;
import org.dcache.restful.util.HttpServletRequests;
import org.dcache.restful.util.RequestUser;
import org.dcache.restful.util.namespace.NamespaceUtils;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * RestFul API to  provide files/folders manipulation operations.
 */
@Tag(name = "namespace")
@Component
@Path("/namespace")
public class FileResources {

    private static final Logger LOG = LoggerFactory.getLogger(FileResources.class);

    /*
     * Used to get fully qualified name of the client
     * or the last proxy that sent the request.
     * Later used for querying locality of the file.
     */
    @Context
    private HttpServletRequest request;

    @Inject
    private PoolMonitor poolMonitor;

    @Inject
    private PathMapper pathMapper;

    @Inject
    private ListDirectoryHandler listDirectoryHandler;

    @Inject
    @Named("pool-manager-stub")
    private CellStub poolmanager;

    @Inject
    @Named("pinManagerStub")
    private CellStub pinmanager;

    @Inject
    @Named("pnfs-stub")
    private CellStub pnfsmanager;

    @Inject
    @Named("qos-engine")
    private CellStub qosEngine;

    private boolean useQosService;

    @GET
    @Operation(summary = "Find metadata and optionally directory contents.",
          description = "The method offers the possibility to list the content of a "
                + "directory in addition to providing metadata of a "
                + "specified file or directory.")
    @ApiResponses({
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Path("{path : .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public JsonFileAttributes getFileAttributes(@Parameter(description = "Path of file or directory.")
    @PathParam("path") String requestPath,
          @Parameter(description = "Whether to include directory listing.")
          @DefaultValue("false")
          @QueryParam("children") boolean isList,
          @Parameter(description = "Whether to include file locality information.")
          @DefaultValue("false")
          @QueryParam("locality") boolean isLocality,
          @Parameter(description = "Whether to include replica locations.")
          @QueryParam("locations") boolean isLocations,
          @Parameter(description = "Whether to include quality of service.")
          @DefaultValue("false")
          @QueryParam("qos") boolean isQos,
          @Parameter(description = "Whether to include extended attributes.")
          @QueryParam("xattr") boolean isXattr,
          @Parameter(description = "Whether to include labels.")
          @QueryParam("labels") boolean isLabels,
          @Parameter(description = "Whether or not to list checksum values.")
          @QueryParam("checksum") boolean isChecksum,
          @Parameter(description = "Whether or not to print optional attributes")
          @DefaultValue("false")
	  @QueryParam("optional") boolean isOptional,
          @Parameter(description = "Limit number of replies in directory listing.")
          @QueryParam("limit") String limit,
          @Parameter(description = "Number of entries to skip in directory listing.")
          @QueryParam("offset") String offset) throws CacheException {
        JsonFileAttributes fileAttributes = new JsonFileAttributes();
        Set<FileAttribute> attributes =
              NamespaceUtils.getRequestedAttributes(isLocality,
                    isLocations,
                    isQos,
                    isChecksum,
                    isOptional);
        PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager);
        FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new, handler);
        try {

            FileAttributes namespaceAttributes = handler.getFileAttributes(path, attributes);
            NamespaceUtils.chimeraToJsonAttributes(path.name(), fileAttributes,
                  namespaceAttributes,
                  isLocality, isLocations, isLabels,
                  isOptional, isXattr, isChecksum,
                  request, poolMonitor);
            if (isQos) {
                NamespaceUtils.addQoSAttributes(fileAttributes,
                      namespaceAttributes,
                      request, poolMonitor, pinmanager);
            }

            // fill children list id it's a directory and listing is requested
            if (namespaceAttributes.getFileType() == FileType.DIR && isList) {
                Range<Integer> range;
                try {
                    int lower = (offset == null) ? 0 : Integer.parseInt(offset);
                    int ceiling = (limit == null) ? Integer.MAX_VALUE : Integer.parseInt(limit);
                    if (ceiling < 0 || lower < 0) {
                        throw new BadRequestException(
                              "limit and offset can not be less than zero.");
                    }
                    range = (Integer.MAX_VALUE - lower < ceiling) ? Range.atLeast(lower)
                          : Range.closedOpen(lower, lower + ceiling);
                } catch (NumberFormatException e) {
                    throw new BadRequestException("limit and offset must be an integer value.");
                }

                List<JsonFileAttributes> children = new ArrayList<>();

                DirectoryStream stream = listDirectoryHandler.list(
                      HttpServletRequests.roleAwareSubject(request),
                      HttpServletRequests.roleAwareRestriction(request),
                      path,
                      null,
                      range,
                      attributes);

                for (DirectoryEntry entry : stream) {
                    String fName = entry.getName();

                    JsonFileAttributes childrenAttributes = new JsonFileAttributes();

                    NamespaceUtils.chimeraToJsonAttributes(fName,
                          childrenAttributes,
                          entry.getFileAttributes(),
                          isLocality, isLocations, isLabels,
                          isOptional, isXattr, isChecksum,
                          request, poolMonitor);
                    childrenAttributes.setFileName(fName);
                    if (isQos) {
                        NamespaceUtils.addQoSAttributes(childrenAttributes,
                              entry.getFileAttributes(),
                              request, poolMonitor, pinmanager);
                    }
                    children.add(childrenAttributes);
                }

                fileAttributes.setChildren(children);
            }

        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (CacheException | InterruptedException | NoRouteToCellException ex) {
            LOG.warn(Exceptions.meaningfulMessage(ex));
            throw new InternalServerErrorException(ex);
        }
        return fileAttributes;
    }

    @POST
    @Operation(summary = "Modify a file or directory.")
    @Path("{path : .*}")
    @ApiResponses({
          @ApiResponse(responseCode = "400", description = "Transition for directories not supported"),
          @ApiResponse(responseCode = "400", description = "Unsupported QoS transition"),
          @ApiResponse(responseCode = "400", description = "Unknown target QoS"),
          @ApiResponse(responseCode = "400", description = "Unknown action"),
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "409", description = "Attribute already exists"),
          @ApiResponse(responseCode = "409", description = "No such attribute"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces(MediaType.APPLICATION_JSON)
    public Response cmrResources(
          @Parameter(description = "Path of file or directory to be modified.", required = true)
          @PathParam("path") String requestPath,
          @Parameter(description = "A JSON object that has an 'action' "
                + "item with a String value.\n"
                + "\n"
                + "If the 'action' value is 'mkdir' "
                + "then a new directory is created "
                + "with the name taken from the "
                + "value of the JSON object 'name' "
                + "item.  This directory is created "
                + "within the supplied path parameter, "
                + "which must be an existing directory.\n"
                + "\n"
                + "If action is 'mv' then the file "
                + "or directory specified by the path "
                + "parameter is moved and/or "
                + "renamed with the value of the JSON "
                + "object 'destination' item describing "
                + "the final location.  If the "
                + "'destination' value is a relative "
                + "path then it is resolved against "
                + "the path parameter value.\n"
                + "\n"
                + "If action is 'qos' then the value "
                + "of the JSON object 'target' item "
                + "describes the desired QoS."
                + "\n"
                + "If action is 'pin' then the default "
                + "value of lifetime is 0 and liftime-unit "
                + "SECONDS."
                + "\n"
                + "If action is 'rm-xattr' then "
                + "extended attributes of a file "
                + "or directory are removed as "
                + "given by the 'names' item.  The "
                + "'names' value is either a "
                + "string or an array of strings."
                + "\n"
                + "If action is 'set-xattr' then "
                + "extended attributes are created "
                + "or modified.  The optional "
                + "'mode' item controls whether to "
                + "create a new attribute (CREATE), "
                + "to modify an existing attribute "
                + "(MODIFY), or to assign the value "
                + "by either creating a new "
                + "attribute or modifying an "
                + "existing attribute (EITHER).  "
                + "EITHER is the default mode.  The "
                + "'attributes' item value is a JSON "
                + "Object with the new attributes,"
                + "where the JSON Object's key is "
                + "the attribute name and the "
                + "corresponding JSON Object's "
                + "value is this attribute's value."
                + "\n"
                + "If action is 'set-label' then "
                + "a label is added to the"
                + "given file object."
                + "'label' item value is a String."
                + "\n"
                + "If action is 'rm-label' then the corresponding"
                + "label of a file is removed."
                + "The  'label' value is either a string."
                + "\n"
                + "If action is 'chgrp' then the "
                + "command requests the change of "
                + "group-owner of the target file "
                + "or directory.  The value of the "
                + "JSON object 'gid' item is the "
                + "numerical value of the desired "
                + "new group-owner."
                + "\n"
                + "If action is 'chmod' then the "
                + "command reqests the change of "
                + "the target file's or directory's "
                + "permissions.  The value of the "
                + "JSON object 'mode' item is the "
                + "numerical value of the desired "
                + "mode.",
                required = true)
                String requestPayload) {
        try {
            JSONObject reqPayload = new JSONObject(requestPayload);
            String action = (String) reqPayload.get("action");
            PnfsHandler pnfsHandler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager);
            FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new, pnfsHandler);
            PnfsId pnfsId;
            switch (action) {
                case "mkdir":
                    String name = (String) reqPayload.get("name");
                    FsPath.checkChildName(name, BadRequestException::new);
                    pnfsHandler = HandlerBuilders.pnfsHandler(
                          pnfsmanager); // FIXME: non-role identity to ensure correct ownership
                    pnfsHandler.createPnfsDirectory(path.child(name).toString());
                    break;
                case "mv":
                    String dest = (String) reqPayload.get("destination");
                    FsPath target = pathMapper.resolve(request, path.parent(), dest);
                    pnfsHandler.renameEntry(path.toString(), target.toString(), true);
                    break;
                case "qos":
                    String targetQos =
                          reqPayload.has("target") ? reqPayload.getString("target") : null;
                    String qosPolicy =
                          reqPayload.has("policy") ? reqPayload.getString("policy") : null;
                    Integer qosState =
                          reqPayload.has("state") ? (Integer) reqPayload.get("state") : null;
                    Subject subject = RequestUser.getSubject();
                    if (!useQosService) {
                        new QoSTransitionEngine(poolmanager,
                              poolMonitor,
                              pnfsHandler,
                              pinmanager)
                              .adjustQoS(path,
                                    targetQos, request.getRemoteHost());
                    } else {
                        /*
                         *  fire and forget, does not wait for transition to complete
                         */
                        FileAttributes attr
                              = pnfsHandler.getFileAttributes(path.toString(),
                              NamespaceUtils.getRequestedAttributes(false, false,
                                    true, false, true));
                        FileQoSRequirements requirements = getBasicRequirements(targetQos,
                              qosPolicy, qosState, attr);
                        RemoteQoSRequirementsClient client = new RemoteQoSRequirementsClient();
                        client.setRequirementsService(qosEngine);
                        client.fileQoSRequirementsModified(requirements, subject);
                    }
                    break;
                case "pin":
                    Integer lifetime = reqPayload.optInt("lifetime");
                    if (lifetime == null) {
                        lifetime = 0;
                    }
                    String lifetimeUnitVal = Strings.emptyToNull(
                          reqPayload.optString("lifetime-unit"));
                    TimeUnit lifetimeUnit = lifetimeUnitVal == null ?
                          TimeUnit.SECONDS : TimeUnit.valueOf(lifetimeUnitVal);
                    pnfsId = pnfsHandler.getPnfsIdByPath(path.toString());

                    /*
                     *  Fire-and-forget, as it was in 5.2
                     */
                    pinmanager.notify(new PinManagerPinMessage(FileAttributes.ofPnfsId(pnfsId),
                          getProtocolInfo(),
                          HttpServletRequests.roleAwareRestriction(request),
                          getRequestId(),
                          lifetimeUnit.toMillis(lifetime)));
                    break;
                case "unpin":
                    pnfsId = pnfsHandler.getPnfsIdByPath(path.toString());
                    PinManagerUnpinMessage message = new PinManagerUnpinMessage(pnfsId);
                    message.setRequestId(getRequestId());
                    pinmanager.notify(message);
                    break;
                case "rm-xattr":
                    Object namesArgument = reqPayload.get("names");
                    if (namesArgument instanceof String) {
                        pnfsHandler.removeExtendedAttribute(path, (String) namesArgument);
                    } else if (namesArgument instanceof JSONArray) {
                        JSONArray namesArray = (JSONArray) namesArgument;
                        List<String> names = new ArrayList<>(namesArray.length());
                        for (int i = 0; i < namesArray.length(); i++) {
                            names.add(namesArray.getString(i));
                        }
                        pnfsHandler.removeExtendedAttribute(path, names);
                    } else {
                        throw new JSONException("\"names\" is not a String or an array");
                    }
                    break;
                case "set-xattr":
                    String modeString = reqPayload.optString("mode", "EITHER");
                    Mode xattrSetMode = modeOf(modeString);

                    JSONObject attributeOject = reqPayload.getJSONObject("attributes");
                    Map<String, byte[]> attributes = new HashMap<>(attributeOject.length());
                    for (String key : attributeOject.keySet()) {
                        String value = attributeOject.getString(key);
                        attributes.put(key, value.getBytes(StandardCharsets.UTF_8));
                    }
                    pnfsHandler.writeExtendedAttribute(path, attributes, xattrSetMode);
                    break;
                case "set-label":
                    String label = reqPayload.getString("label");
                    pnfsHandler.setFileAttributes(path, FileAttributes.ofLabel(label));
                    break;
                case "rm-label":
                    String labelsArgument = reqPayload.getString("label");
                    pnfsHandler.removeLabel(path, labelsArgument);
                    break;
                case "chgrp":
                    int gid = reqPayload.getInt("gid");
                    pnfsHandler.setFileAttributes(path, FileAttributes.ofGid(gid));
                    break;
                case "chmod":
                    int mode = reqPayload.getInt("mode");
                    pnfsHandler.setFileAttributes(path, FileAttributes.ofMode(mode));
                    break;
                default:
                    throw new UnsupportedOperationException("No such action " + action);
            }
        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (AttributeExistsCacheException e) {
            throw new WebApplicationException(Response.status(409, "Attribute already exist")
                  .build());
        } catch (NoAttributeCacheException e) {
            throw new WebApplicationException(Response.status(409, "No such attribute")
                  .build());
        } catch (NoRouteToCellException | InterruptedException e) {
            throw new InternalServerErrorException(e.toString());
        } catch (UnsupportedOperationException |
              URISyntaxException |
              JSONException |
              CacheException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
        return successfulResponse(Response.Status.CREATED);
    }

    private ProtocolInfo getProtocolInfo() throws URISyntaxException {
        return new HttpProtocolInfo("Http", 1, 1,
              new InetSocketAddress("localhost", 0),
              null, null, null,
              new URI("http", "localhost", null, null));
    }

    private String getRequestId() throws PermissionDeniedCacheException {
        if (RequestUser.isAnonymous()) {
            throw new PermissionDeniedCacheException("cannot get request id for user.");
        }

        return String.valueOf(Subjects.getUid(RequestUser.getSubject()));
    }

    private Mode modeOf(String value) throws JSONException {
        try {
            return Mode.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new JSONException("Unknown mode \"" + value + "\", must be"
                  + " one of " + Arrays.asList(Mode.values()));
        }
    }

    @DELETE
    @Path("{path : .*}")
    @Operation(summary = "delete a file or directory",
          description = "If a directory is targeted then the directory must already be empty.")
    @Produces(MediaType.APPLICATION_JSON)
    @ApiResponses({
          @ApiResponse(responseCode = "401", description = "Unauthorized"),
          @ApiResponse(responseCode = "403", description = "Forbidden"),
          @ApiResponse(responseCode = "404", description = "Not Found"),
          @ApiResponse(responseCode = "500", description = "Internal Server Error"),
    })
    public Response deleteFileEntry(@Parameter(description = "Path of file or directory.", required = true)
    @PathParam("path") String requestPath) throws CacheException {
        PnfsHandler handler = HandlerBuilders.roleAwarePnfsHandler(pnfsmanager);
        FsPath path = pathMapper.asDcachePath(request, requestPath, ForbiddenException::new, handler);

        try {
            handler.deletePnfsEntry(path.toString());

        } catch (FileNotFoundCacheException e) {
            throw new NotFoundException(e);
        } catch (PermissionDeniedCacheException e) {
            if (RequestUser.isAnonymous()) {
                throw new NotAuthorizedException(e);
            } else {
                throw new ForbiddenException(e);
            }
        } catch (JSONException | IllegalArgumentException | CacheException e) {
            throw new BadRequestException(e);
        } catch (Exception e) {
            LOG.warn(Exceptions.meaningfulMessage(e));
            throw new InternalServerErrorException(e);
        }
        return successfulResponse(Response.Status.OK);
    }

    @Autowired
    public void setUseQosService(boolean useQosService) {
        this.useQosService = useQosService;
    }

    private FileQoSRequirements getBasicRequirements(String targetQos, String qosPolicy,
          Integer qosState, FileAttributes attributes) {
        FileQoSRequirements requirements = new FileQoSRequirements(attributes.getPnfsId(),
              attributes);

        if (qosPolicy != null) {
            requirements.setRequiredQoSPolicy(qosPolicy);
            requirements.setRequiredQoSStateIndex( qosState == null ? 0 : qosState);
            return requirements;
        }

        if (targetQos == null) {
            throw new IllegalArgumentException("no target qos given.");
        }

        if (targetQos.toLowerCase(Locale.ROOT).contains("disk")) {
            requirements.setRequiredDisk(1);
        }

        if (targetQos.toLowerCase(Locale.ROOT).contains("tape")) {
            requirements.setRequiredTape(1);
        }

        return requirements;
    }
}
