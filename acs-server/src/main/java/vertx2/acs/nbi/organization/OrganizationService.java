package vertx2.acs.nbi.organization;

import vertx2.*;
import vertx2.acs.nbi.AbstractAcNbiCrudService;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.cwmp.CwmpMessage;
import vertx2.model.*;
import vertx2.util.AcsConfigProperties;
import vertx2.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * Project:  cwmp
 *
 * cwmp Organization Service.
 *
 * @author: ronyang
 */
public class OrganizationService extends AbstractAcNbiCrudService {
    /**
     * Constant(s)
     */
    public static final VertxException INVALID_URL = new VertxException("Invalid URL!");
    public static final VertxException INVALID_HTTP_PROTOCOL = new VertxException("Invalid HTTP Protocol!");
    public static final VertxException INVALID_EXT_IMAGE_SERVER_URL =
            new VertxException("Invalid External Image File Server URL!");
    public static final JsonObject DELETE_EXISTING_IMAGES_FIRST = new JsonObject()
            .putString(
                    AcsConstants.FIELD_NAME_ERROR,
                    "Prior to adding/changing External Image Server, all existing images must be deleted first!"
            );

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return Organization.DB_COLLECTION_NAME;
    }

    /**
     * An Organization is uniquely identified by "name".
     */
    @Override
    public String[] getIndexFieldName() {
        return Organization.INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return Organization.EDITABLE_FIELDS;
    }

    /**
     * Validate an NBI Request.
     *
     * Must be implemented by actual services.
     *
     * If validation is completed, returns true.
     * If validation is not completed (for example pending a further DB query callback), returns false.
     *
     * @param nbiRequest
     * @param crudType      Type of the CRUD operation.
     *
     * @return boolean
     * @throws vertx2.VertxException
     */
    @Override
    public boolean validate(final AcsNbiRequest nbiRequest, final AcsApiCrudTypeEnum crudType) throws VertxException {
        /**
         * Common Validation for both Create and Update
         */
        switch (crudType) {
            case Create:
            case Update:
                JsonObject org = nbiRequest.body;
                VertxJsonUtils.validateFields(org, Organization.MANDATORY_FIELDS, Organization.OPTIONAL_FIELDS);

                /**
                 * Validate URL
                 */
                String urlString = nbiRequest.body.getString(Organization.FIELD_NAME_URL, "");
                try {
                    URL url = new URL(urlString);

                    // Validate HTTP protocol
                    if (AcsConfigProperties.CPE_SERVER_LB_HTTPS_ENABLED &&
                            !"https".equals(url.getProtocol().toLowerCase())) {
                        log.error("HTTPS is enabled by the URL does not have HTTPs as the protocol!");
                        throw INVALID_HTTP_PROTOCOL;
                    }
                    if (!AcsConfigProperties.CPE_SERVER_LB_HTTPS_ENABLED &&
                            !"http".equals(url.getProtocol().toLowerCase())) {
                        log.error("HTTPS is disabled by the URL does not have HTTP as the protocol!");
                        throw INVALID_HTTP_PROTOCOL;
                    }

                    // URL Path cannot start with file server root url
                    if (url.getPath().startsWith(AcsConfigProperties.FILE_SERVER_URL_ROOT)) {
                        log.error("Per-Org ACS URL cannot start with file server root URL!");
                        throw INVALID_URL;
                    }

                    // Rebuild the URL string to a known format
                    int port = url.getPort();
                    if (port <= 0) {
                        port = AcsConfigProperties.CPE_SERVER_LB_PORT;
                    }
                    String newUrlString = url.getProtocol().toLowerCase() + "://" + url.getHost() + ":" + port;
                    if (url.getPath() == null || url.getPath() == "") {
                        newUrlString += "/";
                    } else {
                        newUrlString += url.getPath();
                    }
                    log.info("Converted URL string to " + newUrlString + " (from " + urlString + ")");
                    nbiRequest.body.putString(Organization.FIELD_NAME_URL, newUrlString);
                } catch (MalformedURLException e) {
                    if (urlString.startsWith("/")) {
                        // This is the URL suffix
                    } else {
                        log.error("Invalid URL for organization " + nbiRequest.body.getString(Organization.FIELD_NAME_NAME));
                        throw INVALID_URL;
                    }
                }
                break;
        }

        /**
         * Validate External Image Server if any
         */
        Organization.ExternalFileServer newExtImageServer = null;
        if (nbiRequest.body.containsField(Organization.FIELD_NAME_EXTERNAL_IMAGE_SERVER)) {
            try {
                newExtImageServer = new Organization.ExternalFileServer(
                        nbiRequest.body.getObject(Organization.FIELD_NAME_EXTERNAL_IMAGE_SERVER)
                );
            } catch (MalformedURLException e) {
                throw INVALID_EXT_IMAGE_SERVER_URL;
            }
        }

        /**
         * Further validation
         */
        final String orgId = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
        Organization existingOrg = (Organization)organizationCache.hashMap.get(orgId);
        switch (crudType) {
            case Create:
                if (existingOrg != null) {
                    throw new VertxException("Organization Id " + orgId + " is already in use by "
                            + existingOrg.name + "!");
                }
                break;

            case Update:
                if (existingOrg == null) {
                    throw new VertxException("Organization Id " + orgId + " Not Found!");
                } else {
                    /**
                     * If adding/changing External Image Server, all existing images must be deleted first
                     */
                    String newImageServerUrl = newExtImageServer == null? null : newExtImageServer.baseUrl;
                    String oldImageServerUrl = existingOrg.extImageServer == null?
                            null : existingOrg.extImageServer.baseUrl;
                    if (newImageServerUrl != null && !newImageServerUrl.equals(oldImageServerUrl)) {
                        VertxMongoUtils.count(
                                vertx.eventBus(),
                                AcsFile.DB_COLLECTION_NAME,
                                new JsonObject()
                                    .putString(AcsConstants.FIELD_NAME_ORG_ID, orgId)
                                    .putString(AcsFile.FIELD_NAME_TYPE, AcsFileType.Image.typeString),
                                new Handler<Long>() {
                                    @Override
                                    public void handle(Long count) {
                                        if (count == null) {
                                            log.error("Unable to get count of images for orgId " + orgId + "!");
                                            nbiRequest.sendResponse(
                                                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                    MONGODB_TIMED_OUT
                                            );
                                        } else if (count > 0) {
                                            log.error("org " + orgId + " has " + count + " image(s).");
                                            nbiRequest.sendResponse(
                                                    HttpResponseStatus.BAD_REQUEST,
                                                    DELETE_EXISTING_IMAGES_FIRST
                                            );
                                        } else {
                                            log.info("org " + orgId + " has no image.");
                                            postValidation(nbiRequest, crudType);
                                        }
                                    }
                                }
                        );
                        return VALIDATION_PENDING_OR_FAILED;
                    }
                }
                break;
        }
        return true;
    }

    /**
     * Build a MongoDB Query Matcher with all the index fields.
     *
     * If any index field is missing, respond to client with BAD_REQUEST.
     *
     * @param nbiRequest
     * @param crudType
     *
     * @return  The matcher, or null if the service has no index field.
     *
     * @throws vertx2.VertxException  if one or more index fields are missing.
     */
    @Override
    public JsonObject buildIndexMatcher(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType)
            throws VertxException{
        JsonObject org = nbiRequest.body;

        // Name/URL/API-Client-Username must be unique
        JsonArray or = new JsonArray();
        or.add(new JsonObject().putString(AcsConstants.FIELD_NAME_NAME, org.getString(AcsConstants.FIELD_NAME_NAME)));
        or.add(new JsonObject().putString(Organization.FIELD_NAME_URL, org.getString(Organization.FIELD_NAME_URL)));
        or.add(new JsonObject().putString(Organization.FIELD_NAME_API_CLIENT_USERNAME,
                org.getString(Organization.FIELD_NAME_API_CLIENT_USERNAME)));

        JsonObject indexMatcher = new JsonObject().putArray(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR, or);

        return indexMatcher;
    }

    /**
     * After validation, perform any service specific actions against this request.
     *
     * Default to no action.
     *
     * @param nbiRequest
     * @param crudType      Type of the CRUD operation.
     *
     * @return None
     * @throws vertx2.VertxException
     */
    @Override
    public void preProcess(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
    };

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_ORGANIZATION;
    }

    /**
     * Get the Vert.x Event Bus Address for publishing the CRUD Events to.
     *
     * Default to null (do not publish CRUD events.
     *
     * @return
     */
    @Override
    public String getPublishCrudEventsAddress() {
        return AcsConstants.VERTX_ADDRESS_ACS_ORGANIZATION_CRUD;
    }

    /**
     * Post Delete Handler.
     *
     * Default to no action, but can be override by actual services.
     *
     * @param nbiRequest
     * @param bSucceeded
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    private static final String[] COLLECTIONS_TO_BE_CLEANED_UP = {
            Cpe.CPE_COLLECTION_NAME,
            CwmpMessage.DB_COLLECTION_NAME,
            Event.DB_COLLECTION_NAME,
            CpeDeviceType.DB_COLLECTION_NAME,
            AcsFile.DB_COLLECTION_NAME,
            CpeGroup.DB_COLLECTION_NAME,
            Workflow.DB_COLLECTION_NAME,
            ConfigurationProfile.DB_COLLECTION_NAME,
            ServicePlan.DB_COLLECTION_NAME,
            Subscriber.DB_COLLECTION_NAME
    };
    @Override
    public boolean postDelete(final AcsNbiRequest nbiRequest, boolean bSucceeded) {
        // Clean up
        if (bSucceeded == true) {
            final String orgId = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
            JsonObject matcher = new JsonObject().putString(AcsConstants.FIELD_NAME_ORG_ID, orgId);

            /**
             * Clean up other related DB collections
             */
            for (String collectionName : COLLECTIONS_TO_BE_CLEANED_UP) {
                try {
                    VertxMongoUtils.deleteWithMatcher(
                            vertx.eventBus(),
                            collectionName,
                            matcher,
                            null
                    );
                } catch (VertxException e) {
                    e.printStackTrace();
                }
            }

            /**
             * Delete all files from local file system
             */
            final String fileRoot = AcsFile.getOrgFileRoot(orgId);
            vertx.fileSystem().delete(fileRoot, true,  new Handler<AsyncResult<Void>>(){
                @Override
                public void handle(AsyncResult<Void> event) {
                    if (event.succeeded()) {
                        log.info("Successfully deleted all files for org " + orgId + ". (file root: " + fileRoot + ")");
                    /*
                    } else {
                        log.error(VertxUtils.highlightWithHashes("Failed to delete files for org " + orgId
                                + "! file store root path: " + fileRoot
                                + ". Cause: " + event.cause()));
                    */
                    }
                }
            });

            /**
             * Delete all files from GridFS
             */
            VertxMongoGridFsFile.deleteFilesWithMetadata(
                    vertx.eventBus(),
                    new JsonObject().putString(
                            VertxMongoGridFsVertice.FIELD_NAME_METADATA + "." + AcsConstants.FIELD_NAME_ORG_ID,
                            orgId
                    ),
                    new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> result) {
                            if (result.failed()) {
                                log.error(orgId + ": Failed to delete all files from GridFS! Cause: " + result.cause());
                            } else {
                                log.info(orgId + ": Deleted "
                                        + result.result().body().getNumber(VertxMongoUtils.MOD_MONGO_FIELD_NAME_NUMBER, 0)
                                        + " file(s) from GridFS.");
                            }
                        }
                    }
            );
        }

        return super.postDelete(nbiRequest, bSucceeded);
    }
}
