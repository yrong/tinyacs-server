package vertx.acs.nbi.file;

import io.vertx.core.json.JsonArray;
import vertx.*;
import vertx.acs.nbi.AbstractAcNbiCrudService;
import vertx.acs.nbi.model.AcsNbiRequest;
import vertx.model.*;
import vertx.util.AcsConfigProperties;
import vertx.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.bson.types.ObjectId;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Project:  cwmp
 *
 * ACS API File Services.
 *
 * @author: ronyang
 */
public class FileService extends AbstractAcNbiCrudService{
    /**
     * Index Fields
     */
    private String[] indexFields = {
            AcsConstants.FIELD_NAME_ORG_ID,
            AcsConstants.FIELD_NAME_NAME,
            AcsFile.FIELD_NAME_TYPE
    };

    /**
     * Allowed Query Parameter Name/Type Pairs
     */
    private static final HashMap<String, VertxJsonUtils.JsonFieldType> QUERY_PARAMETER_NAME_TYPE_PAIRS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsFile.FIELD_NAME_TYPE, VertxJsonUtils.JsonFieldType.String);

    /**
     * Start the service
     */
    @Override
    public void start(final Vertx vertx) {
        this.vertx = vertx;

        /**
         * Cleanup log files on startup
         */
        cleanupLogFiles();

        /**
         * Also Start a daily timer to clean up log files that are older than 1 day
         */
        vertx.setPeriodic(
                VertxUtils.ONE_DAY,
                new Handler<Long>() {
                    public void handle(Long event) {
                        cleanupLogFiles();
                    }
                }
        );
    }

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     * <p/>
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */

    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_FILE;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return AcsFile.DB_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     */
    @Override
    public String[] getIndexFieldName() {
        return indexFields;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return AcsFile.EDITABLE_FIELDS;
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
     * @throws vertx.VertxException
     */
    public boolean validate(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
        /**
         * Perform basic Validate Mandatory/Optional Field Types
         */
        switch (crudType) {
            case Create:
            case Update:
                AcsFile.validate(nbiRequest.body);
                break;
        }

        /**
         * TODO: Do not allow updating certain fields such as file name
         */

        return true;
    }

    /**
     * Get all the supported query parameter name/type pairs.
     *
     * @param crudType
     * @return
     */
    @Override
    public HashMap<String, VertxJsonUtils.JsonFieldType> getQueryParameterNameTypePairs(AcsApiCrudTypeEnum crudType) {
        return QUERY_PARAMETER_NAME_TYPE_PAIRS;
    }

    /**
     * Pre-Process the request.
     *
     * @param nbiRequest
     * @param crudType      Type of the CRUD operation.
     *
     * @return None
     * @throws vertx.VertxException
     */
    @Override
    public void preProcess(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
        switch (crudType) {
            case Create:
                // Initialize the file-size and number-of-downloads to 0
                if (!nbiRequest.body.containsKey(AcsFile.FIELD_NAME_TEXT_CONTENT)) {
                    nbiRequest.body.put(AcsFile.FIELD_NAME_SIZE, 0);
                } else {
                    nbiRequest.body.put(AcsFile.FIELD_NAME_SIZE,
                            nbiRequest.body.getString(AcsFile.FIELD_NAME_TEXT_CONTENT).length());
                }

                // Generate Real MongoDB ObjectId (otherwise an UUID will be generated)
                // ObjectId is required by mod-gridfs
                nbiRequest.body.put(AcsConstants.FIELD_NAME_ID, ObjectId.get().toString());

                // Initialize # of downloads to 0
                nbiRequest.body.put(AcsFile.FIELD_NAME_NUMBER_OF_DOWNLOADS, 0);

                // Generate Username/Password for Internal File Server.
                nbiRequest.body
                        .put(AcsFile.FIELD_NAME_USERNAME, AcsFile.getUsername(nbiRequest.body))
                        .put(AcsFile.FIELD_NAME_PASSWORD, AcsFile.getPassword(nbiRequest.body));
                break;
        }
    }

    /**
     * Post Create Handler.
     *
     * Default to no action, but can be override by actual services.
     *
     * @param nbiRequest
     * @param httpResponseStatus
     * @param error         Error String if failed
     * @param id            Id String of the newly created record.
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    @Override
    public boolean postCreate(
            AcsNbiRequest nbiRequest,
            HttpResponseStatus httpResponseStatus,
            String id,
            String error) {
        // Send Response
        if (HttpResponseStatus.OK.equals(httpResponseStatus)) {
            JsonObject customResponse = new JsonObject()
                    .put(AcsConstants.FIELD_NAME_ID, id);
            /**
             * Respond with id and upload URL and credentials
             */
            customResponse
                    .put(AcsFile.FIELD_NAME_UPLOAD_URL, AcsFile.getUploadUrl(id))
                    .put(AcsFile.FIELD_NAME_USERNAME, nbiRequest.body.getString(AcsFile.FIELD_NAME_USERNAME))
                    .put(AcsFile.FIELD_NAME_PASSWORD, nbiRequest.body.getString(AcsFile.FIELD_NAME_PASSWORD));

            /**
             * Is it a SW Image which requires upload?
             */
            if (AcsFileType.Image.typeString.equals(nbiRequest.body.getString(AcsFile.FIELD_NAME_TYPE))) {
                // Check if the Organization has external file server
                String orgId = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ORG_ID);
                Organization org = organizationCache.getOrgById(orgId);
                if (org != null && org.extImageServer != null && org.extImageServer.baseUrl != null) {
                    /**
                     * External File Server
                     */
                    customResponse.put(
                            AcsFile.FIELD_NAME_EXTERNAL_URL,
                            org.extImageServer.baseUrl + AcsFile.getExternalFilePath(nbiRequest.body)
                    );
                    if (org.extImageServer.username != null) {
                        customResponse.put(
                                AcsFile.FIELD_NAME_EXTERNAL_USERNAME,
                                org.extImageServer.username
                        );
                    }
                    if (org.extImageServer.password != null) {
                        customResponse.put(
                                AcsFile.FIELD_NAME_EXTERNAL_PASSWORD,
                                org.extImageServer.password
                        );
                    }
                }
            }

            nbiRequest.sendResponse(HttpResponseStatus.OK, customResponse);
            return true;
        }

        // Fall through to send the default response on errors
        return false;
    }

    /**
     * Custom Post Delete Handler that deletes the associated file from ACS File Store.
     *
     * @param nbiRequest
     * @param bSucceeded
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    @Override
    public boolean postDelete(final AcsNbiRequest nbiRequest, boolean bSucceeded) {
        if (bSucceeded) {
            if (AcsFileType.Image.typeString.equals(nbiRequest.body.getString(AcsFile.FIELD_NAME_TYPE))) {
                /**
                 * Try to delete image from GridFS
                 */
                final String filename = nbiRequest.body.getString(AcsConstants.FIELD_NAME_NAME);
//                VertxMongoGridFsFile.deleteFile(
//                        vertx.eventBus(),
//                        nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID),
//                        new Handler<AsyncResult<Message<JsonObject>>>() {
//
//                            public void handle(AsyncResult<Message<JsonObject>> deleteResult) {
//                                if (deleteResult.failed()) {
//                                    // Send Timeout Response
//                                    log.error(filename + ": Failed to delete from GridFS!");
//                                    nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, MONGODB_TIMED_OUT);
//                                } else {
//                                    // Send "OK" response
//                                    if (deleteResult.result().body()
//                                            .getNumber(VertxMongoUtils.MOD_MONGO_FIELD_NAME_NUMBER, 0).intValue() == 1) {
//                                        log.debug(filename + ": Deleted from GridFS.");
//                                    }
//                                    nbiRequest.sendResponse(HttpResponseStatus.OK);
//                                }
//                            }
//                        }
//                );
            } else {
                /**
                 * Try to delete from local file system
                 */
                final String filename = AcsConfigProperties.ACS_FILE_STORE_PATH + "/" + AcsFile.getFilePath(nbiRequest.body);
                vertx.fileSystem().delete(filename, new Handler<AsyncResult<Void>> (){

                    public void handle(AsyncResult<Void> event) {
                        if (event.succeeded()) {
                            log.info("Successfully deleted " + filename + ".");
                        } else {
                            //log.error("Failed to delete " + filename + "!");
                        }
                    }
                });

                // Send "OK" response
                nbiRequest.sendResponse(HttpResponseStatus.OK);
            }

            return true;
        }

        // Fall through to send the default response on errors
        return false;
    }

    /**
     * Build MongoDB Matcher for Retrieve
     */
    @Override
    public JsonObject buildRetrieveMatcher(AcsNbiRequest nbiRequest) throws VertxException {
        if (nbiRequest.body.containsKey(AcsConstants.FIELD_NAME_ID)) {
            return nbiRequest.body;
        }

        // Convert the CPE Identifier to matcher
        JsonObject cpeIdJsonObject = nbiRequest.body.getJsonObject(AcsConstants.FIELD_NAME_CPE_ID);
        if (cpeIdJsonObject != null) {
            JsonObject matcher = nbiRequest.body.copy();
            String sn = cpeIdJsonObject.getString(CpeIdentifier.FIELD_NAME_SN);
            String oui = cpeIdJsonObject.getString(CpeIdentifier.FIELD_NAME_OUI);
            String mac = cpeIdJsonObject.getString(CpeIdentifier.FIELD_NAME_MAC_ADDRESS);

            matcher.remove(AcsConstants.FIELD_NAME_CPE_ID);
            if (sn != null) {
                matcher.put(AcsConstants.FIELD_NAME_CPE_ID + "." + CpeIdentifier.FIELD_NAME_SN, sn);
            }
            if (oui != null) {
                matcher.put(AcsConstants.FIELD_NAME_CPE_ID + "." + CpeIdentifier.FIELD_NAME_OUI, oui);
            }
            if (mac != null) {
                matcher.put(AcsConstants.FIELD_NAME_CPE_ID + "." + CpeIdentifier.FIELD_NAME_MAC_ADDRESS, mac);
            }

            if (AcsFileType.ConfigFile.typeString.equals(matcher.getString(AcsFile.FIELD_NAME_TYPE))) {
                matcher.put(AcsFile.FIELD_NAME_UPLOAD_TIME, VertxMongoUtils.EXISTS);
                matcher.put(AcsFile.FIELD_NAME_SIZE, VertxMongoUtils.GREATER_THAN_ZERO);
            }
            return matcher;
        } else {
            if (nbiRequest.body.containsKey(AcsConstants.FIELD_NAME_CPE_ID + "." + CpeIdentifier.FIELD_NAME_SN)) {
                return nbiRequest.body.copy()
                        .put(AcsFile.FIELD_NAME_UPLOAD_TIME, VertxMongoUtils.EXISTS)
                        .put(AcsFile.FIELD_NAME_SIZE, VertxMongoUtils.GREATER_THAN_ZERO);
            } else {
                // exclude individual CPE files
                return nbiRequest.body.put(AcsConstants.FIELD_NAME_CPE_ID, VertxMongoUtils.EXISTS_FALSE);
            }
        }
    }

    /**
     * Query Keys
     *
     * Default to return everything except the file content.
     */
    private static final JsonObject DEFAULT_QUERY_KEY = new JsonObject()
            .put(AcsFile.FIELD_NAME_TEXT_CONTENT, 0)
            .put(AcsFile.FIELD_NAME_BINARY_CONTENT, 0);
    private static final JsonObject QUERY_KEY_WITH_CONTENT = new JsonObject()
            .put(AcsFile.FIELD_NAME_BINARY_CONTENT, 0);

    /**
     * Build MongoDB Query Keys for Retrieve.
     *
     * Default to null (return everything)
     */
    @Override
    public JsonObject buildRetrieveQueryKeys(AcsNbiRequest nbiRequest) {
        if (nbiRequest.httpServerRequest != null) {
            if ("true".equalsIgnoreCase(nbiRequest.httpServerRequest.params().get("withContent"))) {
                return QUERY_KEY_WITH_CONTENT;
            }
        }

        return DEFAULT_QUERY_KEY;
    }

    /**
     * Sort query result by "uploadTime" with newest files first
     *
     * @param nbiRequest
     */
    public static final JsonObject SORT_BY_UPLOAD_TIME =
            new JsonObject().put(AcsFile.FIELD_NAME_UPLOAD_TIME, -1);
    @Override
    public JsonObject getDefaultQuerySort(AcsNbiRequest nbiRequest) {
        return SORT_BY_UPLOAD_TIME;
    }

    /**
     * Before returning the query results to the client, call this method to perform additional actions on a per-record
     * basis.
     *
     * Default to no action.
     *
     * @param nbiRequest
     * @param aRecord
     * @return
     */
    @Override
    public JsonObject additionalPostRetrievePerRecordHandler(AcsNbiRequest nbiRequest, JsonObject aRecord) {
        /**
         * Add upload URL
         */
        aRecord.put(
                AcsFile.FIELD_NAME_UPLOAD_URL,
                AcsFile.getUploadUrl(aRecord.getString(AcsConstants.FIELD_NAME_ID))
        );
        return aRecord;
    }

    /**
     * Get a VertxMongoUtils.FindHandler instance for a retrieve request.
     *
     * @param nbiRequest
     * @return
     */
    public Handler getMongoFindHandler(AcsNbiRequest nbiRequest) {
        return new FileRetrieveResultHandler(nbiRequest);
    }

    /**
     * Retrieve Result Handler
     */
    public class FileRetrieveResultHandler implements Handler<List<JsonObject>>{
        AcsNbiRequest nbiRequest;

        /**
         * Constructor that requires an AcsNbiRequest POJO
         */
        public FileRetrieveResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
        }

        /**
         * The handler method body.
         * @param queryResultsList
         */
        @Override
        public void handle(List<JsonObject> queryResultsList) {


            {
                JsonArray queryResults = new JsonArray(queryResultsList);
                queryResults = postRetrieve(nbiRequest, queryResults, false);
                if (queryResults != null) {
                    // send response
                    if (bReturnRetrieveResultInChunkMode(nbiRequest)) {
                        nbiRequest.sendResponseChunk(HttpResponseStatus.OK, queryResults, false);
                    } else {
                        if (queryResults.size() == 0) {
                            if (nbiRequest.bInternalRequest) {
                                // Return additional error message for internal requests
                                nbiRequest.sendResponse(HttpResponseStatus.NOT_FOUND, NO_MATCH_FOUND);
                            } else {
                                nbiRequest.sendResponse(HttpResponseStatus.NOT_FOUND);
                            }
                        } else {
                            final JsonObject aFile = queryResults.getJsonObject(0);

                            if (aFile.getInteger(AcsFile.FIELD_NAME_SIZE, 0) > 0 &&
                                    !aFile.containsKey(AcsFile.FIELD_NAME_EXTERNAL_URL) &&
                                    !aFile.containsKey(AcsFile.FIELD_NAME_TEXT_CONTENT) &&
                                    buildRetrieveQueryKeys(nbiRequest) == QUERY_KEY_WITH_CONTENT &&
                                    !aFile.getString(AcsFile.FIELD_NAME_TYPE).equals(AcsFileType.Image.typeString)) {
                                // Read Config/Log file content from File System
                                final String filePath = AcsFile.getFullFilePath(aFile);
                                vertx.fileSystem().readFile(
                                        filePath,
                                        new Handler<AsyncResult<Buffer>>() {
                                            public void handle(AsyncResult<Buffer> ar) {
                                                if (ar.succeeded()) {
                                                    if (ar.result() != null) {
                                                        aFile.put(AcsFile.FIELD_NAME_TEXT_CONTENT, ar.result().toString());
                                                    }
                                                } else {
                                                    log.error("File " + filePath + " does not exist!");
                                                }

                                                nbiRequest.sendResponse(HttpResponseStatus.OK, aFile);
                                            }
                                        });

                            } else {
                                nbiRequest.sendResponse(HttpResponseStatus.OK, aFile);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * When deleting a single record, allow finding the record (and save it as request body) before deleting it.
     *
     * Disabled by Default.
     *
     * Only applicable when bulk delete is disallowed.
     * @return
     */
    @Override
    public boolean bFindOneBeforeDelete() {
        return true;
    }

    /**
     * Whether Cross Reference Check(s) is needed on Delete.
     *
     * Default to false (i.e. no cross-reference checks are needed).
     */
    @Override
    public boolean doCrossReferenceCheckOnDelete() {
        return true;
    }

    /**
     * Return all the cross-reference checks needed when deleting a profile instance
     *
     * Default to return null (i.e. no cross-reference checks are needed).
     *
     * @param id    Internal id for this profile instance
     * @return      A Sorted Set that contains one or more CrossReferenceCheck instance(s), or null.
     */
    @Override
    public List<CrossReferenceCheck> getAllCrossReferenceChecks(String id) {
        JsonObject matcher = new JsonObject()
                .put(
                        Workflow.FIELD_NAME_ACTIONS,
                        new JsonObject().put(
                                VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_ELEM_MATCH,
                                new JsonObject().put(WorkflowAction.FIELD_NAME_FILE_ID, id)
                        )
                )
                // Do not allow editing if used by a running scheduled workflow
                .put(
                        Workflow.FIELD_NAME_EXEC_POLICY + "."
                                + ExecPolicy.FIELD_NAME_INITIAL_TRIGGER
                                + WorkflowTrigger.FIELD_NAME_TRIGGER_TYPE,
                        WorkflowTrigger.TriggerTypeEnum.MAINTENANCE_WINDOW.typeString
                )
                .put(Workflow.FIELD_NAME_STATE, Workflow.STATE_IN_PROGRESS);

        final CrossReferenceCheck crossReferenceCheck = new CrossReferenceCheck(matcher, Workflow.DB_COLLECTION_NAME);

        return new ArrayList<CrossReferenceCheck>() {{
            add(crossReferenceCheck);
        }};
    }

    /**
     * Get the list of field names that contain MongoDB "$date" timestamp.
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    private static final String[] TIMESTAMP_FIELDS = {
            AcsFile.FIELD_NAME_UPLOAD_TIME
    };
    @Override
    public String[] getDateTimeFieldName() {
        return TIMESTAMP_FIELDS;
    }

    /**
     * Cleanup old log files
     */
    public void cleanupLogFiles() {
        log.info("Performing daily log file cleanup...");
        try {
            VertxMongoUtils.deleteWithMatcher(
                    mongoClient,
                    AcsFile.DB_COLLECTION_NAME,
                    new JsonObject()
                            .put(AcsFile.FIELD_NAME_TYPE, AcsFileType.LogFile.typeString)
                            .put(
                                    AcsFile.FIELD_NAME_UPLOAD_TIME,
                                    new JsonObject().put(
                                            VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_LESS_THAN,
                                            VertxMongoUtils.getDateObject(
                                                    System.currentTimeMillis() - VertxUtils.ONE_DAY
                                            )
                                    )
                            ),
                    null
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }
}
