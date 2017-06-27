package vertx.acs.nbi;

import io.vertx.ext.mongo.MongoClient;
import io.vertx.redis.RedisClient;
import vertx.VertxException;
import vertx.VertxJsonUtils;
import vertx.VertxMongoUtils;
import vertx.VertxUtils;
import vertx.acs.cache.PassiveWorkflowCache;
import vertx.acs.nbi.model.AcsNbiRequest;
import vertx.cache.ConfigurationProfileCache;
import vertx.cache.DialPlanCache;
import vertx.cache.GroupCache;
import vertx.cache.OrganizationCache;
import vertx.model.AcsApiCrudTypeEnum;
import vertx.util.AcsApiUtils;
import vertx.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;

/**
 * Project:  cwmp / CCNG ACS API
 *
 * Abstract/Common/Base CRUD Service.
 *
 * A typical CRUD service would have the following mapping between HTTP Methods and Service Operation Type:
 *
 *  HTTP Method         CRUD Operation Type
 *  GET                 Retrieve
 *  POST                Create
 *  PUT                 Update
 *  DELETE              Delete
 *
 * This class tries to implement as much common functionality as possible.
 *
 * @author: ronyang
 */
public abstract class AbstractAcNbiCrudService implements AcsApiService{
    /**
     * Vert.X Instance.
     *
     * Usually instantiated when calling the start() method.
     */
    public Vertx vertx;

    /**
     * Logger Instance
     */
    public Logger log = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * Organization Cache
     */
    public OrganizationCache organizationCache;

    /**
     * A Local Cache of all passive workflows
     */
    public PassiveWorkflowCache passiveWorkflowCache;

    /**
     * Dial-plan Cache
     */
    public DialPlanCache dialPlanCache;

    /**
     * Configuration Profile Cache
     */
    public ConfigurationProfileCache configurationProfileCache;

    /**
     * CPE Group Cache
     */
    public GroupCache groupCache;

    /**
     * Redis Client
     */
    public RedisClient redisClient;

    public MongoClient mongoClient;

    /**
     * Query Parameter Keywords
     */
    public static final String QUERY_KEYWORD_COUNT = "count";
    public static final String QUERY_KEYWORD_SKIP = "skip";
    public static final String QUERY_KEYWORD_LIMIT = "limit";
    public static final String QUERY_KEYWORD_BRIEF = "brief";

    /**
     * Special/Complex Query Fields that have to be carried within the request body
     */
    public static final String QUERY_OPERATOR_PREFIX = "$";
    public static final String QUERY_OPERATOR_ORDER_BY = QUERY_OPERATOR_PREFIX + "orderby";
    public static final String[] STRING_ARRAY = new String[1];

    /**
     * Commonly Used Objects
     */
    public boolean VALIDATION_SUCCEEDED = true;
    public boolean VALIDATION_PENDING_OR_FAILED = false;
    public static final JsonObject QUERY_KEY_ID_ONLY = new JsonObject().put("_id", 1);
    public static final JsonArray EMPTY_JSON_ARRAY = new JsonArray();
    public static final JsonObject BAD_REQUEST = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR, "Bad Request!");
    public final JsonObject CONFLICT = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR,
                    "One or more index fields contain values currently used by another " + getServiceName());
    public static final JsonObject INVALID_URL_PATH_OR_QUERY_PARAMETERS = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR, "Invalid URL Path and/or Query Parameter(s)!");
    public static final VertxException INVALID_URL_PATH_OR_QUERY_PARAMETERS_EXCEPTION =
            new VertxException("Invalid URL Path and/or Query Parameter(s)!");
    public static final JsonObject MISSING_REQUIRED_FIELD = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR, "Missing Required Field(s)!");
    public final JsonObject MISSING_ID = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR, "Missing " + getServiceName() +" Id!");
    public final VertxException MISSING_ID_EXCEPTION =
            new VertxException("Missing " + getServiceName() +" Id!");
    public static final JsonObject MISSING_ID_OR_FILTER = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR, "Missing Document ID Or Filter(s)!");
    public final JsonObject NO_MATCH_FOUND = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR,
                    "No " + getServiceName() + " found with the given query filter!");
    public static final String INTERNAL_SERVER_ERROR_CONTACT_CALIX =
            "Internal server error! Please contact Calix Support Team.";
    public static final JsonObject RESOURCE_EXISTS = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR, "Trying to create resource that already exists!");
    public static final VertxException MISSING_REQUIRED_FIELD_EXCEPTION =
            new VertxException("Missing Required Field(s)!");
    public static final String MONGODB_TIMED_OUT_STRING =
            "Internal Server Error (ACS DB Timed Out)! Please contact Calix Support Team.";
    public static final JsonObject MONGODB_TIMED_OUT =
            new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, MONGODB_TIMED_OUT_STRING);
    public final JsonObject CROSS_REF_CHECK_FAILED = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR,
                    "Trying to delete a " + getServiceName() + " that is being referenced by other object(s)!");
    public static final VertxException INVALID_QUERY_PARAMETER =
            new VertxException("Invalid Query Parameter(s)!");

    /**
     * Start the service
     */
    @Override
    public void start(Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * Stop the service
     */
    @Override
    public void stop(Vertx vertx) {
    }

    /**
     * Set Organization Cache
     */
    public void setOrganizationCache(OrganizationCache organizationCache) {
        this.organizationCache = organizationCache;
    }

    /**
     * Set Passive Workflow Cache
     *
     * @param passiveWorkflowCache
     */
    @Override
    public void setPassiveWorkflowCache(PassiveWorkflowCache passiveWorkflowCache) {
        this.passiveWorkflowCache = passiveWorkflowCache;
    }

    /**
     * Set Dial Plan Cache
     *
     * @param dialPlanCache
     */
    @Override
    public void setDialPlanCache(DialPlanCache dialPlanCache) {
        this.dialPlanCache = dialPlanCache;
    }

    /**
     * Set Configuration Profile Cache
     *
     * @param configurationProfileCache
     */
    @Override
    public void setConfigurationProfileCache(ConfigurationProfileCache configurationProfileCache) {
        this.configurationProfileCache = configurationProfileCache;
    }

    /**
     * Set Group Cache
     *
     * @param groupCache
     */
    @Override
    public void setGroupCache(GroupCache groupCache) {
        this.groupCache = groupCache;
    }

    /**
     * Set Redis Client
     *
     * @param redisClient
     */
    @Override
    public void setRedisClient(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public void setMongoClient(MongoClient mongoClient) {this.mongoClient = mongoClient;}

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     * @return
     */
    public abstract String getDbCollectionName();

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     *
     * If more than one index fields are defined, they are considered as "AND" relation, i.e. all index fields
     * combined together to represent the unique index.
     */
    public abstract String[] getIndexFieldName();

    /**
     * Is this service for internal uses only?
     *
     * Default to true.
     */
    public boolean bInternalServiceOnly() {
        return true;
    }

    /**
     * Check if all the index fields are present.
     *
     * Return true if all index field are found, or false if one or more are missing.
     *
     * @param nbiRequest
     */
    public boolean checkIndexFields(AcsNbiRequest nbiRequest) {
        String[] indexFields = getIndexFieldName();
        if (indexFields == null) {
            // This service has no index field
            return true;
        }

        for (String anIndexField : getIndexFieldName()) {
            if (!nbiRequest.body.containsKey(anIndexField)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Bulk Delete is not allowed by default.
     *
     * I.e. an "_id" field or all the index fields must be present for delete requests.
     * @return
     */
    public boolean bAllowBulkDelete() {
        return false;
    }

    /**
     * When deleting a single record, allow finding the record (and save it as request body)) before deleting it.
     *
     * Disabled by Default.
     *
     * Only applicable when bulk delete is disallowed.
     * @return
     */
    public boolean bFindOneBeforeDelete() {
        return false;
    }

    /**
     * Process the to-be-deleted record.
     *
     * @param nbiRequest
     * @param matcher
     * @param record
     *
     * @return      If one or more operations are pending, returns true.
     *              If no more pending operations, returns false so the main flow will continue (do the actual delete).
     */
    public boolean processToBeDeletedRecord(
            AcsNbiRequest nbiRequest,
            JsonObject matcher,
            JsonObject record) {
        return false;
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
     * @throws vertx.VertxException  if one or more index fields are missing.
     */
    public JsonObject buildIndexMatcher(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType)
            throws VertxException{
        String[] indexFields = getIndexFieldName();

        if (indexFields == null) {
            // This service has no index field
            return null;
        }

        // Build the matcher
        JsonObject indexMatcher = new JsonObject();
        for (String anIndexField : getIndexFieldName()) {
            if (!nbiRequest.body.containsKey(anIndexField)) {
                log.error("Missing index field " + anIndexField + "!");
                throw MISSING_REQUIRED_FIELD_EXCEPTION;
            } else {
                VertxJsonUtils.append(indexMatcher, anIndexField, nbiRequest.body.getValue(anIndexField));
            }
        }

        return indexMatcher;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    public abstract List<String> getEditableFields();

    /**
     * Get Default CRUD Type when a method string is not present.
     *
     * Only applicable for Vert.x Event Bus Based Requests.
     *
     * Default to "Unknown" (i.e. a valid "method" must be specified)
     */
    public AcsApiCrudTypeEnum getDefaultCrudType() {
        return AcsApiCrudTypeEnum.Unknown;
    }

    /**
     * Get the Vert.x Event Bus Address for publishing the CRUD Events to.
     *
     * Default to null (do not publish CRUD events.
     *
     * @return
     */
    public String getPublishCrudEventsAddress() {
        return null;
    }

    /**
     * Process HTTP URL Path parameters.
     *
     * @param pathParams
     * @param nbiRequest
     */
    public void processPathParameters(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType, String[] pathParams)
            throws VertxException {
        if (pathParams.length > 3) {
            // Save the path params
            nbiRequest.urlPathParams = new String[pathParams.length - 3];
            for (int i = 3; i < pathParams.length; i ++) {
                nbiRequest.urlPathParams[i-3] = pathParams[i];
            }

            // Process the first one
            switch (nbiRequest.urlPathParams[0]) {
                case QUERY_KEYWORD_COUNT:
                    // Get total count. For retrievals only
                    if (!crudType.equals(AcsApiCrudTypeEnum.Retrieve)) {
                        throw INVALID_URL_PATH_OR_QUERY_PARAMETERS_EXCEPTION;
                    }
                    break;

                default:
                    nbiRequest.body.put(AcsConstants.FIELD_NAME_ID, nbiRequest.urlPathParams[0]);
            }
        }
    }

    /**
     * Get all the supported query parameter name/type pairs.
     *
     * The followings are reserved:
     *
     * - orgId
     * - skip
     * - limit
     *
     * @param crudType
     * @return
     */
    public HashMap<String, VertxJsonUtils.JsonFieldType> getQueryParameterNameTypePairs(AcsApiCrudTypeEnum crudType) {
        return null;
    }

    /**
     * Process HTTP URL Query parameters.
     *
     * @param nbiRequest
     */
    public void processQueryParameters(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
        if (nbiRequest.httpServerRequest.params().size() == 0) {
            return;
        }

        HashMap<String, VertxJsonUtils.JsonFieldType> allowedQueryParams = getQueryParameterNameTypePairs(crudType);

        // Extract all query parameters as key/values pairs (values are strings)
        for (Map.Entry<String, String> entry : nbiRequest.httpServerRequest.params().entries()) {
            String name = entry.getKey();
            String stringValue = entry.getValue();
            //log.debug(name + ": " + stringValue);
            // Default to String (which may be overly flexible)
            VertxJsonUtils.JsonFieldType type = null;
            Boolean bSaveToBody = null;

            switch (name) {
                case AcsConstants.FIELD_NAME_ORG_ID:
                    break;

                case QUERY_KEYWORD_SKIP:
                case QUERY_KEYWORD_LIMIT:
                    bSaveToBody = false;
                    type = VertxJsonUtils.JsonFieldType.Integer;
                    break;

                case QUERY_KEYWORD_BRIEF:
                    bSaveToBody = false;
                    type = VertxJsonUtils.JsonFieldType.Boolean;
                    break;

                default:
                    if (allowedQueryParams != null) {
                        type = allowedQueryParams.get(name);
                    }
                    bSaveToBody = true;
                    break;
            }

            if (bSaveToBody == null) {
                // orgId
                continue;
            }

            Object value = null;

            // Add this key/value pair into request body as proper type
            if (type == null) {
                // This may be overly flexible
                type = VertxJsonUtils.JsonFieldType.String;
            }
            switch (type) {
                case String:
                    value = stringValue;
                    break;

                case Integer:
                    int intValue;
                    try {
                        intValue = Integer.valueOf(stringValue);
                    } catch (Exception ex) {
                        log.error("Expecting integer value for query parameter " + name + "!");
                        throw INVALID_QUERY_PARAMETER;
                    }
                    value = intValue;
                    break;

                case Boolean:
                    boolean booleanValue;
                    try {
                        booleanValue = Boolean.valueOf(stringValue);
                    } catch (Exception ex) {
                        log.error("Expecting boolean value for query parameter " + name + "!");
                        throw INVALID_QUERY_PARAMETER;
                    }
                    value = booleanValue;
                    break;

                default:
                    /**
                     * TODO: Json Object/Array are not supported via query parameters
                     */
                    break;
            }

            if (value != null) {
                if (bSaveToBody == false && nbiRequest.queryParameters == null) {
                    nbiRequest.queryParameters = new JsonObject();
                }

                VertxJsonUtils.append(
                        bSaveToBody ? nbiRequest.body : nbiRequest.queryParameters,
                        name,
                        value
                );
            }
        }

        /**
         * Also Check for special query related fields in the request body such as "$sort"
         */
        for (String fieldName : nbiRequest.body.fieldNames().toArray(STRING_ARRAY)) {
            if (fieldName.startsWith(QUERY_OPERATOR_PREFIX)) {
                if (nbiRequest.queryParameters == null) {
                    nbiRequest.queryParameters = new JsonObject();
                }
                nbiRequest.queryParameters.put(fieldName, nbiRequest.body.getValue(fieldName));
                nbiRequest.body.remove(fieldName);
            }
        }
    }

    /**
     * Validate an NBI Request.
     *
     * Must be implemented by actual services.
     *
     * If authentication is needed, this is the right place to implement.
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
    public abstract boolean validate(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException;

    /**
     * After validation, perform any service specific actions against this request.
     *
     * Default to no action.
     *
     * @param nbiRequest
     * @param crudType      Type of the CRUD operation.
     *
     * @return None
     * @throws vertx.VertxException
     */
    public void preProcess(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {

    }

    /**
     * Handle a new request received via either Vert.x Event Bus or HTTP.
     *
     * @param nbiRequest
     * @param pathParams An array of strings that holds all the URL path parameters if any. The first two array
     *                      element are the API context root ("cc") and the service name.
     */
    @Override
    public void handle(AcsNbiRequest nbiRequest, String[] pathParams) {
        /**
         * Reject if accessing internal service from outside
         */
        if (nbiRequest.bInternalRequest == false && bInternalServiceOnly()) {
            log.error("Remote host " + nbiRequest.httpServerRequest.remoteAddress().host()
                    + " is trying to access internal service " + getServiceName() + "!");

            /**
             * TODO: Do we want to log this incident?
             */

            nbiRequest.sendResponse(HttpResponseStatus.FORBIDDEN);
            return;
        }

        /**
         * Get CRUD Operation Type
         */
        String methodString;
        AcsApiCrudTypeEnum crudType;
        if (nbiRequest.httpServerRequest != null) {
            methodString = nbiRequest.httpServerRequest.rawMethod();
        } else {
            methodString = nbiRequest.body.getString(AcsConstants.FIELD_NAME_METHOD);
            nbiRequest.body.remove(AcsConstants.FIELD_NAME_METHOD);
        }
        crudType = getCrudTypeByMethodString(methodString);
        if (crudType == AcsApiCrudTypeEnum.Unknown) {
            nbiRequest.sendResponse(
                    HttpResponseStatus.BAD_REQUEST,
                    new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, "Unexpected Method " + methodString));
            return;
        }

        /**
         * Process HTTP Path/Query Parameters if any
         */
        if (nbiRequest.httpServerRequest != null) {
            try {
                processPathParameters(nbiRequest, crudType, pathParams);
                processQueryParameters(nbiRequest, crudType);
            } catch (VertxException ex) {
                log.error("Caught exception " + ex.getMessage() + " while processing URL "
                        + nbiRequest.httpServerRequest.absoluteURI() + "!");
                nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, INVALID_URL_PATH_OR_QUERY_PARAMETERS);
                return;
            }

        }

        /**
         * Validate the request body
         */
        boolean bValidationCompleted = false;
        try {
            bValidationCompleted = validate(nbiRequest, crudType);
        } catch (VertxException e) {
            log.error("Caught VertxException " + e.getMessage() + " while processing " + crudType.name() + " request!\n"
                    + nbiRequest.body.encodePrettily());
            nbiRequest.sendResponse(
                    HttpResponseStatus.BAD_REQUEST,
                    new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, e.getMessage()));
            return;
        }

        /**
         * Continue if validation is completed
         */
        if (bValidationCompleted == true) {
            postValidation(nbiRequest, crudType);
        }
    }

    /**
     * The process that is done after validation.
     * @param nbiRequest
     * @param crudType
     */
    public void postValidation(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) {
        /**
         * PreProcess the request body
         */
        try {
            preProcess(nbiRequest, crudType);
        } catch (VertxException e) {
            log.error("Caught VertxException " + e.getMessage() + " while pre-processing " + crudType.name() + " request!\n"
                    + nbiRequest.body.encodePrettily());
            nbiRequest.sendResponse(
                    HttpResponseStatus.BAD_REQUEST,
                    new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, e.getMessage()));
            return;
        }

        /**
         * Process based on CRUD type
         */
        switch (crudType) {
            case Create:
                handleCreate(nbiRequest);
                break;

            case Retrieve:
                handleRetrieve(nbiRequest);
                break;

            case Update:
                handleUpdate(nbiRequest);
                break;

            case Delete:
                handleDelete(nbiRequest);
                break;
        }
    }

    /**
     * Should the retrieve results be sent to clients in chunk mode?
     */
    public boolean bReturnRetrieveResultInChunkMode(AcsNbiRequest nbiRequest) {
        if (nbiRequest.body.containsKey(AcsConstants.FIELD_NAME_ID)) {
            // Return a single record if querying with ID
            return false;
        } else {
            // Return JSON Array if no id is present in the query filter
            return true;
        }
    }

    /**
     * For bulk query, get the default "sort" JSON Object on how to sort the results.
     *
     * @param nbiRequest
     * @return  Default to null (let MongoDB to sort it)
     */
    public JsonObject getDefaultQuerySort(AcsNbiRequest nbiRequest) {
        return null;
    }

    /**
     * For bulk query, get the "$sort" Object from the request body if any; or return the default sort if "$sort" is not
     * found in the request body.
     *
     * @param nbiRequest
     * @return  Default to null (let MongoDB to sort it)
     */
    public JsonObject getQuerySort(AcsNbiRequest nbiRequest) {
        Object sort = nbiRequest.getQueryParam(QUERY_OPERATOR_ORDER_BY);
        if (sort != null && sort instanceof JsonObject) {
            return (JsonObject)sort;
        } else {
            return getDefaultQuerySort(nbiRequest);
        }
    }

    /**
     * For bulk query, get the # of records to be skipped.
     *
     * @param nbiRequest
     * @return  Default to 0 (skip nothing)
     */
    public int getQuerySkipCount(AcsNbiRequest nbiRequest) {
        return nbiRequest.getQuerySkipCount();
    }

    /**
     * For bulk query, get the max # of records to be returned.
     *
     * @param nbiRequest
     * @return  Default to -1 (return everything, no limit)
     */
    public int getQueryLimitCount(AcsNbiRequest nbiRequest) {
        return nbiRequest.getQueryLimitCount();
    }

    /**
     * Default Retrieve Request Handler.
     *
     * @param nbiRequest
     */
    public void handleRetrieve(final AcsNbiRequest nbiRequest) {
        if (nbiRequest.urlPathParams != null && nbiRequest.urlPathParams[0].equals(QUERY_KEYWORD_COUNT)) {
            /**
             * Send the count request
             */
            try {
                VertxMongoUtils.count(
                        mongoClient,
                        getDbCollectionName(),
                        buildRetrieveMatcher(nbiRequest),
                        new Handler<Long>() {
                            @Override
                            public void handle(Long count) {
                                if (count == null) {
                                    nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, MONGODB_TIMED_OUT);
                                } else {
                                    nbiRequest.httpServerRequest.response().putHeader("Content-Type", "application/json");
                                    nbiRequest.sendResponse(
                                            HttpResponseStatus.OK,
                                            new JsonObject().put(QUERY_KEYWORD_COUNT, count)
                                    );
                                }
                            }
                        }
                );
            } catch (VertxException e) {
                e.printStackTrace();
                nbiRequest.sendResponseChunk(HttpResponseStatus.OK, null, false);
            }
        } else {
            /**
             * Regular Retrieval
             */

            /**
             * Set HTTP Headers (Chunk Mode and Content Type)
             */
            if (nbiRequest.httpServerRequest != null) {
                nbiRequest.httpServerRequest.response().putHeader("Content-Type", "application/json");
                VertxUtils.setResponseStatus(nbiRequest.httpServerRequest, HttpResponseStatus.OK);
                if (bReturnRetrieveResultInChunkMode(nbiRequest)) {
                    nbiRequest.httpServerRequest.response().setChunked(true);
                    nbiRequest.httpServerRequest.response().write("[");
                }
            }

            /**
             * Send the query
             */
            try {
                VertxMongoUtils.find(
                        mongoClient,
                        getDbCollectionName(),
                        buildRetrieveMatcher(nbiRequest),
                        getQuerySort(nbiRequest),
                        getQuerySkipCount(nbiRequest),
                        getQueryLimitCount(nbiRequest),
                        getMongoFindHandler(nbiRequest),
                        buildRetrieveQueryKeys(nbiRequest),
                        null
                );
            } catch (VertxException e) {
                e.printStackTrace();
                nbiRequest.sendResponseChunk(HttpResponseStatus.OK, null, false);
            }
        }
    }

    /**
     * Default Create Request Handler.
     *
     * @param nbiRequest
     */
    public void handleCreate(AcsNbiRequest nbiRequest) {
        /*
        // The Create Request cannot have the "_id" field
        if (nbiRequest.body.containsField(AcsConstants.FIELD_NAME_ID)) {
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, CREATE_REQ_CANNOT_HAVE_ID);
            return;
        }
        */

        // Build matcher by index fields if any
        JsonObject indexMatcher = null;
        try {
            indexMatcher = buildIndexMatcher(nbiRequest, AcsApiCrudTypeEnum.Create);
        } catch (VertxException e) {
            log.error("Create request is missing one or more index field(s)!");
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, MISSING_REQUIRED_FIELD);
            return;
        }

        try {
            if (indexMatcher != null) {
                /**
                 * First Query with the index field, so we will not overwrite existing document
                 */
                VertxMongoUtils.findOne(
                        mongoClient,
                        getDbCollectionName(),
                        indexMatcher,
                        getFindBeforeCreateResultHandler(nbiRequest),
                        null
                );
            } else {
                /**
                 * No index fields means that the service allows duplicated records
                 *
                 * Save without querying the DB.
                 */
                VertxMongoUtils.save(
                        mongoClient,
                        getDbCollectionName(),
                        nbiRequest.body,
                        getMongoSaveHandler(nbiRequest)
                );
            }
        } catch (VertxException e) {
            e.printStackTrace();
            nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    getServerInternalErrorWithDetails());
        }
    }

    /**
     * Default Update Request Handler.
     *
     * @param nbiRequest
     */
    public void handleUpdate(AcsNbiRequest nbiRequest) {
        // "_id" is mandatory for update
        String id = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
        if (id == null) {
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, MISSING_ID);
            return;
        }
        JsonObject idMatcher = new JsonObject().put(AcsConstants.FIELD_NAME_ID, id);

        // Get index matcher by index fields if any
        JsonObject indexMatcher = null;
        try {
            indexMatcher = buildIndexMatcher(nbiRequest, AcsApiCrudTypeEnum.Update);
        } catch (VertxException e) {
            log.error("Caught exception " + e.getMessage());
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, MISSING_REQUIRED_FIELD);
            return;
        }

        // Get the final matcher
        JsonObject matcher;
        if (indexMatcher == null) {
            // Match by id only
            matcher = idMatcher;
        } else {
            // Match by id OR index fields
            if (indexMatcher.containsKey(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR)) {
                // The index matcher already has an "$or" array
                // add the "_id" into the array
                indexMatcher.getJsonArray(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR).add(idMatcher);
                matcher = indexMatcher;
            } else {
                JsonArray or = new JsonArray()
                        .add(idMatcher)
                        .add(indexMatcher);
                matcher = new JsonObject().put(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR, or);
            }
        }

        /**
         * Do Query with the index field, so we only overwrite existing document
         */
        try {
            VertxMongoUtils.find(
                    mongoClient,
                    getDbCollectionName(),
                    matcher,
                    null,   // sort
                    0,      // skip
                    2,      // limit to 2 records
                    getFindBeforeUpdateResultHandler(nbiRequest),
                    null,        // null query key means returns everything
                    2       // batch size
            );
        } catch (VertxException e) {
            e.printStackTrace();
            nbiRequest.sendResponse(
                    HttpResponseStatus.BAD_REQUEST,
                    NO_MATCH_FOUND);
        }
    }

    /**
     * Default Delete Request Handler.
     *
     * @param nbiRequest
     */
    public void handleDelete(AcsNbiRequest nbiRequest) {
        /**
         * Is Cross Reference Check needed?
         */
        if (doCrossReferenceCheckOnDelete()) {
            String id = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
            if (id == null) {
                String error = "Id is required when Deleting " + getServiceName() + "!";
                log.error(error);
                nbiRequest.sendResponse(
                        HttpResponseStatus.BAD_REQUEST,
                        new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, error)
                );
            } else {

                // Build a list of checks as well as a handler, and kicks off the first check
                CrossReferenceCheckResultHandler handler = new CrossReferenceCheckResultHandler(
                        nbiRequest, AcsApiCrudTypeEnum.Delete, null, getAllCrossReferenceChecks(id)
                );
                if (handler.allChecks.size() > 0) {
                    handler.allChecks.get(0).runCheck(handler);
                }
            }
        } else {
            /**
             * Delete now
             */
            doDelete(nbiRequest);
        }
    }

    /**
     * Perform the actual Delete.
     *
     * @param nbiRequest
     */
    public void doDelete(final AcsNbiRequest nbiRequest) {
        // Get matcher
        final JsonObject matcher;
        if (!bAllowBulkDelete()) {
            // The "_id" is mandatory for deleting a single entry
            final String id = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
            if (id == null) {
                nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, MISSING_ID_OR_FILTER);
                return;
            }
            matcher = new JsonObject().put(AcsConstants.FIELD_NAME_ID, id);

            if (bFindOneBeforeDelete()) {
                // Find before delete
                try {
                    VertxMongoUtils.findOne(
                            mongoClient,
                            getDbCollectionName(),
                            matcher,
                            new Handler<JsonObject>() {
                                @Override
                                public void handle(JsonObject queryResult) {
                                    if (queryResult == null || queryResult.size() == 0) {
                                        getMongoDeleteHandler(nbiRequest).handle(null);
                                    } else {
                                        // Save the to-be-deleted record as the request body
                                        nbiRequest.body = queryResult;

                                        // Perform service specific process before delete
                                        boolean pending = processToBeDeletedRecord(
                                                nbiRequest,
                                                matcher,
                                                queryResult);

                                        // Delete it
                                        if (pending == false) {
                                            doDeleteNow(nbiRequest, matcher);
                                        }
                                    }
                                }
                            }
                            ,
                            null
                    );
                } catch (VertxException e) {
                    e.printStackTrace();
                }
                return;
            }
        } else {
            matcher = nbiRequest.body;
        }

        // Delete it
        try {
            VertxMongoUtils.deleteWithMatcher(
                    mongoClient,
                    getDbCollectionName(),
                    matcher,
                    getMongoDeleteHandler(nbiRequest)
            );
        } catch (Exception e) {
            e.printStackTrace();
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, BAD_REQUEST);
        }
    }

    /**
     * Perform the actual Delete now.
     *
     * @param nbiRequest
     */
    public void doDeleteNow(final AcsNbiRequest nbiRequest, final JsonObject matcher) {
        // Delete it
        try {
            VertxMongoUtils.deleteWithMatcher(
                    mongoClient,
                    getDbCollectionName(),
                    matcher,
                    getMongoDeleteHandler(nbiRequest)
            );
        } catch (Exception e) {
            e.printStackTrace();
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, BAD_REQUEST);
        }
    }

    /**
     * Build MongoDB Matcher for Retrieve
     */
    public JsonObject buildRetrieveMatcher(AcsNbiRequest nbiRequest) throws VertxException{
        return nbiRequest.body;
    }

    /**
     * Build MongoDB Query Keys for Retrieve.
     *
     * Default to null (return everything)
     */
    public JsonObject buildRetrieveQueryKeys(AcsNbiRequest nbiRequest) {
        return null;
    }

    /**
     * Get a VertxMongoUtils.FindHandler instance for a retrieve request.
     *
     * @param nbiRequest
     * @return
     */
    public Handler getMongoFindHandler(AcsNbiRequest nbiRequest) {
        return new RetrieveResultHandler(nbiRequest);
    }

    /**
     * Retrieve Result Handler
     */
    public class RetrieveResultHandler implements Handler<List<JsonObject>>{
        AcsNbiRequest nbiRequest;

        /**
         * Constructor that requires an AcsNbiRequest POJO
         */
        public RetrieveResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
        }

        /**
         * The handler method body.
         * @param mongo_query_results
         */
        @Override
        public void handle(List<JsonObject> mongo_query_results) {
            // Call super
            JsonArray queryResults = new JsonArray(mongo_query_results);
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
                        JsonObject firstResult = queryResults.getJsonObject(0);
                        nbiRequest.sendResponse(HttpResponseStatus.OK, firstResult);
                    }
                }
            }
        }
    }

    /**
     * Post Retrieve Handler.
     *
     * Default to only convert MongoDB "$date" to Strings, but can be override by actual services.
     *
     * @param nbiRequest
     * @param queryResults
     * @param moreExists
     *
     * @return  The processed query results as a JSON Array, or null if there is more work to be done.
     */
    public JsonArray postRetrieve(AcsNbiRequest nbiRequest, JsonArray queryResults, boolean moreExists) {
        if (queryResults != null &&
                queryResults.size() > 0) {
            JsonArray newResults = new JsonArray();
            for (int i=0; i < queryResults.size(); i ++) {
                JsonObject aRecord = queryResults.getJsonObject(i);

                if (getDateTimeFieldName() != null) {
                    // Convert the timestamp JSON Object to string
                    for (String dateTimeFieldName : getDateTimeFieldName()) {
                        if (aRecord.containsKey(dateTimeFieldName)) {
                            try {
                                VertxJsonUtils.convertMongoDateToString(aRecord, dateTimeFieldName);
                            } catch (Exception ex) {
                                log.error("Field " + dateTimeFieldName
                                        + " does not contain a MongoDB \"$date\"! ("
                                        + aRecord.getValue(dateTimeFieldName).toString() + ")");
                            }
                        }
                    }
                }

                // perform additional custom actions
                newResults.add(additionalPostRetrievePerRecordHandler(nbiRequest, aRecord));
            }

            // update the query results with converted results
            return newResults;
        } else if (queryResults == null) {
            return EMPTY_JSON_ARRAY;
        } else {
            return queryResults;
        }
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
    public JsonObject additionalPostRetrievePerRecordHandler(AcsNbiRequest nbiRequest, JsonObject aRecord) {
        return aRecord;
    }

    /**
     * Get the list of field names that contain MongoDB "$date" timestamp.
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    public String[] getDateTimeFieldName() {
        return null;
    }

    /**
     * Get a VertxMongoUtils.SaveHandler instance for a retrieve request.
     *
     * @param nbiRequest
     * @return
     */
    public Handler getMongoSaveHandler(AcsNbiRequest nbiRequest) {
        return new CreateResultHandler(nbiRequest);
    }

    /**
     * Create Result Handler
     */
    public class CreateResultHandler implements Handler<String>{
        AcsNbiRequest nbiRequest;
        JsonObject newDoc;

        /**
         * Constructor that requires an AcsNbiRequest POJO
         */
        public CreateResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
            this.newDoc = nbiRequest.body;
        }

        /**
         * Default no-arg Constructor.
         */
        public CreateResultHandler() {
        }

        /**
         * The handler method body.
         * @param saveResult
         */
        @Override
        public void handle(String saveResult) {
            // Call super


            String id = saveResult;


            nbiRequest.body.put(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, saveResult);


            // Get Response Status Code and Error Message (if any)
            HttpResponseStatus responseStatus = HttpResponseStatus.OK;
            String error = null;

            if (postCreate(nbiRequest, responseStatus, id, error) == false) {
                // send response
                sendCreateResponse(nbiRequest, responseStatus, id, error);
            }
        }
    }

    /**
     * Send Response for a Creation Request.
     *
     * @param nbiRequest
     * @param responseStatus
     * @param id
     * @param error
     */
    public void sendCreateResponse(
            AcsNbiRequest nbiRequest,
            HttpResponseStatus responseStatus,
            String id,
            String error) {
        if (HttpResponseStatus.OK.equals(responseStatus)) {
            if (nbiRequest.bInternalRequest) {
                nbiRequest.sendResponse(HttpResponseStatus.OK,
                        new JsonObject().put(AcsConstants.FIELD_NAME_ID, id));
            } else {
                nbiRequest.sendResponse(HttpResponseStatus.OK, id);
            }
        } else {
            nbiRequest.sendResponse(responseStatus, new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, error));
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
    public boolean postCreate(
            AcsNbiRequest nbiRequest,
            HttpResponseStatus httpResponseStatus,
            String id,
            String error) {
        // Publish CRUD Event
        if (httpResponseStatus.equals(HttpResponseStatus.OK) == true) {
            publishCrudEvent(nbiRequest, AcsApiCrudTypeEnum.Create);
        }

        return false;
    }

    /**
     * Get a VertxMongoUtils.SaveHandler instance for an update request.
     *
     * Although it is an "Update" request, we are using the MongoDB's "save" method to overwrite existing documents
     *
     * @param nbiRequest
     * @return
     */
    public Handler getMongoUpdateHandler(AcsNbiRequest nbiRequest) {
        return new UpdateResultHandler(nbiRequest);
    }

    /**
     * Update Result Handler
     */
    public class UpdateResultHandler implements Handler<String>{
        AcsNbiRequest nbiRequest;
        JsonObject newDoc;

        /**
         * Constructor that requires an AcsNbiRequest POJO
         */
        public UpdateResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
            this.newDoc = nbiRequest.body;
        }

        /**
         * The handler method body.
         * @param saveResult
         */
        @Override
        public void handle(String saveResult) {
            // Call super


            if (postUpdate(nbiRequest, HttpResponseStatus.OK, null) == false) {
                // send response
                sendUpdateResponse(nbiRequest, HttpResponseStatus.OK, null);
            }
        }
    }

    /**
     * Send HTTP Response for an update request.
     *
     * @param nbiRequest
     * @param responseStatus
     * @param error
     */
    public void sendUpdateResponse(
            AcsNbiRequest nbiRequest,
            HttpResponseStatus responseStatus,
            String error) {
        if (HttpResponseStatus.OK.equals(responseStatus)) {
            nbiRequest.sendResponse(HttpResponseStatus.OK);
        } else {
            nbiRequest.sendResponse(responseStatus, new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, error));
        }
    }

    /**
     * Post Update Handler.
     *
     * Default to no action, but can be override by actual services.
     *
     * @param nbiRequest
     * @param responseStatus
     * @param error
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    public boolean postUpdate(
            AcsNbiRequest nbiRequest,
            HttpResponseStatus responseStatus,
            String error) {
        // Publish CRUD Event
        if (HttpResponseStatus.OK.equals(responseStatus) == true) {
            publishCrudEvent(nbiRequest, AcsApiCrudTypeEnum.Update);
        }

        return false;
    }

    /**
     * Get a VertxMongoUtils.DeleteHandler instance for a delete request.
     *
     * @param nbiRequest
     * @return
     */
    public Handler getMongoDeleteHandler(AcsNbiRequest nbiRequest) {
        return new DeleteResultHandler(nbiRequest);
    }

    /**
     * Delete Result Handler
     */
    public class DeleteResultHandler implements Handler<JsonObject>{
        AcsNbiRequest nbiRequest;

        /**
         * Constructor that requires an AcsNbiRequest POJO
         */
        public DeleteResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
        }

        /**
         * The handler method body.
         * @param jsonObjectMessage
         */
        @Override
        public void handle(JsonObject jsonObjectMessage) {


            // Determine succeed/fail by checking the number of record
            boolean bSucceeded = true;


            if (postDelete(nbiRequest, bSucceeded) == false) {
                // send response
                if (bSucceeded) {
                    if (bReturnNumberOfRecordsDeleted(nbiRequest)) {
                        nbiRequest.sendResponse(
                                HttpResponseStatus.OK,
                                new JsonObject().put(AcsConstants.FIELD_NAME_NBR_OF_RECORDS, 1)
                        );
                    } else {
                        nbiRequest.sendResponse(HttpResponseStatus.OK);
                    }
                } else {
                    if (bReturnNumberOfRecordsDeleted(nbiRequest)) {
                        nbiRequest.sendResponse(HttpResponseStatus.NOT_FOUND, NO_MATCH_FOUND);
                    } else {
                        nbiRequest.sendResponse(HttpResponseStatus.NOT_FOUND);
                    }
                }
            }
        }
    }

    /**
     * Shall we return the # of records deleted?
     * @return  Default to true for internal requests, and false for external requests.
     */
    public boolean bReturnNumberOfRecordsDeleted(AcsNbiRequest nbiRequest) {
        return nbiRequest.bInternalRequest;
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
    public boolean postDelete(AcsNbiRequest nbiRequest, boolean bSucceeded) {
        // Publish CRUD Event
        if (bSucceeded == true) {
            publishCrudEvent(nbiRequest, AcsApiCrudTypeEnum.Delete);
        }

        return false;
    }

    /**
     * Get a FindBeforeCreateResultHandler instance.
     * @param nbiRequest
     * @return
     */
    public Handler getFindBeforeCreateResultHandler(AcsNbiRequest nbiRequest) {
        return new FindBeforeCreateResultHandler(nbiRequest);
    }

    /**
     * Find Result Handler (used by Create)
     */
    public class FindBeforeCreateResultHandler implements Handler<JsonObject>{
        AcsNbiRequest nbiRequest;

        /**
         * Constructor that requires an AcsNbiRequest POJO
         */
        public FindBeforeCreateResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
        }

        /**
         * The handler method body.
         * @param queryResult
         */
        @Override
        public void handle(JsonObject queryResult) {

            // Any match found?
            if (VertxMongoUtils.FIND_ONE_TIMED_OUT.equals(queryResult)) {
                // MongoDB Timed Out
                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, MONGODB_TIMED_OUT);
            } else if (queryResult != null && queryResult.containsKey(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID)) {
                // Found existing document, respond with error
                log.error("Found an existing document with the same index fields!\n" + queryResult.encodePrettily());
                nbiRequest.sendResponse(
                        HttpResponseStatus.CONFLICT,
                        getConflictDetails(nbiRequest, AcsApiCrudTypeEnum.Create, queryResult)
                );
            } else {
                // Ok to create new record
                log.info("Creating new document with\n" + nbiRequest.body.encodePrettily());
                try {
                    VertxMongoUtils.save(
                            mongoClient,
                            getDbCollectionName(),
                            nbiRequest.body,
                            getMongoSaveHandler(nbiRequest)
                    );
                } catch (VertxException e) {
                    e.printStackTrace();
                    nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            getServerInternalErrorWithDetails());
                }
            }
        }
    }

    /**
     * Get a FindBeforeUpdateResultHandler instance.
     * @param nbiRequest
     * @return
     */
    public Handler getFindBeforeUpdateResultHandler(AcsNbiRequest nbiRequest) {
        return new FindBeforeUpdateResultHandler(nbiRequest);
    }

    /**
     * Find Result Handler (used by update)
     */
    public class FindBeforeUpdateResultHandler implements Handler<List<JsonObject>>{
        AcsNbiRequest nbiRequest;

        /**
         * Constructor that requires an AcsNbiRequest POJO
         */
        public FindBeforeUpdateResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
        }

        /**
         * The handler method body.
         * @param queryResults
         */
        @Override
        public void handle(List<JsonObject> queryResults) {
            // Call super

            String id = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);

            // Any match found?
            if (queryResults.equals(VertxMongoUtils.FIND_TIMED_OUT)) {
                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, MONGODB_TIMED_OUT);
                return;
            }

            if (queryResults.size() == 1) {
                // Found only one document
                JsonObject oldRecord = queryResults.get(0);

                if (!id.equals(oldRecord.getString(AcsConstants.FIELD_NAME_ID))) {
                    log.error("No match found by id " + nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID) + "!");
                    nbiRequest.sendResponse(HttpResponseStatus.NOT_FOUND, NO_MATCH_FOUND);
                    return;
                }

                // Check if the client is trying to update read-only fields
                String error = null;
                for (String aFieldName : nbiRequest.body.fieldNames()) {
                    // Process a field in the new record
                    if (oldRecord.containsKey(aFieldName)) {
                        // Same field shows up in both existing record and the new record
                        if (!getEditableFields().contains(aFieldName)) {
                             // Read-only field, make sure no change is being made
                            Object newValue = nbiRequest.body.getValue(aFieldName);
                            Object oldValue = oldRecord.getValue(aFieldName);
                            if (!newValue.equals(oldValue)) {
                                error = "Field \"" + aFieldName + "\" is read-only thus cannot be edited.";
                                break;
                            }
                        } else {
                            // this field does not exist before
                            if (!getEditableFields().contains(aFieldName)) {
                                error = "Inserting a read-only field \"" + aFieldName + "\"";
                                break;
                            }
                        }
                    }
                }

                // Check the comparison result
                if (error != null) {
                    log.error(error
                            + "\nOld Record:\n" + oldRecord.encodePrettily()
                            + "\nNew Record:\n" + nbiRequest.body.encodePrettily() + "\n");
                    nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST,
                            new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, error));
                } else {
                    // Add the missing read-only fields to the new record
                    for (String aFieldName : oldRecord.fieldNames()) {
                        // Process a field in the old record
                        if (!getEditableFields().contains(aFieldName)) {
                            if (!nbiRequest.body.containsKey(aFieldName)) {
                                // this read-only field is not in the new record
                                VertxJsonUtils.append(nbiRequest.body, aFieldName, oldRecord.getValue(aFieldName));
                            }
                        }
                    }

                    /**
                     * Is Cross Reference Check needed?
                     */
                    if (doCrossReferenceCheckOnUpdate()) {
                        // Build a list of checks as well as a handler, and kicks off the first check
                        CrossReferenceCheckResultHandler handler = new CrossReferenceCheckResultHandler(
                                nbiRequest,
                                AcsApiCrudTypeEnum.Update,
                                oldRecord,
                                getAllCrossReferenceChecks(nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID))
                        );
                        if (handler.allChecks.size() > 0) {
                            handler.allChecks.get(0).runCheck(handler);
                        }
                    } else {
                        // Run additional validation if any
                        if (additionalFindBeforeUpdateQueryResultValidation(nbiRequest, nbiRequest.body, oldRecord)
                                == VALIDATION_SUCCEEDED) {
                            // Now everything looks good, let us save the update
                            saveUpdate(nbiRequest);
                        }
                    }
                }
            } else if (queryResults.size() == 0) {
                nbiRequest.sendResponse(HttpResponseStatus.NOT_FOUND, NO_MATCH_FOUND);
            } else {
                log.error("One or more index fields contain values currently used by another "
                        + getServiceName() + " document/record.");
                JsonObject otherRecord = null;
                for (int i = 0; i < queryResults.size(); i ++) {
                    JsonObject aRecord = queryResults.get(i);
                    if (!id.equals(aRecord.getString(AcsConstants.FIELD_NAME_ID))) {
                        otherRecord = aRecord;
                        break;
                    }
                }
                nbiRequest.sendResponse(
                        HttpResponseStatus.CONFLICT,
                        getConflictDetails(nbiRequest, AcsApiCrudTypeEnum.Update, otherRecord)
                );
            }
        }
    }

    /**
     * When returning an HTTP "409 Conflict" error, call this method to get more error details.
     *
     * @param nbiRequest
     * @param crudType
     * @param otherRecord
     */
    public JsonObject getConflictDetails(
            AcsNbiRequest nbiRequest,
            AcsApiCrudTypeEnum crudType,
            JsonObject otherRecord) {
        JsonObject indexMatcher = null;
        try {
            indexMatcher = buildIndexMatcher(nbiRequest, crudType);
        } catch (VertxException e) {
            e.printStackTrace();
        }
        JsonObject thisRecord = nbiRequest.body;
        String conflictField = null;
        Object conflictValue = null;

        // Compare this and other record
        if (indexMatcher.containsKey(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR)) {
            JsonArray allFields = indexMatcher.getJsonArray(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR);

            String elemMatchField = null;
            for (int i = 0; i < allFields.size(); i ++) {
                JsonObject aField = allFields.getJsonObject(i);
                String fieldName = aField.fieldNames().iterator().next();
                Object value = aField.getValue(fieldName);
                if (value instanceof JsonObject) {
                    // Must be "$elemMatch" for now
                    elemMatchField = fieldName;
                } else {
                    if (!fieldName.equals(AcsConstants.FIELD_NAME_ID) &&
                            !fieldName.equals(AcsConstants.FIELD_NAME_ORG_ID)) {
                        Object thisValue = VertxJsonUtils.deepGet(thisRecord, fieldName);
                        Object otherValue = VertxJsonUtils.deepGet(otherRecord, fieldName);
                        if (thisValue.equals(otherValue)) {
                            // Found a conflict
                            conflictField = fieldName;
                            conflictValue = thisValue;
                            break;
                        }
                    }
                }
            }
            if (conflictField == null) {
                conflictField = elemMatchField;
            }
        } else {
            Set<String> allFields = indexMatcher.fieldNames();
            for (String fieldName : allFields) {
                if (!fieldName.equals(AcsConstants.FIELD_NAME_ID) &&
                        !fieldName.equals(AcsConstants.FIELD_NAME_ORG_ID)) {
                    Object thisValue = VertxJsonUtils.deepGet(thisRecord, fieldName);
                    Object otherValue = VertxJsonUtils.deepGet(otherRecord, fieldName);
                    if (thisValue.equals(otherValue)) {
                        // Found a conflict
                        conflictField = fieldName;
                        conflictValue = thisValue;
                        break;
                    }
                }
            }
        }

        if (conflictField == null) {
            log.error("Unable to find the conflict field! Matcher:\n" + indexMatcher.encodePrettily()
                    + "This Record:\n" + thisRecord.encodePrettily()
                    + "Other Record:\n" + otherRecord.encodePrettily());
            // Return the default error which has no details.
            return CONFLICT;
        } else {
            String error = "Field " + conflictField + " contains value "
                    + (conflictValue == null? "" : "(" + conflictValue.toString() + ") ")
                    + " that conflicts with another "
                    + getServiceName().replace("-", " ");
            return new JsonObject()
                    .put(
                            AcsConstants.FIELD_NAME_ERROR,
                            error
                    );
        }
    }

    /**
     * Additional Validation for updates after the find-before-update query result is in.
     *
     * @param nbiRequest
     * @param newRecord
     * @param oldRecord
     *
     * @return      If validation is completed, returns true.
     *              If validation is not completed (for example pending a further DB query callback), returns false.
     */
    public boolean additionalFindBeforeUpdateQueryResultValidation(
            AcsNbiRequest nbiRequest,
            JsonObject newRecord,
            JsonObject oldRecord) {
        return VALIDATION_SUCCEEDED;
    }

    /**
     * Save the updated document.
     *
     * @param nbiRequest
     */
    public void saveUpdate(AcsNbiRequest nbiRequest) {
        log.info("Overwriting document with\n" + nbiRequest.body.encodePrettily());
        try {
            // Overwrite it
            VertxMongoUtils.save(
                    mongoClient,
                    getDbCollectionName(),
                    nbiRequest.body,
                    getMongoUpdateHandler(nbiRequest)
            );
        } catch (VertxException e) {
            e.printStackTrace();
            nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    getServerInternalErrorWithDetails());
        }
    }

    /**
     * Get CRUD Type by Method String.
     *
     * @param methodString
     * @return
     */
    public AcsApiCrudTypeEnum getCrudTypeByMethodString(String methodString) {
        if (methodString == null) {
            return getDefaultCrudType();
        }

        switch (methodString) {
            case "GET":
                return AcsApiCrudTypeEnum.Retrieve;
            case "POST":
                return AcsApiCrudTypeEnum.Create;
            case "PUT":
                return AcsApiCrudTypeEnum.Update;
            case "DELETE":
                return AcsApiCrudTypeEnum.Delete;
            default:
                log.error("Unknown Method " + methodString + "!");
                return AcsApiCrudTypeEnum.Unknown;
        }
    }

    /**
     * Notify other server instances about this CRUD operation.
     *
     * @param nbiRequest
     * @param crudType
     */
    public void publishCrudEvent(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) {
        String address = getPublishCrudEventsAddress();
        if (address != null) {
            // Build a new Event with CRUD Type
            nbiRequest.body.put(AcsConstants.FIELD_NAME_ACS_CRUD_TYPE, crudType.name());

            // Send it
            vertx.eventBus().publish(address, nbiRequest.body);
        }
    }

    /**
     * Whether Cross Reference Check(s) is needed on Update.
     *
     * Default to false (i.e. no cross-reference checks are needed).
     */
    public boolean doCrossReferenceCheckOnUpdate() {
        return false;
    }

    /**
     * Whether Cross Reference Check(s) is needed on Delete.
     *
     * Default to false (i.e. no cross-reference checks are needed).
     */
    public boolean doCrossReferenceCheckOnDelete() {
        return false;
    }

        /**
     * Return all the cross-reference checks needed when deleting a profile instance
     *
     * Default to return null (i.e. no cross-reference checks are needed).
     *
     * @param id    Internal id for this profile instance
     * @return      A Sorted Set that contains one or more CrossReferenceCheck instance(s), or null.
     */
    public List<CrossReferenceCheck> getAllCrossReferenceChecks(String id) {
        return null;
    }

    /**
     * Handler Class for handling cross reference check query results
     */
    public class CrossReferenceCheckResultHandler implements Handler<Long> {
        public List<CrossReferenceCheck> allChecks;
        public AcsNbiRequest nbiRequest;
        public AcsApiCrudTypeEnum crudType;
        public JsonObject oldRecord;

        /**
         * Constructor
         */
        public CrossReferenceCheckResultHandler(
                AcsNbiRequest nbiRequest,
                AcsApiCrudTypeEnum crudType,
                JsonObject oldRecord,
                List<CrossReferenceCheck> allChecks) {
            this.allChecks = allChecks;
            this.nbiRequest = nbiRequest;
            this.crudType = crudType;
            this.oldRecord = oldRecord;
        }

        /**
         * Handler Body
         * @param count
         */
        @Override
        public void handle(Long count) {
            if (count != null && count > 0) {
                String dbCollectionName = allChecks.get(0).dbCollectionName;
                String error = "This " + getServiceName().replace("-", " ")
                        + " is currently in use by one or more "
                        + AcsApiUtils.getDocumentTypeByCollectionName(dbCollectionName)
                        + "(s) thus cannot be " + crudType.name() + "d.";
                nbiRequest.sendResponse(
                        HttpResponseStatus.BAD_REQUEST,
                        new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, error)
                );
            } else {
                // Remove this cross reference check from the list
                allChecks.remove(0);

                // Do we have more checks in the list?
                if (allChecks.size() > 0) {
                    allChecks.get(0).runCheck(this);
                } else {
                    // Cross-Reference Check is now completed,
                    if (crudType.equals(AcsApiCrudTypeEnum.Delete)) {
                        // Delete the target instance now
                        doDelete(nbiRequest);
                    } else {
                        // Run additional validation if any
                        if (additionalFindBeforeUpdateQueryResultValidation(nbiRequest, nbiRequest.body, oldRecord)
                                == VALIDATION_SUCCEEDED) {
                            // Now everything looks good, let us save the update
                            saveUpdate(nbiRequest);
                        }
                    }
                }
            }
        }
    }

    /**
     * A single cross reference (by "single" we mean the cross reference for a single DB collection)
     */
    public class CrossReferenceCheck {
        public JsonObject matcher;
        public String dbCollectionName;

        // Constructor
        public CrossReferenceCheck(JsonObject matcher, String dbCollectionName) {
            this.matcher = matcher;
            this.dbCollectionName = dbCollectionName;
        }

        // Run the check
        public void runCheck(CrossReferenceCheckResultHandler handler) {
            log.info("Cross Checking " + dbCollectionName + " with matcher\n" + matcher.encodePrettily());
            try {
                VertxMongoUtils.count(
                        mongoClient,
                        dbCollectionName,
                        matcher,
                        handler
                );
            } catch (VertxException e) {
                e.printStackTrace();
                handler.nbiRequest.sendResponse(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, e.getMessage())
                );
            }
        }
    }

    /**
     * Get an Internal Error JSON Object with a timestamp to be displayed on frontend (so the user can contact
     * Calix with that timestamp and then we can easily look up the error in the logs)
     */
    public JsonObject getServerInternalErrorWithDetails() {
        String errorDetails = VertxUtils.getLocalIpAddress() + "~" + VertxUtils.getPid()
                + "~" + new Date().toString() + "~" + System.currentTimeMillis();
        log.error("Returning an internal error to frontend or API client. error details: " + errorDetails);
        return new JsonObject()
                .put(AcsConstants.FIELD_NAME_ERROR,
                        AbstractAcNbiCrudService.INTERNAL_SERVER_ERROR_CONTACT_CALIX
                                + " (error detail: " + errorDetails + ")"
                );
    }
}
