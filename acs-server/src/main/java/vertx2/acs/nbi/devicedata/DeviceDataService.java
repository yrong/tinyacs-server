package vertx2.acs.nbi.devicedata;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.VertxMongoUtils;
import com.calix.sxa.VertxUtils;
import vertx2.acs.nbi.AbstractAcNbiCrudService;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.connreq.ConnectionRequestUtils;
import vertx2.cwmp.CwmpMessage;
import vertx2.model.*;
import vertx2.util.AcsConstants;
import vertx2.util.sxajboss.SxaJBossApiUtils;
import vertx2.util.sxajboss.SxaStagerApiUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Project:  SXA-CC
 *
 * Device Pre-Provisioning Service API.
 *
 * @author: ronyang
 */
public class DeviceDataService extends AbstractAcNbiCrudService{
    /**
     * Query Syntax
     */
    public static final String QUERY_UNLINKED = "unlinked";

    /**
     * Allowed Query Parameter Name/Type Pairs
     */
    private static final HashMap<String, VertxJsonUtils.JsonFieldType> QUERY_PARAMETER_NAME_TYPE_PAIRS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(Cpe.DB_FIELD_NAME_SN, VertxJsonUtils.JsonFieldType.String)
                    .append(QUERY_UNLINKED, VertxJsonUtils.JsonFieldType.Boolean);

    /**
     * Inner class that keeps tracks of all interim data needed when processing an API request
     */
    private class RequestTracker {
        // For query only
        public String orgId = null;
        public boolean bQueryUnlinked = false;
        public boolean bQueryCount = false;
        public boolean bQueryMoreExist = false;
        public Map<Integer, JsonArray> rawDeviceQueryResults;
        public JsonArray unlinkedDevices;
        public boolean bSentResponse = false;
        public int nextBatchIndex = 0;

        // For update only
        public JsonObject oldRecord = null;
    }

    /**
     * Is this service for internal uses only?
     */
    public boolean bInternalServiceOnly() {
        return false;
    }

    /**
     * Query Parameter(s)
     */

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator CREATE_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(Cpe.DB_FIELD_NAME_SN, VertxJsonUtils.JsonFieldType.String)
                    .append(CpeDeviceType.FIELD_NAME_OUI, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator CREATE_OPTIONAL_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING, VertxJsonUtils.JsonFieldType.JsonObject);

    public static final VertxJsonUtils.JsonFieldValidator UPDATE_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator UPDATE_OPTIONAL_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(Cpe.DB_FIELD_NAME_SN, VertxJsonUtils.JsonFieldType.String)
                    .append(CpeDeviceType.FIELD_NAME_OUI, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING, VertxJsonUtils.JsonFieldType.JsonObject);

    public static final VertxJsonUtils.JsonFieldValidator RETRIEVE_MANDATORY_FIELDS_EXTERNAL =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String);
    public static final VertxJsonUtils.JsonFieldValidator RETRIEVE_OPTIONAL_FIELDS_EXTERNAL =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(Cpe.DB_FIELD_NAME_SN, VertxJsonUtils.JsonFieldType.String)
                    .append(CpeDeviceType.FIELD_NAME_MODEL_NAME, VertxJsonUtils.JsonFieldType.String)
                    .append(CpeDeviceType.FIELD_NAME_HW_VER, VertxJsonUtils.JsonFieldType.String)
                    .append(CpeDeviceType.FIELD_NAME_SW_VER, VertxJsonUtils.JsonFieldType.String)
                    .append(Cpe.DB_FIELD_NAME_IP_ADDRESS, VertxJsonUtils.JsonFieldType.String)
                    .append(Cpe.DB_FIELD_NAME_DEFAULT_GATEWAY, VertxJsonUtils.JsonFieldType.String)
                    .append(Cpe.DB_FIELD_NAME_REGISTRATION_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(QUERY_UNLINKED, VertxJsonUtils.JsonFieldType.Boolean)
                    .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator RETRIEVE_MANDATORY_FIELDS_INTERNAL = null;
    public static final VertxJsonUtils.JsonFieldValidator RETRIEVE_OPTIONAL_FIELDS_INTERNAL =
            RETRIEVE_OPTIONAL_FIELDS_EXTERNAL.copy()
                    .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String);

    /**
     * Static Errors
     */
    private static final JsonObject MISSING_ID_ON_DELETES =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, "Device Id is required for deletes");
    private static final JsonObject INVALID_CPE_ID =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, "Invalid Device Id!");
    public static final JsonObject FAILED_TO_UPDATE_SEARCH_ENGINE =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR,
                    "Internal Server Error! (failed to update the search engine)");
    public static final JsonObject FAILED_TO_UPDATE_SUBSCRIBER_DB =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR,
                    "Internal Server Error! (failed to update the subscriber database)");
    public static SxaVertxException MISSING_ORG_ID = new SxaVertxException("Missing Org Id!");

    /**
     * Other Static Constants
     */
    public static final JsonObject NON_EQUAL_TO_TRUE = new JsonObject()
            .putBoolean(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_NOT_EQUAL, true);

    /**
     * Start the service
     */
    @Override
    public void start(Vertx vertx) {
        super.start(vertx);

        /**
         * Initialize the HTTP Client for SXA JBoss API Utils
         */
        SxaJBossApiUtils.initHttpClient(vertx);
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return Cpe.CPE_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     */
    private static final String[] INDEX_FIELDS = {
            AcsConstants.FIELD_NAME_ORG_ID,
            Cpe.DB_FIELD_NAME_SN
    };
    @Override
    public String[] getIndexFieldName() {
        return INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{
        add(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING);
    }};
    @Override
    public List<String> getEditableFields() {
        return EDITABLE_FIELDS;
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
     * @param crudType   Type of the CRUD operation.
     * @return boolean
     * @throws com.calix.sxa.SxaVertxException
     */
    @Override
    public boolean validate(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws SxaVertxException {
        switch (crudType) {
            case Create:
                VertxJsonUtils.validateFields(
                        nbiRequest.body,
                        CREATE_MANDATORY_FIELDS,
                        CREATE_OPTIONAL_FIELDS
                );

                // add "_id"
                nbiRequest.body.putString(
                        AcsConstants.FIELD_NAME_ID,
                        Cpe.getCpeKey(
                                nbiRequest.body.getString(AcsConstants.FIELD_NAME_ORG_ID),
                                nbiRequest.body.getString(CpeDeviceType.FIELD_NAME_OUI),
                                nbiRequest.body.getString(Cpe.DB_FIELD_NAME_SN)
                        )
                );
                break;

            case Update:
                VertxJsonUtils.validateFields(
                        nbiRequest.body,
                        UPDATE_MANDATORY_FIELDS,
                        UPDATE_OPTIONAL_FIELDS
                );
                break;

            case Retrieve:
                if (nbiRequest.bInternalRequest) {
                    VertxJsonUtils.validateFields(
                            nbiRequest.body,
                            RETRIEVE_MANDATORY_FIELDS_INTERNAL,
                            RETRIEVE_OPTIONAL_FIELDS_INTERNAL
                    );
                } else {
                    VertxJsonUtils.validateFields(
                            nbiRequest.body,
                            RETRIEVE_MANDATORY_FIELDS_EXTERNAL,
                            RETRIEVE_OPTIONAL_FIELDS_EXTERNAL
                    );
                }
                if (nbiRequest.body.containsField(QUERY_UNLINKED)) {
                    /**
                     * Querying unlinked devices (i.e. not associated with any subscriber)
                     */
                    boolean bQueryUnlinked = nbiRequest.body.getBoolean(QUERY_UNLINKED);
                    if (bQueryUnlinked) {
                        RequestTracker reqTracker = new RequestTracker();
                        reqTracker.bQueryUnlinked = true;
                        reqTracker.orgId = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ORG_ID);
                        if (reqTracker.orgId == null) {
                            throw MISSING_ORG_ID;
                        }
                        nbiRequest.serviceData = reqTracker;
                    }
                    nbiRequest.body.removeField(QUERY_UNLINKED);
                }
                break;
        }
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
     * After validation, perform any service specific actions against this request.
     *
     * Default to no action.
     *
     * @param nbiRequest
     * @param crudType      Type of the CRUD operation.
     *
     * @return None
     * @throws com.calix.sxa.SxaVertxException
     */
    public void preProcess(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws SxaVertxException {
        switch (crudType) {
            case Create:
                // add create time
                nbiRequest.body.putObject(AcsConstants.FIELD_NAME_CREATE_TIME, VertxMongoUtils.getDateObject());
                break;

            case Retrieve:
                // When retrieving by regId, only return the active one (exclude the decommissioned device)
                if (nbiRequest.body.containsField(Cpe.DB_FIELD_NAME_REGISTRATION_ID)) {
                    nbiRequest.body.putObject(Cpe.DB_FIELD_NAME_DECOMMISSIONED, NON_EQUAL_TO_TRUE);
                }
                break;
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
    private static final String HW_SN_PREFIX = "UnitSerialNumber=";     // (see SXACC-649)
    @Override
    public JsonObject additionalPostRetrievePerRecordHandler(AcsNbiRequest nbiRequest, JsonObject aRecord) {
        if (aRecord.containsField(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_ENABLE)) {
            boolean bPeriodicInformEnabled = aRecord.getBoolean(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_ENABLE);
            if (bPeriodicInformEnabled == false) {
                aRecord.putNumber(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_INTERVAL, 0);
            }
            aRecord.removeField(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_ENABLE);
        }

        // Extract HW Serial # from "AdditionalHardwareVersion" (see SXACC-649)
        if (aRecord.containsField(Cpe.DB_FIELD_NAME_ADDITIONAL_HW_VER)) {
            String additionalHwVersion = aRecord.getString(Cpe.DB_FIELD_NAME_ADDITIONAL_HW_VER);
            aRecord.removeField(Cpe.DB_FIELD_NAME_ADDITIONAL_HW_VER);

            if (additionalHwVersion != null && additionalHwVersion.startsWith(HW_SN_PREFIX)) {
                aRecord.putString("hardwareSerialNumber", additionalHwVersion.substring(HW_SN_PREFIX.length()));
            /*
            } else {
                log.info(aRecord.getString(Cpe.DB_FIELD_NAME_SN) + ": no HW SN found in additionalHwVer ("
                        + additionalHwVersion + ")");
            */
            }
        }

        return aRecord;
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
    @Override
    public boolean additionalFindBeforeUpdateQueryResultValidation(
            AcsNbiRequest nbiRequest,
            JsonObject newRecord,
            JsonObject oldRecord) {
        // Update initial provisioning
        if (newRecord.containsField(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING)) {
            // send a conn-req to the CPE to apply immediately if the CPE is already known
            if (oldRecord.containsField(Cpe.DB_FIELD_NAME_CONNREQ_URL)) {
                RequestTracker requestTracker = new RequestTracker();
                requestTracker.oldRecord = oldRecord;
                nbiRequest.serviceData = requestTracker;

                // Set the force flag
                newRecord.getObject(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING)
                        .putBoolean(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING_FORCE_APPLY, true);
            }
        }

        return VALIDATION_SUCCEEDED;
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
    @Override
    public boolean postUpdate(
            AcsNbiRequest nbiRequest,
            HttpResponseStatus responseStatus,
            String error) {
        if (HttpResponseStatus.OK.equals(responseStatus)) {
            // send a conn-req to the CPE to apply immediately if the CPE is already known
            RequestTracker reqTracker = nbiRequest.getServiceData();
            JsonObject queryResult = reqTracker.oldRecord;
            if (queryResult != null && queryResult.containsField(Cpe.DB_FIELD_NAME_CONNREQ_URL)) {
                ConnectionRequestUtils.sendNewConnectionRequestRequest(vertx, queryResult);
            }
        }

        return super.postUpdate(nbiRequest, responseStatus, error);
    }

    /**
     * Build MongoDB Query Keys for Retrieve.
     *
     * Default to null (return everything)
     */
    private static final JsonObject QUERY_KEYS = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ORG_ID, 0)
            .putNumber(Cpe.DB_FIELD_NAME_LAST_UPDATE_TIME, 0)
            .putNumber(Cpe.DB_FIELD_NAME_WAN_CONNECTION_PATH, 0)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_PASSWORD, 0)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_URL, 0)
            .putNumber(Cpe.DB_FIELD_NAME_WORKFLOW_EXEC, 0)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_USERNAME, 0)
            .putNumber(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING + "." + Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING_FORCE_APPLY, 0)
            .putNumber(Cpe.DB_FIELD_NAME_PARAM_VALUES, 0)
            .putNumber(Cpe.DB_FIELD_NAME_PARAM_ATTRIBUTES, 0);
    private static final JsonObject QUERY_KEY_BRIEF = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ORG_ID, 1)
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1)
            .putNumber(Cpe.DB_FIELD_NAME_REGISTRATION_ID, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_MANUFACTURER, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_MODEL_NAME, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_SW_VER, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_HW_VER, 1)
            .putNumber(Cpe.DB_FIELD_NAME_ADDITIONAL_HW_VER, 1)
            .putNumber(Cpe.DB_FIELD_NAME_LAST_INFORM_TIME, 1)
            .putNumber(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_INTERVAL, 1)
            .putNumber(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_ENABLE, 1);
    private static final JsonObject QUERY_KEY_UNLINKED = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ID, 1)
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1)
            .putNumber(Cpe.DB_FIELD_NAME_IP_ADDRESS, 1)
            .putNumber(Cpe.DB_FIELD_NAME_LAST_INFORM_TIME, 1)
            .putNumber(Cpe.DB_FIELD_NAME_PREV_SUBSCRIBER, 1)
            .putNumber(Cpe.DB_FIELD_NAME_REGISTRATION_ID, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_MODEL_NAME, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_SW_VER, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_HW_VER, 1)
            .putNumber(Cpe.DB_FIELD_NAME_ADDITIONAL_HW_VER, 1)
            .putNumber(Cpe.DB_FIELD_NAME_DECOMMISSIONED, 1)
            .putNumber(Cpe.DB_FIELD_NAME_PREV_SUBSCRIBER, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CREATE_TIME, 1);
    private static final JsonObject QUERY_KEY_UNLINKED_COUNT = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ID, 1)
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1)
            .putNumber(Cpe.DB_FIELD_NAME_REGISTRATION_ID, 1)
            .putNumber(Cpe.DB_FIELD_NAME_DECOMMISSIONED, 1)
            .putNumber(Cpe.DB_FIELD_NAME_PREV_SUBSCRIBER, 1);

    /**
     * Query Key when querying Subscribers for unlinked devices
     */
    private static final JsonObject QUERY_KEY_UNLINKED_SUBSCRIBERS = new JsonObject()
            .putNumber(Subscriber.FIELD_NAME_LOCATIONS + "." + Subscriber.FIELD_NAME_LOCATIONS_DEVICES, 1);

    @Override
    public JsonObject buildRetrieveQueryKeys(AcsNbiRequest nbiRequest) {
        if (nbiRequest.getQueryBrief()) {
            return QUERY_KEY_BRIEF;
        }
        return QUERY_KEYS;
    }

    /**
     * Should the retrieve results be sent to clients in chunk mode?
     */
    @Override
    public boolean bReturnRetrieveResultInChunkMode(AcsNbiRequest nbiRequest) {
        if (nbiRequest.body.containsField(AcsConstants.FIELD_NAME_ID) ||
                nbiRequest.body.containsField(Cpe.DB_FIELD_NAME_SN) ||
                nbiRequest.body.containsField(Cpe.DB_FIELD_NAME_REGISTRATION_ID) ||
                nbiRequest.body.containsField(Cpe.DB_FIELD_NAME_MAC_ADDRESS) ||
                nbiRequest.body.containsField(Cpe.DB_FIELD_NAME_IP_ADDRESS)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Always query a single record
     */
    public int getQueryLimitCount(AcsNbiRequest nbiRequest) {
        if (isQueryUnlinked(nbiRequest)) {
            int limit = nbiRequest.getQueryLimitCount();
            if (limit <= 0) {
                // no limit specified. Default to return everything
                limit = Integer.MAX_VALUE;
            }
            return limit;
        }

        if (nbiRequest.bInternalRequest)
            return super.getQueryLimitCount(nbiRequest);
        return 1;
    }

    /**
     * Is querying unlinked devices?
     * @param nbiRequest
     * @return
     */
    public boolean isQueryUnlinked(AcsNbiRequest nbiRequest) {
        RequestTracker reqTracker = nbiRequest.getServiceData();

        if (reqTracker != null && reqTracker.bQueryUnlinked) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Retrieve Request Handler.
     *
     * @param nbiRequest
     */
    @Override
    public void handleRetrieve(final AcsNbiRequest nbiRequest) {
        if (isQueryUnlinked(nbiRequest)) {
            RequestTracker reqTracker = nbiRequest.getServiceData();
            if (nbiRequest.urlPathParams != null && nbiRequest.urlPathParams[0].equals(QUERY_KEYWORD_COUNT)) {
                reqTracker.bQueryCount = true;
            } else {
                if (nbiRequest.httpServerRequest != null) {
                    nbiRequest.httpServerRequest.response().putHeader("Content-Type", "application/json");
                    VertxUtils.setResponseStatus(nbiRequest.httpServerRequest, HttpResponseStatus.OK);
                    nbiRequest.httpServerRequest.response().setChunked(true);
                    nbiRequest.httpServerRequest.response().write("[");
                }
            }

            /**
             * Have to retrieve all devices regardless
             */
            try {
                VertxMongoUtils.find(
                        vertx.eventBus(),
                        getDbCollectionName(),
                        buildRetrieveMatcher(nbiRequest),
                        getDefaultQuerySort(nbiRequest),
                        0,
                        -1,
                        new RetrieveUnlinkedResultHandler(nbiRequest),
                        reqTracker.bQueryCount? QUERY_KEY_UNLINKED_COUNT : QUERY_KEY_UNLINKED,
                        null
                );
            } catch (SxaVertxException e) {
                e.printStackTrace();
                nbiRequest.sendResponseChunk(HttpResponseStatus.OK, null, false);
            }
        } else {
            /**
             * Not querying unlinked devices. Just call super method
             */
            super.handleRetrieve(nbiRequest);
        }
    }

    /**
     * Retrieve Unlinked Devices Result Handler
     */
    public class RetrieveUnlinkedResultHandler extends VertxMongoUtils.FindHandler{
        AcsNbiRequest nbiRequest;
        RequestTracker reqTracker;

        /**
         * Constructor
         */
        public RetrieveUnlinkedResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
            reqTracker = nbiRequest.getServiceData();
        }

        /**
         * The handler method body.
         * @param jsonObjectMessage
         */
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            if (reqTracker.bSentResponse) {
                return;
            }

            // Call super
            super.handle(jsonObjectMessage);

            if (VertxMongoUtils.FIND_TIMED_OUT.equals(queryResults)) {
                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, MONGODB_TIMED_OUT);
                return;
            }

            if (reqTracker.unlinkedDevices != null &&
                    reqTracker.unlinkedDevices.size() >= (getQuerySkipCount(nbiRequest) + getQueryLimitCount(nbiRequest))) {
                /**
                 * Done
                 */
                finishUnlinkedDeviceQuery(nbiRequest, reqTracker);
                return;
            }

            reqTracker.bQueryMoreExist = moreExist;

            /**
             * Process a result batch
             */
            // Build a string array that holds all possible device id strings for this batch
            JsonArray deviceIdStrings = new JsonArray();
            JsonArray devices = new JsonArray();
            if (queryResults != null && queryResults.size() > 0) {
                for (int i = 0; i < queryResults.size(); i++) {
                    JsonObject aDevice = queryResults.get(i);

                    // Decommissioned Devices are always unlinked
                    if (aDevice.getBoolean(Cpe.DB_FIELD_NAME_DECOMMISSIONED, false) ||
                            // Device is unlinked if its "prevSubscriber" field is set
                            aDevice.containsField(Cpe.DB_FIELD_NAME_PREV_SUBSCRIBER)) {
                        if (reqTracker.unlinkedDevices == null) {
                            reqTracker.unlinkedDevices = new JsonArray();
                        }
                        reqTracker.unlinkedDevices.add(aDevice);
                    } else {
                        // Add internal-id/FSAN/RegId to device-id string array
                        deviceIdStrings.add(aDevice.getString(AcsConstants.FIELD_NAME_ID));
                        deviceIdStrings.add(aDevice.getString(Cpe.DB_FIELD_NAME_SN));
                        if (aDevice.containsField(Cpe.DB_FIELD_NAME_REGISTRATION_ID)) {
                            deviceIdStrings.add(aDevice.getString(Cpe.DB_FIELD_NAME_REGISTRATION_ID));
                        }
                        devices.add(aDevice);
                    }
                }
            }

            if (deviceIdStrings.size() > 0) {
                // The query results of each batch is stored in the HashMap indexed by batch index
                if (reqTracker.rawDeviceQueryResults == null) {
                    reqTracker.rawDeviceQueryResults = new HashMap<>();
                }
                int batchIndex = reqTracker.nextBatchIndex;
                reqTracker.nextBatchIndex++;
                reqTracker.rawDeviceQueryResults.put(batchIndex, devices);

                // Query Subscribers with deviceId strings
                try {
                    VertxMongoUtils.find(
                            vertx.eventBus(),
                            Subscriber.DB_COLLECTION_NAME,
                            Subscriber.getDeviceMatcherByDeviceIdArray(reqTracker.orgId, deviceIdStrings),
                            new RetrieveSubscriberResultHandlerForUnlinked(nbiRequest, batchIndex),
                            QUERY_KEY_UNLINKED_SUBSCRIBERS,
                            null
                    );
                } catch (SxaVertxException e) {
                    e.printStackTrace();
                }
            } else {
                /**
                 * Last Batch???
                 */
                if (!moreExist &&
                        (reqTracker.rawDeviceQueryResults == null
                                || reqTracker.rawDeviceQueryResults.size() == 0)) {
                    /**
                     * Done
                     */
                    finishUnlinkedDeviceQuery(nbiRequest, reqTracker);
                }
            }
        }
    }

    /**
     * Subscriber Retrieve Result Handler used when retrieving Unlinked Devices.
     */
    public class RetrieveSubscriberResultHandlerForUnlinked extends VertxMongoUtils.FindHandler {
        AcsNbiRequest nbiRequest;
        RequestTracker reqTracker;
        int batchIndex;

        /**
         * Constructor
         */
        public RetrieveSubscriberResultHandlerForUnlinked(
                AcsNbiRequest nbiRequest,
                int batchIndex) {
            this.nbiRequest = nbiRequest;
            reqTracker = nbiRequest.getServiceData();
            this.batchIndex = batchIndex;
        }

        /**
         * The handler method body.
         *
         * @param jsonObjectMessage
         */
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            if (reqTracker.bSentResponse) {
                return;
            }
            // Call super
            super.handle(jsonObjectMessage);

            if (VertxMongoUtils.FIND_TIMED_OUT.equals(queryResults)) {
                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, MONGODB_TIMED_OUT);
                return;
            }

            log.debug("Processing subscriber query. batchIndex: " + batchIndex);
            /**
             * Process a result batch
             */
            List<String> allAssociatedDeviceIdStrings = null;
            if (queryResults != null && queryResults.size() > 0) {
                allAssociatedDeviceIdStrings = new ArrayList<>(queryResults.size());

                for (int i = 0; i < queryResults.size(); i++) {
                    JsonObject aSubscriber = queryResults.get(i);
                    JsonArray locations = aSubscriber.getArray(Subscriber.FIELD_NAME_LOCATIONS);
                    if (locations != null) {
                        // Validate all locations (and the associated devices per location)
                        for (int j = 0; j < locations.size(); j++) {
                            JsonObject aLocation = locations.get(j);
                            // Add the associated devices to Array
                            JsonArray deviceIdArray = aLocation.getArray(Subscriber.FIELD_NAME_LOCATIONS_DEVICES);
                            if (deviceIdArray != null) {
                                for (int k = 0; k < deviceIdArray.size(); k++) {
                                    allAssociatedDeviceIdStrings.add((String) deviceIdArray.get(k));
                                }
                            }
                        }
                    }
                }
            }

            // Check all devices in the raw batch
            JsonArray rawDeviceBatch = reqTracker.rawDeviceQueryResults.get(batchIndex);
            for (int i = 0; i < rawDeviceBatch.size(); i ++) {
                JsonObject aDevice = rawDeviceBatch.get(i);

                if (allAssociatedDeviceIdStrings != null && (
                        allAssociatedDeviceIdStrings.contains(aDevice.getString(AcsConstants.FIELD_NAME_ID)) ||
                        allAssociatedDeviceIdStrings.contains(aDevice.getString(Cpe.DB_FIELD_NAME_SN)) ||
                        allAssociatedDeviceIdStrings.contains(
                                aDevice.getString(Cpe.DB_FIELD_NAME_REGISTRATION_ID, "(not reg id)")))) {
                    // This device is linked
                } else {
                    // Found an unlinked device
                    if (reqTracker.unlinkedDevices == null) {
                        reqTracker.unlinkedDevices = new JsonArray();
                    }
                    reqTracker.unlinkedDevices.add(aDevice);
                    if (hadEnoughUnlinkedDevices(nbiRequest, reqTracker)) {
                        finishUnlinkedDeviceQuery(nbiRequest, reqTracker);
                        return;
                    }
                }
            }

            // Remove the raw device query result batch
            reqTracker.rawDeviceQueryResults.remove(batchIndex);

            log.debug("size of reqTracker.rawDeviceQueryResults: " + reqTracker.rawDeviceQueryResults.size());
            log.debug("size of reqTracker.unlinkedDevices: " +
                    (reqTracker.unlinkedDevices == null? 0 : reqTracker.unlinkedDevices.size()));

            if (hadEnoughUnlinkedDevices(nbiRequest, reqTracker)) {
                finishUnlinkedDeviceQuery(nbiRequest, reqTracker);
            }
        }
    }

    /**
     * Check if we had processed enough unlinked devices.
     * @param nbiRequest
     * @param reqTracker
     * @return
     */
    public boolean hadEnoughUnlinkedDevices(AcsNbiRequest nbiRequest, RequestTracker reqTracker) {
        if (reqTracker.bQueryMoreExist == false && reqTracker.rawDeviceQueryResults.size() == 0) {
            return true;
        }
        if (reqTracker.bQueryCount) {
            // We will have to query all devices.
            return false;
        } else {
            int skip = getQuerySkipCount(nbiRequest);
            int limit = getQueryLimitCount(nbiRequest);
            int nbrOfDevices = reqTracker.unlinkedDevices.size();
            if (limit > 0) {
                if(skip > 0) {
                    if (nbrOfDevices >= (skip + limit)) {
                        return true;
                    }
                } else {
                    if (nbrOfDevices >= limit) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    /**
     * Finish up handling querying the unlinked devices.
     *
     * @param nbiRequest
     * @param reqTracker
     */
    public void finishUnlinkedDeviceQuery(AcsNbiRequest nbiRequest, RequestTracker reqTracker) {
        if (reqTracker.bSentResponse == true) {
            return;
        }

        /**
         * Done
         */
        reqTracker.bSentResponse = true;
        if (reqTracker.bQueryCount) {
            // Getting count only
            nbiRequest.httpServerRequest.response().putHeader("Content-Type", "application/json");
            nbiRequest.sendResponse(
                    HttpResponseStatus.OK,
                    new JsonObject().putNumber(
                            QUERY_KEYWORD_COUNT,
                            reqTracker.unlinkedDevices == null ? 0 : reqTracker.unlinkedDevices.size()
                    )
            );
        } else {
            // Querying the actual device data
            log.debug("Found "
                    + (reqTracker.unlinkedDevices == null ? 0 : reqTracker.unlinkedDevices.size())
                    + " unlinked devices so far, "
                    + "skip: " + getQuerySkipCount(nbiRequest)
                    + ", limit: " + getQueryLimitCount(nbiRequest));
            nbiRequest.sendResponseChunk(
                    HttpResponseStatus.OK,
                    reqTracker.unlinkedDevices,
                    getQuerySkipCount(nbiRequest),
                    getQueryLimitCount(nbiRequest),
                    false
            );
        }
    }

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     * <p/>
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_DEVICE_DATA;
    }

    /**
     * Get the list of field names that contain MongoDB "$date" timestamp.
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    private static final String[] TIMESTAMP_FIELDS = {
            Cpe.DB_FIELD_NAME_LAST_INFORM_TIME,
            Cpe.DB_FIELD_NAME_CREATE_TIME
    };
    @Override
    public String[] getDateTimeFieldName() {
        return TIMESTAMP_FIELDS;
    }

    /**
     * Default Delete Request Handler.
     *
     * @param nbiRequest
     */
    private static final JsonObject QUERY_KEY_DELETE = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ID, 1)
            .putNumber(AcsConstants.FIELD_NAME_ORG_ID, 1)
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_MANUFACTURER, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_MODEL_NAME, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_SW_VER, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_HW_VER, 1);
    @Override
    public void handleDelete(final AcsNbiRequest nbiRequest) {
        final String orgId = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ORG_ID);
        final String cpeKey = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);

        /**
         * Validations
         */
        if (cpeKey == null) {
            /**
             * If the CPE Key (i.e. "_id") is missing, SN is required
             */
            String sn = nbiRequest.body.getString(Cpe.DB_FIELD_NAME_SN);
            if (sn == null) {
                nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, MISSING_ID_ON_DELETES);
                return;
            }
        }

        /**
         * Query the CPE Record and Subscriber Record
         */
        try {
            // Subscriber Query Handler
            final Handler<JsonObject> subscriberQueryResultHandler = new Handler<JsonObject>() {
                @Override
                public void handle(final JsonObject subscriberData) {
                    final JsonObject cpe = nbiRequest.getServiceData();

                    if (subscriberData != null) {
                        /**
                         * Return Error if device is associated with any subscriber
                         */
                        nbiRequest.sendResponse(
                                HttpResponseStatus.BAD_REQUEST,
                                new JsonObject().
                                        putString(
                                                AcsConstants.FIELD_NAME_ERROR,
                                                "Device " + cpe.getString(Cpe.DB_FIELD_NAME_SN)
                                                        + " is associated with subscriber "
                                                        + subscriberData.getString(AcsConstants.FIELD_NAME_NAME)
                                                        + ", please disassociate the device from the subscriber "
                                                        + "before delete."
                                        )
                        );
                        return;
                    }

                    /**
                     * Call JBoss API to Delete the device from Elastic Search
                     */
                    SxaStagerApiUtils.deleteDevice(
                            cpe,
                            null,
                            new Handler<JsonObject>() {
                                @Override
                                public void handle(JsonObject jbossDeleteResult) {
                                    String error = jbossDeleteResult.getString(AcsConstants.FIELD_NAME_ERROR);
                                    if (error != null) {
                                        log.error("Failed to delete device " + cpeKey + " due to " + error);
                                        nbiRequest.sendResponse(
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, error)
                                        );
                                        return;
                                    }

                                    // Clean up related collections
                                    cleanupRelatedCollections(cpe);

                                    // Done
                                    doDelete(nbiRequest);
                                }
                            }
                    );
                }
            };

            // Device Query Handler
            Handler<JsonObject> deviceQueryResultHandler = new Handler<JsonObject>() {
                @Override
                public void handle(final JsonObject cpe) {
                    if (cpe == null) {
                        nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, INVALID_CPE_ID);
                    } else if (cpe.equals(VertxMongoUtils.FIND_ONE_TIMED_OUT)) {
                        nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, MONGODB_TIMED_OUT);
                    } else {
                        // Found it
                        String id = cpeKey;
                        if (cpeKey == null) {
                            // Save the CPE id string into request body
                            id = cpe.getString(AcsConstants.FIELD_NAME_ID);
                            nbiRequest.body.putString(AcsConstants.FIELD_NAME_ID, id);
                        }

                        // Save the device data
                        nbiRequest.serviceData = cpe;

                        // Query Subscriber
                        Subscriber.querySubscriberData(
                                vertx.eventBus(),
                                cpe,
                                subscriberQueryResultHandler
                        );
                    }
                }
            };

            // Start with querying device
            VertxMongoUtils.findOne(
                    vertx.eventBus(),
                    Cpe.CPE_COLLECTION_NAME,
                    nbiRequest.body,
                    new VertxMongoUtils.FindOneHandler(deviceQueryResultHandler),
                    QUERY_KEY_DELETE
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }

    }

    /**
     * When deleting a CPE device, clean up related collections:
     *
     * - cwmp-messages
     * - device-types
     * - events
     * - backup files
     *
     * @param cpe
     */
    public void cleanupRelatedCollections(JsonObject cpe) {
        try {
            /**
             * 1. Delete all CWMP Messages for this device
             */
            VertxMongoUtils.deleteWithMatcher(
                    vertx.eventBus(),
                    CwmpMessage.DB_COLLECTION_NAME,
                    new JsonObject().putString(
                            AcsConstants.FIELD_NAME_CPE_ID + "." + CpeIdentifier.FIELD_NAME_SN,
                            cpe.getString(Cpe.DB_FIELD_NAME_SN)),
                    null
            );

            /**
             * 2. Delete all Events for this device
             */
            VertxMongoUtils.deleteWithMatcher(
                    vertx.eventBus(),
                    Event.DB_COLLECTION_NAME,
                    new JsonObject().putString(
                            Event.FIELD_NAME_DEVICE_SN,
                            cpe.getString(Cpe.DB_FIELD_NAME_SN)),
                    null
            );

            /**
             * 3. Delete all backup files for this device
             */
            VertxMongoUtils.deleteWithMatcher(
                    vertx.eventBus(),
                    AcsFile.DB_COLLECTION_NAME,
                    new JsonObject().putString(
                            AcsConstants.FIELD_NAME_CPE_ID + "." + Cpe.DB_FIELD_NAME_SN,
                            cpe.getString(Cpe.DB_FIELD_NAME_SN)
                    ),
                    null
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Inner Handler Class for handling Jboss CPE-Delete Result
     */
    private class JbossCpeDeleteResultHandler implements Handler<String> {
        AcsNbiRequest nbiRequest;
        String orgId;
        String cpeKey;

        // Constructor
        public JbossCpeDeleteResultHandler(AcsNbiRequest nbiRequest, String orgId, String cpeKey) {
            this.nbiRequest = nbiRequest;
            this.orgId = orgId;
            this.cpeKey = cpeKey;
        }

        /**
         * Handler Body
         * @param result
         */
        @Override
        public void handle(String result) {
            if (result == null || VertxUtils.getLocalHostname().startsWith("vmlnx-ronyang")) {
                // Deleted CPE from Elastic Search
                log.info("Successfully deleted CPE " + cpeKey + " from Elastic Search via JBoss/plugin-cc API.");

                /**
                 * Step 3: If the device is already associated with a Subscriber, delete this device from
                 * the device list of that subscriber and Call the Elastic Search API to update this
                 * subscriber
                 */
                // Query Subscriber that this device may be associated with
                try {
                    VertxMongoUtils.findOne(
                            vertx.eventBus(),
                            Subscriber.DB_COLLECTION_NAME,
                            //Subscriber.getDeviceMatcherByDeviceData()
                            // Matcher
                            new JsonObject().putObject(
                                    Subscriber.FIELD_NAME_LOCATIONS,
                                    new JsonObject().putObject(
                                            VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_ELEM_MATCH,
                                            new JsonObject().putObject(
                                                    Subscriber.FIELD_NAME_LOCATIONS_DEVICES,
                                                    new JsonObject().putArray(
                                                            VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_ALL,
                                                            new JsonArray().add(cpeKey)
                                                    )
                                            )
                                    )
                            ),
                            getSubscriberFindOneHandler(nbiRequest, cpeKey),
                            null
                    );
                } catch (SxaVertxException e) {
                    e.printStackTrace();
                }
            } else {
                log.error("Failed to delete CPE " + cpeKey + " from Elastic Search via JBoss/plugin-cc API!"
                        + " error/exception: " + result);
                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, FAILED_TO_UPDATE_SEARCH_ENGINE);
            }
        }
    }

    /**
     * Get a Handler instance for the Subscriber Query Result.
     *
     * @param nbiRequest
     * @param cpeKey
     * @return
     */
    private VertxMongoUtils.FindOneHandler getSubscriberFindOneHandler(
            final AcsNbiRequest nbiRequest,
            final String cpeKey) {
        try {
            return new VertxMongoUtils.FindOneHandler(
                    new Handler<JsonObject>() {
                        @Override
                        public void handle(JsonObject subscriber) {
                            if (subscriber == null) {
                                // This CPE is not associated with any subscriber
                                // Continue on
                                doDelete(nbiRequest);
                            } else if (subscriber.equals(VertxMongoUtils.FIND_ONE_TIMED_OUT)) {
                                log.error("Failed to delete device " + cpeKey
                                        + " from Subscriber DB due to Internal MongoDB error!");
                                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                        FAILED_TO_UPDATE_SUBSCRIBER_DB);
                            } else {
                                // Remove this CPE from this Subscriber record
                                log.info("Deleting CPE " + cpeKey + " from Subscriber "
                                        + subscriber.getString(AcsConstants.FIELD_NAME_NAME));
                                JsonArray locations = subscriber.getArray(Subscriber.FIELD_NAME_LOCATIONS);
                                for (int i = 0; i < locations.size(); i ++) {
                                    JsonObject aLocation = locations.get(i);
                                    JsonArray devices = aLocation.getArray(Subscriber.FIELD_NAME_LOCATIONS_DEVICES);
                                    if (devices.contains(cpeKey)) {
                                        JsonArray updatedDevices = new JsonArray();
                                        for (int j = 0; j < locations.size(); j++) {
                                            String deviceId = devices.get(j);
                                            if (!deviceId.equals(cpeKey)) {
                                                updatedDevices.add(deviceId);
                                            }
                                        }
                                        aLocation.putArray(Subscriber.FIELD_NAME_LOCATIONS_DEVICES, updatedDevices);
                                    }
                                }
                                try {
                                    VertxMongoUtils.update(
                                            vertx.eventBus(),
                                            Subscriber.DB_COLLECTION_NAME,
                                            subscriber.getString(AcsConstants.FIELD_NAME_ID),
                                            subscriber,
                                            new Handler<Long>() {
                                                @Override
                                                public void handle(Long updateResult) {
                                                    if (updateResult == null) {
                                                        log.error("Failed to delete device " + cpeKey
                                                                + " from Subscriber DB due to Internal MongoDB error!");
                                                        nbiRequest.sendResponse(
                                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                FAILED_TO_UPDATE_SUBSCRIBER_DB);
                                                    } else {
                                                        // Continue on
                                                        doDelete(nbiRequest);
                                                    }
                                                }
                                            }
                                    );
                                } catch (SxaVertxException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
            );
        } catch (SxaVertxException e) {
            return null;
        }
    }
}
