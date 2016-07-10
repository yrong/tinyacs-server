package vertx2.acs.nbi.subscriber;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.VertxMongoUtils;
import vertx2.acs.nbi.AbstractAcNbiCrudService;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.acs.nbi.serviceplan.ServicePlanService;
import vertx2.acs.utils.ReplacementUtils;
import vertx2.model.*;
import vertx2.util.AcsConstants;
import vertx2.util.sxajboss.SxaStagerApiUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;

/**
 * Project:  SXA-CC
 *
 * SXA-CC Subscriber Service.
 *
 * @author: ronyang
 */
public class SubscriberService extends AbstractAcNbiCrudService {
    private final SxaVertxException NO_QUERY_FILTER_FOUND = new SxaVertxException("No Query Filter Found!");

    /**
     * Error Constants
     */
    public static final String ERROR_STRING_UNKNOWN_DEVICE = "Not All Associated Devices are found in the DB!";
    public static final JsonObject UNKNOWN_DEVICE = new JsonObject()
            .putString(AcsConstants.FIELD_NAME_ERROR, ERROR_STRING_UNKNOWN_DEVICE);
    public static final JsonObject ACCT_NUMBER_IN_USE = new JsonObject()
            .putString(AcsConstants.FIELD_NAME_ERROR, "The account number conflicts with another subscriber!");
    public static final String ERROR_STRING_MULTI_DEVICE_ALREADY_ASSOCIATED =
            "One or more device(s) have already been associated with other Subscriber(s)!";
    public static final JsonObject MULTI_DEVICE_ALREADY_ASSOCIATED  = new JsonObject()
            .putString(AcsConstants.FIELD_NAME_ERROR, ERROR_STRING_MULTI_DEVICE_ALREADY_ASSOCIATED);
    public static final SxaVertxException INVALID_SERVICE_PREFIX =
            new SxaVertxException("The max length of \"servicePrefix\" is 20 chars and can only contain letters"
                    + " and/or digits (0-9) and/or dots (\".\")!");
    public static final SxaVertxException CANNOT_HAVE_MULTIPLE_PRIMARY_LOCATIONS = new SxaVertxException(
            "Cannot have multiple primary locations!"
    );
    public static final SxaVertxException CANNOT_HAVE_MULTIPLE_PRIMARY_CONTACTS = new SxaVertxException(
            "Cannot have multiple primary contacts in the same locations!"
    );
    public static final String FAILED_TO_UPDATE_SEARCH_ENGINE_STRING =
            "Internal Error ! (Failed to update search engine)";
    public static final JsonObject FAILED_TO_UPDATED_SEARCH_ENGINE = new JsonObject()
            .putString(AcsConstants.FIELD_NAME_ERROR, FAILED_TO_UPDATE_SEARCH_ENGINE_STRING);

    /**
     * Mod Mongo Query Keys when finding devices
     */
    public static final JsonObject FIND_DEVICE_QUERY_KEY =
            new JsonObject()
                    .putNumber(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, 1)
                    .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_URL, 1)
                    .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_USERNAME, 1)
                    .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_PASSWORD, 1)
                    .putNumber(Cpe.DB_FIELD_NAME_REGISTRATION_ID, 1)
                    .putNumber(CpeDeviceType.FIELD_NAME_MODEL_NAME, 1)
                    .putNumber(Cpe.DB_FIELD_NAME_SN, 1);

    /**
     * Is this service for internal uses only?
     */
    public boolean bInternalServiceOnly() {
        return false;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return Subscriber.DB_COLLECTION_NAME;
    }

    /**
     * An Organization is uniquely identified by "name".
     */
    @Override
    public String[] getIndexFieldName() {
        return null;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return Subscriber.EDITABLE_FIELDS;
    }

    /**
     * Inner class that keeps tracks of all interim data needed when processing an API request
     */
    private class SubscriberRequestTracker {
        public String servicePrefix = null;
        public String orgId;
        //public JsonArray allRegIDs = new JsonArray();
        //public JsonArray allFSANs = new JsonArray();
        //public JsonArray allInternalIDs = new JsonArray();
        public JsonArray allDeviceIdStrings = new JsonArray();
        public JsonObject matcher = new JsonObject();
        public JsonArray matchingDevices;
        JsonObject oldRecord;   // For update only
        public long numberOfDevicesDisassociated = 0;
        public String subscriberName;
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
     * @throws com.calix.sxa.SxaVertxException
     */
    @Override
    public boolean validate(final AcsNbiRequest nbiRequest, final AcsApiCrudTypeEnum crudType) throws SxaVertxException {
        switch (crudType) {
            case Create:
            case Update:
                // Validate the request by simply instantiate a new Subscriber POJO
                VertxJsonUtils.validateFields(nbiRequest.body, Subscriber.MANDATORY_FIELDS, Subscriber.OPTIONAL_FIELDS);

                // Request Tracker
                final SubscriberRequestTracker reqTracker = new SubscriberRequestTracker();
                reqTracker.orgId = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ORG_ID);
                reqTracker.subscriberName = nbiRequest.body.getString(AcsConstants.FIELD_NAME_NAME);
                nbiRequest.serviceData = reqTracker;

                // Validate App Data
                String servicePrefix = Subscriber.getAppDataField(
                        nbiRequest.body,
                        Subscriber.FIELD_NAME_APP_SERVICE_CONNECT,
                        Subscriber.FIELD_NAME_SERVICE_PREFIX
                );

                if (servicePrefix != null) {
                    // The max length of "servicePrefix" is 20 chars and can only contain letters
                    // (upper and lower cases) and/or digits (0-9) and/or dots (".").
                    if (servicePrefix.length() > 20) {
                        throw INVALID_SERVICE_PREFIX;
                    }

                    for (int i = 0; i < servicePrefix.length(); i ++) {
                        char aChar = servicePrefix.charAt(i);
                        if (aChar == '.' ||
                                (aChar >= '0' && aChar <= '9') ||
                                (aChar >= 'A' && aChar <= 'Z') ||
                                (aChar >= 'a' && aChar <= 'z')) {
                            // good
                        } else {
                            log.error("Found illegal char " + aChar + "!");
                            throw INVALID_SERVICE_PREFIX;
                        }
                    }
                }

                JsonArray locations = nbiRequest.body.getArray(Subscriber.FIELD_NAME_LOCATIONS);
                if (locations != null) {

                    // Validate all locations (and the associated devices per location)
                    int primaryLocationIndex = -1;
                    for (int i = 0; i < locations.size(); i ++) {
                        JsonObject aLocation = locations.get(i);
                        VertxJsonUtils.validateFields(
                                aLocation,
                                Subscriber.LOCATIONS_MANDATORY_FIELDS,
                                Subscriber.LOCATIONS_OPTIONAL_FIELDS
                        );

                        // Check for multiple primary locations
                        if (aLocation.getBoolean(Subscriber.FIELD_NAME_LOCATIONS_PRIMARY, false)) {
                            if (primaryLocationIndex >= 0) {
                                throw CANNOT_HAVE_MULTIPLE_PRIMARY_LOCATIONS;
                            } else {
                                primaryLocationIndex = i;
                            }
                        }

                        // Add the associated devices to Array
                        JsonArray deviceIdArray = aLocation.getArray(Subscriber.FIELD_NAME_LOCATIONS_DEVICES);
                        if (deviceIdArray != null) {
                            for (int j = 0; j < deviceIdArray.size(); j ++) {
                                reqTracker.allDeviceIdStrings.add(deviceIdArray.get(j));
                            }
                        }

                        // Check for multiple primary contacts in the same location
                        JsonArray contacts = aLocation.getArray(Subscriber.FIELD_NAME_LOCATIONS_CONTACTS);
                        if (contacts != null && contacts.size() > 0) {
                            int primaryContactIndex = -1;
                            for (int j = 0; j < contacts.size(); j ++) {
                                JsonObject aContact = contacts.get(j);
                                VertxJsonUtils.validateFields(
                                        aContact,
                                        Subscriber.CONTACTS_MANDATORY_FIELDS,
                                        Subscriber.CONTACTS_OPTIONAL_FIELDS
                                );

                                // Check for multiple primary contacts
                                if (aContact.getBoolean(Subscriber.FIELD_NAME_LOCATIONS_CONTACTS_PRIMARY, false)) {
                                    if (primaryContactIndex >= 0) {
                                        throw CANNOT_HAVE_MULTIPLE_PRIMARY_CONTACTS;
                                    } else {
                                        primaryContactIndex = j;
                                    }
                                }
                            }
                        }
                    }

                    /**
                     * Try to find all matching devices
                     */
                    if (reqTracker.allDeviceIdStrings.size() > 0) {
                        /**
                         * Save the matcher in order to update the "subscriberId" for all matching
                         * devices.
                         */
                        reqTracker.matcher = getDeviceMatcherByDeviceIdArray(reqTracker.orgId, reqTracker.allDeviceIdStrings);

                        VertxMongoUtils.FindHandler findDeviceHandler = new VertxMongoUtils.FindHandler(
                                new Handler<JsonArray>() {
                                    @Override
                                    public void handle(JsonArray queryResult) {
                                        // Check for MongoDB timed out
                                        if (VertxMongoUtils.FIND_TIMED_OUT.equals(queryResult)) {
                                            nbiRequest.sendResponse(
                                                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                    MONGODB_TIMED_OUT
                                            );
                                            return;
                                        }

                                        /**
                                         * make sure all internal IDs are found
                                         */
                                        for (int i = 0; i < reqTracker.allDeviceIdStrings.size(); i ++) {
                                            String deviceId = reqTracker.allDeviceIdStrings.get(i);
                                            Subscriber.DeviceIdStringType deviceIdStringType =
                                                    Subscriber.getDeviceIdStringType(reqTracker.orgId, deviceId);
                                            if (deviceIdStringType.equals(Subscriber.DeviceIdStringType.INTERNAL_ID)) {
                                                boolean bMatched = false;

                                                // Traverse query results
                                                for (int k = 0; k < queryResult.size(); k++) {
                                                    JsonObject aDevice = queryResult.get(k);
                                                    if (aDevice.getString(AcsConstants.FIELD_NAME_ID).equals(deviceId)) {
                                                        bMatched = true;
                                                        break;
                                                    }
                                                }

                                                if (bMatched == false) {
                                                    log.error(ERROR_STRING_UNKNOWN_DEVICE + "(" + deviceId + ")");
                                                    nbiRequest.sendResponse(
                                                            HttpResponseStatus.BAD_REQUEST,
                                                            UNKNOWN_DEVICE
                                                    );
                                                    return;
                                                }
                                            }
                                        }

                                        // Save the query results
                                        reqTracker.matchingDevices = queryResult;

                                        // Continue
                                        postValidation(nbiRequest, crudType);
                                    }
                                }
                        );

                        // Kick off the query
                        VertxMongoUtils.find(
                                vertx.eventBus(),
                                Cpe.CPE_COLLECTION_NAME,
                                reqTracker.matcher,
                                findDeviceHandler,
                                FIND_DEVICE_QUERY_KEY,
                                reqTracker.allDeviceIdStrings.size()
                        );

                        // Stop here and resume when the query results are back
                        return VALIDATION_PENDING_OR_FAILED;
                    }
                }
                break;
        }

        return VALIDATION_SUCCEEDED;
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
        }
    };

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
     * @throws com.calix.sxa.SxaVertxException  if one or more index fields are missing.
     */
    @Override
    public JsonObject buildIndexMatcher(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType)
            throws SxaVertxException{
        String customId = nbiRequest.body.getString(Subscriber.FIELD_NAME_CUSTOM_ID);
        SubscriberRequestTracker reqTracker = nbiRequest.getServiceData();
        String servicePrefix = reqTracker == null? null : reqTracker.servicePrefix;

        if (customId != null ||
                servicePrefix != null ||
                (reqTracker.allDeviceIdStrings != null && reqTracker.allDeviceIdStrings.size() > 0)) {
            JsonObject indexMatcher = new JsonObject();

            /**
             * Add "servicePrefix" into the index matcher to make sure uniqueness on Creation
             */
            JsonArray or = new JsonArray();
            if (customId != null) {
                or.add(new JsonObject().putString(Subscriber.FIELD_NAME_CUSTOM_ID, customId));
            }
            if (servicePrefix != null) {
                or.add(new JsonObject().putString(Subscriber.SERVICE_PREFIX_FULL_PATH, servicePrefix));
            }
            if (reqTracker.allDeviceIdStrings != null && reqTracker.allDeviceIdStrings.size() > 0) {
                JsonObject elemMatch = new JsonObject()
                        .putArray(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN, reqTracker.allDeviceIdStrings);
                or.add(new JsonObject()
                        .putObject(
                                Subscriber.FIELD_NAME_LOCATIONS + "." + Subscriber.FIELD_NAME_LOCATIONS_DEVICES,
                                new JsonObject().putObject("$elemMatch", elemMatch)
                        )
                );
            }

            indexMatcher
                    .putArray(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR, or)
                    .putString(
                            AcsConstants.FIELD_NAME_ORG_ID,
                            nbiRequest.body.getString(AcsConstants.FIELD_NAME_ORG_ID)
                    );

            return indexMatcher;
        } else {
            return null;
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
            final AcsNbiRequest nbiRequest,
            final HttpResponseStatus httpResponseStatus,
            final String id,
            final String error) {
        boolean superResult = super.postCreate(nbiRequest, httpResponseStatus, id, error);

        if (httpResponseStatus.equals(HttpResponseStatus.OK) && id != null) {
            final SubscriberRequestTracker requestTracker = nbiRequest.getServiceData();

            /**
             * Update Search Engine first
             */
            nbiRequest.body.removeField(AcsConstants.FIELD_NAME_CREATE_TIME);
            SxaStagerApiUtils.createOrUpdateSubscriber(
                    nbiRequest.body,
                    new Handler<Boolean>() {
                        @Override
                        public void handle(Boolean bUpdatedSearchEngine) {
                            if (Boolean.TRUE.equals(bUpdatedSearchEngine)) {
                                if (requestTracker.allDeviceIdStrings.size() > 0) {
                                    /**
                                     * Associate Devices
                                     */
                                    associateDevice(
                                            nbiRequest,
                                            requestTracker.orgId,
                                            requestTracker.allDeviceIdStrings,
                                            new Handler<Long>() {
                                                @Override
                                                public void handle(Long result) {
                                                    sendCreateResponse(nbiRequest, httpResponseStatus, id, error);
                                                }
                                            }
                                    );
                                } else {
                                    sendCreateResponse(nbiRequest, HttpResponseStatus.OK, id, null);
                                }
                            } else {
                                /**
                                 * Failed to update search engine.
                                 *
                                 * Delete the newly created subscriber record
                                 */
                                try {
                                    VertxMongoUtils.delete(
                                            vertx.eventBus(),
                                            Subscriber.DB_COLLECTION_NAME,
                                            id,
                                            new Handler<Message<JsonObject>>() {
                                                @Override
                                                public void handle(Message<JsonObject> deleteResult) {
                                                    sendCreateResponse(
                                                            nbiRequest,
                                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                            null,
                                                            FAILED_TO_UPDATE_SEARCH_ENGINE_STRING);
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
            return true;
        }

        return superResult;
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
            final AcsNbiRequest nbiRequest,
            JsonObject newRecord,
            JsonObject oldRecord) {
        SubscriberRequestTracker reqTracker = nbiRequest.getServiceData();
        reqTracker.oldRecord = oldRecord;
        final String newServicePrefix = reqTracker == null? null : reqTracker.servicePrefix;

        /**
         * Extract the "servicePrefix" from old record
         */
        String oldServicePrefix = Subscriber.getAppDataField(
                oldRecord,
                Subscriber.FIELD_NAME_APP_SERVICE_CONNECT,
                Subscriber.FIELD_NAME_SERVICE_PREFIX
        );

        if (newServicePrefix != null && !newServicePrefix.equals(oldServicePrefix)) {
            /**
             * Make sure the service prefix is unique within the organization
             */
            try {
                VertxMongoUtils.findOne(
                        vertx.eventBus(),
                        getDbCollectionName(),
                        new JsonObject()
                                .putString(AcsConstants.FIELD_NAME_ORG_ID, newRecord.getString(AcsConstants.FIELD_NAME_ORG_ID))
                                .putString(Subscriber.SERVICE_PREFIX_FULL_PATH, newServicePrefix),
                        new VertxMongoUtils.FindOneHandler(
                                new Handler<JsonObject>() {
                                    @Override
                                    public void handle(JsonObject anotherSubscriber) {
                                        if (anotherSubscriber != null) {
                                            String error = "Service Prefix " + newServicePrefix
                                                    + " is already in use by subscriber "
                                                    + anotherSubscriber.getString(AcsConstants.FIELD_NAME_NAME) + "!";
                                            log.error(error);
                                            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST,
                                                    new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, error));
                                        } else {
                                            saveUpdate(nbiRequest);
                                        }
                                    }
                                }
                        ),
                        null        // null query key means returns everything
                );
            } catch (SxaVertxException e) {
                e.printStackTrace();
                nbiRequest.sendResponse(
                        HttpResponseStatus.BAD_REQUEST,
                        NO_MATCH_FOUND);
            }
            return VALIDATION_PENDING_OR_FAILED;
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
            final AcsNbiRequest nbiRequest,
            HttpResponseStatus responseStatus,
            String error) {
        boolean superResult = super.postUpdate(nbiRequest, responseStatus, error);

        if (HttpResponseStatus.OK.equals(responseStatus)) {
            /**
             * Update Search Engine
             */
            nbiRequest.body.removeField(AcsConstants.FIELD_NAME_CREATE_TIME);
            SxaStagerApiUtils.createOrUpdateSubscriber(
                    nbiRequest.body,
                    new Handler<Boolean>() {
                        @Override
                        public void handle(Boolean jbossResult) {
                            if (Boolean.TRUE.equals(jbossResult)) {
                                postUpdateJbossResultHandler(nbiRequest);

                                // Send HTTP Response now (do other thing at background)
                                sendUpdateResponse(
                                        nbiRequest,
                                        HttpResponseStatus.OK,
                                        null
                                );
                            } else {
                                /**
                                 * Failed to update Search Engine.
                                 *
                                 * Revert back to the old record
                                 */
                                SubscriberRequestTracker reqTracker = nbiRequest.getServiceData();
                                try {
                                    VertxMongoUtils.update(
                                            vertx.eventBus(),
                                            Subscriber.DB_COLLECTION_NAME,
                                            nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID),
                                            reqTracker.oldRecord,
                                            new Handler<Long>() {
                                                @Override
                                                public void handle(Long event) {
                                                    sendUpdateResponse(
                                                            nbiRequest,
                                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                            FAILED_TO_UPDATE_SEARCH_ENGINE_STRING
                                                    );
                                                }
                                            }
                                    );
                                } catch (SxaVertxException e) {
                                    e.printStackTrace();
                                    sendUpdateResponse(
                                            nbiRequest,
                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                            INTERNAL_SERVER_ERROR_CONTACT_CALIX
                                    );
                                }
                            }
                        }
                    }
            );
            return true;
        }

        return superResult;
    }

    public void postUpdateJbossResultHandler(final AcsNbiRequest nbiRequest) {
        // Extract all the devices that were associated to this subscriber prior to the update
        final SubscriberRequestTracker reqTracker = nbiRequest.getServiceData();
        final JsonArray toBeAssociated = new JsonArray();
        final JsonArray toBeDisassociated = new JsonArray();
        JsonArray oldAllDeviceIdStrings = new JsonArray();

        JsonArray oldLocations = reqTracker.oldRecord.getArray(Subscriber.FIELD_NAME_LOCATIONS);
        if (oldLocations != null) {
            // Look for device(s) to be dis-associated
            for (int i = 0; i < oldLocations.size(); i++) {
                JsonObject aLocation = oldLocations.get(i);

                // Add the associated devices to Array
                JsonArray deviceIdArray = aLocation.getArray(Subscriber.FIELD_NAME_LOCATIONS_DEVICES);
                if (deviceIdArray != null) {
                    for (int j = 0; j < deviceIdArray.size(); j++) {
                        String deviceId = deviceIdArray.get(j);
                        oldAllDeviceIdStrings.add(deviceId);
                        JsonObject matchingDevice = null;
                        if (reqTracker.matchingDevices != null) {
                            for (int k = 0; k < reqTracker.matchingDevices.size(); k++) {
                                JsonObject aDevice = reqTracker.matchingDevices.get(k);
                                Subscriber.DeviceIdStringType deviceIdStringType =
                                        Subscriber.getDeviceIdStringType(reqTracker.orgId, deviceId);
                                if (deviceIdStringType.equals(Subscriber.DeviceIdStringType.INTERNAL_ID)) {
                                    // internal device id
                                    if (deviceId.equals(aDevice.getString(AcsConstants.FIELD_NAME_ID))) {
                                        matchingDevice = aDevice;
                                        break;
                                    }
                                } else if (deviceIdStringType.equals(Subscriber.DeviceIdStringType.FSAN)) {
                                    // FSAN
                                    if (deviceId.equals(aDevice.getString(Cpe.DB_FIELD_NAME_SN))) {
                                        matchingDevice = aDevice;
                                        break;
                                    }
                                } else {
                                    // Must be RegId
                                    if (deviceId.equals(aDevice.getString(Cpe.DB_FIELD_NAME_REGISTRATION_ID))) {
                                        matchingDevice = aDevice;
                                        break;
                                    }
                                }
                            }
                        }

                        if (matchingDevice == null && !reqTracker.allDeviceIdStrings.contains(deviceId)) {
                            // Dis-associate a device
                            toBeDisassociated.add(deviceId);
                        }
                    }
                }
            }
        }

        // Look for device(s) to be associated
        for (int i = 0; i < reqTracker.allDeviceIdStrings.size(); i ++) {
            String deviceId = reqTracker.allDeviceIdStrings.get(i);
            if (!oldAllDeviceIdStrings.contains(deviceId)) {
                toBeAssociated.add(deviceId);
            }
        }

        log.debug("New Matching Devices: " + reqTracker.allDeviceIdStrings);
        log.debug("oldAllDeviceIdStrings: " + oldAllDeviceIdStrings);
        log.debug("toBeAssociated: " + toBeAssociated);
        log.debug("toBeDisassociated: " + toBeDisassociated);

        final String newDeviceId = (String)(toBeAssociated.size() == 1? toBeAssociated.get(0) : null);
        final String oldDeviceId = (String)(toBeDisassociated.size() == 1? toBeDisassociated.get(0) : null);

        /**
         * Is it a replacement operation?
         */
        final boolean bIsReplacement = toBeAssociated.size() == 1 &
                toBeDisassociated.size() == 1 &
                !toBeAssociated.equals(toBeDisassociated);
        log.debug("bIsReplacement: " + bIsReplacement);

        // Associate-Device Result Handler
        final Handler<Long> associateResultHandler =
                toBeAssociated.size() == 0?
                        null
                        :
                        new Handler<Long> () {
                            @Override
                            public void handle(Long numberOfDevicesAssociated) {
                                if (numberOfDevicesAssociated == null) {
                                    log.debug("Associated " + numberOfDevicesAssociated + " Device(s) (out of "
                                            + toBeAssociated + ") to subscriber " + reqTracker.subscriberName + ".");

                                    // Query service plan (if any)
                                    if (bIsReplacement) {
                                        try {
                                            VertxMongoUtils.findOne(
                                                    vertx.eventBus(),
                                                    ServicePlan.DB_COLLECTION_NAME,
                                                    new JsonObject()
                                                        .putString(
                                                                ServicePlan.FIELD_NAME_SUBSCRIBER_ID,
                                                                nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID)
                                                        )
                                                        .putString(
                                                                ServicePlan.FIELD_NAME_DEVICE_ID,
                                                                oldDeviceId
                                                        ),
                                                    new VertxMongoUtils.FindOneHandler(
                                                            new Handler<JsonObject>() {
                                                                @Override
                                                                public void handle(JsonObject servicePlan) {
                                                                    if (VertxMongoUtils.FIND_ONE_TIMED_OUT
                                                                            .equals(servicePlan)) {
                                                                        log.error(reqTracker.subscriberName
                                                                                + ": DB Timed out"
                                                                                + " when querying service plan!");
                                                                        servicePlan = null;
                                                                    }
                                                                    // Process the query result
                                                                    processServicePlanQueryResult(
                                                                            oldDeviceId,
                                                                            newDeviceId,
                                                                            nbiRequest,
                                                                            reqTracker,
                                                                            servicePlan
                                                                    );
                                                                }
                                                            }
                                                    ),
                                                    null
                                            );
                                        } catch (SxaVertxException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }
                        };

        if (toBeDisassociated.size() > 0) {
            // Disassociate-Device Result Handler
            Handler<Long> disassociateResultHandler = new Handler<Long> () {
                @Override
                public void handle(Long numberOfDevicesDisassociated) {
                    if (numberOfDevicesDisassociated != null) {
                        log.debug("Disassociated " + numberOfDevicesDisassociated + " Device(s) (out of "
                                + toBeDisassociated + ") from subscriber " + reqTracker.subscriberName + ".");
                        if (toBeAssociated.size() > 0) {
                            // Save the # of devices that have been disassociated
                            reqTracker.numberOfDevicesDisassociated = numberOfDevicesDisassociated;

                            // Do Associate
                            associateDevice(
                                    nbiRequest,
                                    reqTracker.orgId,
                                    toBeAssociated,
                                    associateResultHandler
                            );
                        } else {
                            // Delete Service plan if any
                            try {
                                VertxMongoUtils.deleteWithMatcher(
                                        vertx.eventBus(),
                                        ServicePlan.DB_COLLECTION_NAME,
                                        new JsonObject()
                                                .putString(
                                                        ServicePlan.FIELD_NAME_SUBSCRIBER_ID,
                                                        nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID)
                                                )
                                                .putObject(
                                                        ServicePlan.FIELD_NAME_DEVICE_ID,
                                                        new JsonObject().putObject(
                                                                VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN,
                                                                new JsonObject().putArray(
                                                                        VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN,
                                                                        toBeDisassociated
                                                                )
                                                        )
                                                ),
                                        null
                                );
                            } catch (SxaVertxException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            };

            // Do disassociate
            String replacementUnitDeviceFsan = null;
            if (bIsReplacement) {
                if (Subscriber.getDeviceIdStringType(reqTracker.orgId, newDeviceId)
                        .equals(Subscriber.DeviceIdStringType.FSAN)) {
                    // FSAN-based Replacement
                    replacementUnitDeviceFsan = newDeviceId;
                }
            }
            disassociateDevice(
                    nbiRequest,
                    reqTracker.orgId,
                    toBeDisassociated,
                    replacementUnitDeviceFsan,
                    disassociateResultHandler
            );
        } else if (toBeAssociated.size() > 0) {
            // Do Associate
            associateDevice(
                    nbiRequest,
                    reqTracker.orgId,
                    toBeAssociated,
                    associateResultHandler
            );
        }
    }

    /**
     * Query Keys when querying the old device (to be replaced)
     */
    public static final JsonObject REPLACEMENT_QUERY_OLD_DEVICE_KEYS = new JsonObject()
            .putNumber(Cpe.DB_FIELD_NAME_REGISTRATION_ID, 1)
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_MODEL_NAME, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CHANGE_COUNTER, 1);

    /**
     * Process the service plan query result (part of the replacement process)
     * @param oldDeviceId
     * @param newDeviceId
     * @param nbiRequest
     * @param reqTracker
     * @param servicePlan
     */
    public void processServicePlanQueryResult(
            final String oldDeviceId,
            final String newDeviceId,
            final AcsNbiRequest nbiRequest,
            final SubscriberRequestTracker reqTracker,
            final JsonObject servicePlan) {
        /**
         * Check if the new device exists too.
         */
        final JsonObject newDevice = findDeviceFromArrayByDeviceId(
                reqTracker.matchingDevices,
                newDeviceId
        );
        if (reqTracker.numberOfDevicesDisassociated > 0) {
            /**
             * Old Device exists in the system.
             */
            if (newDevice != null) {
                /**
                 * New Device Also Exists
                 */
                log.info(reqTracker.subscriberName + ": new device " + newDeviceId + " is found in DB.");

                /**
                 * Query the old device record and then either do one of the following 3 things:
                 *
                 * 1. replacement (if old device has an auto backup)
                 * 2. apply service plan (if old device does not have an auto backup. but does have a service plan)
                 * 3. nothing (if old device does not have an auto backup or a service plan)
                 */
                log.info(reqTracker.subscriberName + ": Querying the old device " + oldDeviceId + "...");
                try {
                    VertxMongoUtils.findOne(
                            vertx.eventBus(),
                            Cpe.CPE_COLLECTION_NAME,
                            Subscriber.getDeviceMatcherByDeviceId(
                                    reqTracker.orgId,
                                    oldDeviceId
                            ),
                            new VertxMongoUtils.FindOneHandler(
                                    new Handler<JsonObject>() {
                                        @Override
                                        public void handle(JsonObject oldDevice) {
                                            boolean bDoReplacement = false;

                                            if (oldDevice == null) {
                                                log.info(reqTracker.subscriberName + ": Old Device does not exist in DB.");
                                            } else if (oldDevice.equals(VertxMongoUtils.FIND_ONE_TIMED_OUT)) {
                                                log.error(reqTracker.subscriberName + ": DB Timed out when querying "
                                                        + "old Device (" + oldDeviceId + ")!");
                                            } else if (oldDevice.containsField(Cpe.DB_FIELD_NAME_CHANGE_COUNTER)) {
                                                log.info(reqTracker.subscriberName + ": Found the old device ("
                                                        + oldDeviceId + ") which supports change counter.");

                                                /**
                                                 * Check for model mismatch
                                                 */
                                                String oldModel = oldDevice.getString(CpeDeviceType.FIELD_NAME_MODEL_NAME);
                                                String newModel = newDevice.getString(CpeDeviceType.FIELD_NAME_MODEL_NAME);
                                                if (oldModel != null && oldModel.equals(newModel)) {
                                                    bDoReplacement = true;
                                                } else {
                                                    log.error(reqTracker.subscriberName
                                                            + ": Model of Old Device " + oldDeviceId
                                                            + " is " + oldModel + ", while model of the new device "
                                                            + newDeviceId + " is " + newModel
                                                            + " thus cannot perform regular replacement operation.");
                                                }
                                            } else {
                                                log.info(reqTracker.subscriberName
                                                        + ": Found the old device (" + oldDeviceId + ")"
                                                        + " which does not support change counter.");
                                            }

                                            if (bDoReplacement) {
                                                /**
                                                 * Perform Replacement Now
                                                 */
                                                ReplacementUtils.doReplacement(
                                                        vertx.eventBus(),
                                                        redisClient,
                                                        null,
                                                        newDevice,
                                                        reqTracker.orgId,
                                                        newDeviceId,
                                                        oldDevice.getString(AcsConstants.FIELD_NAME_ID),
                                                        false
                                                );

                                                if (servicePlan != null) {
                                                    /**
                                                     * Attach the service plan to new device
                                                     * (DB change only, no device op)
                                                     */
                                                    log.info(reqTracker.subscriberName + ": Moving Service Plan from "
                                                            + oldDeviceId + " to " + newDeviceId + "...");
                                                    ServicePlanService.attachServicePlanToDevice(
                                                            log,
                                                            vertx.eventBus(),
                                                            servicePlan.getString(AcsConstants.FIELD_NAME_ID),
                                                            newDeviceId
                                                        );
                                                }
                                            } else if (servicePlan != null) {
                                                /**
                                                 * Old Device has No Auto Backup, Apply service plan to new device
                                                 *
                                                 * (this shall never happen though)
                                                 */
                                                ServicePlanService.applyServicePlan(
                                                        log,
                                                        vertx.eventBus(),
                                                        servicePlan,
                                                        passiveWorkflowCache,
                                                        reqTracker.orgId,
                                                        newDevice
                                                );
                                            }
                                        }
                                    }
                            ),
                            REPLACEMENT_QUERY_OLD_DEVICE_KEYS
                    );
                } catch (SxaVertxException e) {
                    e.printStackTrace();
                }
            }
        } else if (servicePlan != null) {
            /**
             * Service Plan found for the old device id
             *
             * Old Device does NOT exist in the system.
             *
             * Attach service plan to the new device (may apply service plan to the new device if the new device exists)
             */
            if (newDevice != null) {
                log.info(reqTracker.subscriberName + ": Applying Service plan to new device " + newDeviceId + "...");
                ServicePlanService.applyServicePlan(
                        log,
                        vertx.eventBus(),
                        servicePlan,
                        passiveWorkflowCache,
                        reqTracker.orgId,
                        newDevice
                );
            } else {
                /**
                 * New Device does not exist yet. Just attach the service plan to it
                 */
                log.info(reqTracker.subscriberName + ": Attaching Service plan to new device " + newDeviceId + "...");
                ServicePlanService.attachServicePlanToDevice(
                        log,
                        vertx.eventBus(),
                        servicePlan.getString(AcsConstants.FIELD_NAME_ID),
                        newDeviceId
                );
            }
        } else {
            /**
             * Old Device does not exist, no service plan found either
             */
            log.info(reqTracker.subscriberName + ": Old device " + oldDeviceId +
                    " not found, no service plan found either.");
        }
    }

    /**
     * Find a device using given device id string from an array of JSON Object.
     *
     * @param devices
     * @param deviceId
     * @return
     */
    public JsonObject findDeviceFromArrayByDeviceId(JsonArray devices, String deviceId) {
        if (devices != null) {
            for (int i = 0; i < devices.size(); i ++) {
                JsonObject aMatchingDevice = devices.get(i);
                if (deviceId.equals(aMatchingDevice.getString(AcsConstants.FIELD_NAME_ID)) ||
                        deviceId.equals(aMatchingDevice.getString(Cpe.DB_FIELD_NAME_REGISTRATION_ID)) ||
                        deviceId.equals(aMatchingDevice.getString(Cpe.DB_FIELD_NAME_SN))) {
                    return aMatchingDevice;
                }
            }
        }

        return null;
    }

    /**
     * Associate Device(s) with Subscriber.
     *
     * Currently we only remove the "prevSubscriber" and the "toBeReplacedBy" fields from the device record.
     *
     * @param nbiRequest
     * @param orgId
     * @param deviceIdStrings
     * @param handler
     */
    public void associateDevice(
            AcsNbiRequest nbiRequest,
            String orgId,
            JsonArray deviceIdStrings,
            Handler<Long> handler) {
        if (deviceIdStrings == null || deviceIdStrings.size() == 0) {
            handler.handle(0L);
            return;
        }
        try {
            VertxMongoUtils.updateWithMatcher(
                    vertx.eventBus(),
                    Cpe.CPE_COLLECTION_NAME,
                    getDeviceMatcherByDeviceIdArray(orgId, deviceIdStrings),
                    VertxMongoUtils.getUpdatesObject(
                            null,
                            // Remove the "prevSubscriber" and the "toBeReplacedBy" fields from the device record
                            VertxMongoUtils.addUnset(
                                    VertxMongoUtils.addUnset(null, Cpe.DB_FIELD_NAME_TO_BE_REPLACED_BY),
                                    Cpe.DB_FIELD_NAME_PREV_SUBSCRIBER
                            ),
                            null,
                            null,
                            null
                    ),
                    handler
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }

        // Save Event
        SubscriberRequestTracker reqTracker = nbiRequest.getServiceData();
        if (reqTracker.matchingDevices != null && reqTracker.matchingDevices.size() > 0) {
            for (int i = 0; i < reqTracker.matchingDevices.size(); i++) {
                JsonObject aDevice = reqTracker.matchingDevices.get(i);
                Event.saveEvent(
                        vertx.eventBus(),
                        reqTracker.orgId,
                        aDevice.getString(Cpe.DB_FIELD_NAME_SN),
                        EventTypeEnum.Association,
                        EventSourceEnum.System,
                        new JsonObject().putString(
                                "subscriber",
                                nbiRequest.body.getString(AcsConstants.FIELD_NAME_NAME)
                        )
                );
            }
        }
    }

    /**
     * Disassociate Device(s) with Subscriber.
     *
     * Currently we only update the "prevSubscriber" and the "toBeReplacedBy" fields in the device record.
     *
     * @param nbiRequest
     * @param orgId
     * @param deviceIdStrings
     * @param replacementUnitFsan
     * @param handler
     */
    public void disassociateDevice(
            final AcsNbiRequest nbiRequest,
            final String orgId,
            final JsonArray deviceIdStrings,
            final String replacementUnitFsan,
            final Handler<Long> handler) {
        if (deviceIdStrings == null || deviceIdStrings.size() == 0) {
            handler.handle(0L);
            return;
        }

        final SubscriberRequestTracker reqTracker = nbiRequest.getServiceData();

        /**
         * Query Devices to be disassociated first
         */
        try {
            VertxMongoUtils.find(
                    vertx.eventBus(),
                    Cpe.CPE_COLLECTION_NAME,
                    getDeviceMatcherByDeviceIdArray(orgId, deviceIdStrings),
                    new VertxMongoUtils.FindHandler(
                            new Handler<JsonArray>() {
                                @Override
                                public void handle(JsonArray oldDevices) {
                                    if (VertxMongoUtils.FIND_TIMED_OUT.equals(oldDevices)) {
                                        log.error(reqTracker.subscriberName + ": DB Timed Out when querying devices to"
                                                + " be disassociated " + deviceIdStrings + "!");
                                        return;
                                    }

                                    if (oldDevices == null || oldDevices.size() == 0) {
                                        log.info(reqTracker.subscriberName + ": None of the to-be-disassociated "
                                                + "device(s) exist in the system");
                                        handler.handle(0L);
                                        return;
                                    }

                                    reqTracker.numberOfDevicesDisassociated = oldDevices.size();

                                    /**
                                     * Traverse query result and save disassociation events
                                     */
                                    JsonArray internalDeviceIdStrings = new JsonArray();
                                    for (int i = 0; i < oldDevices.size(); i++) {
                                        JsonObject anOldDevice = oldDevices.get(i);

                                        // Save Internal Device Id
                                        internalDeviceIdStrings.add(anOldDevice.getString(AcsConstants.FIELD_NAME_ID));

                                        // Save Event
                                        Event.saveEvent(
                                                vertx.eventBus(),
                                                reqTracker.orgId,
                                                anOldDevice.getString(Cpe.DB_FIELD_NAME_SN),
                                                EventTypeEnum.Disassociation,
                                                EventSourceEnum.System,
                                                new JsonObject().putString("subscriber name", reqTracker.subscriberName)
                                        );

                                        /**
                                         * Do the actual disassociation
                                         */
                                        try {
                                            JsonObject sets = new JsonObject()
                                                    .putString(
                                                            Cpe.DB_FIELD_NAME_PREV_SUBSCRIBER,
                                                            nbiRequest.body.getString(Subscriber.FIELD_NAME_NAME)
                                                    );
                                            if (replacementUnitFsan != null) {
                                                sets.putString(Cpe.DB_FIELD_NAME_TO_BE_REPLACED_BY, replacementUnitFsan);
                                            }

                                            VertxMongoUtils.updateWithMatcher(
                                                    vertx.eventBus(),
                                                    Cpe.CPE_COLLECTION_NAME,
                                                    getDeviceMatcherByDeviceIdArray(orgId, deviceIdStrings),
                                                    VertxMongoUtils.getUpdatesObject(
                                                            sets,
                                                            null,
                                                            null,
                                                            null,
                                                            null
                                                    ),
                                                    handler
                                            );
                                        } catch (SxaVertxException e) {
                                            e.printStackTrace();
                                            handler.handle(0L);
                                        }
                                    }
                                }
                            }
                    ),
                    FIND_DEVICE_QUERY_KEY,
                    deviceIdStrings.size()
            );
        } catch (Exception ex) {
            ex.printStackTrace();
            handler.handle(0L);
        }
    }

    /**
     * Allowed Query Parameter Name/Type Pairs
     */
    private static final HashMap<String, VertxJsonUtils.JsonFieldType> QUERY_PARAMETER_NAME_TYPE_PAIRS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(Subscriber.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
                    .append(Subscriber.FIELD_NAME_CUSTOM_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(Subscriber.FIELD_NAME_LOCATIONS_CONTACTS_PHONE, VertxJsonUtils.JsonFieldType.String)
                    .append(Subscriber.FIELD_NAME_LOCATIONS_CONTACTS_EMAIL, VertxJsonUtils.JsonFieldType.String)
            ;
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
     * Build MongoDB Matcher for Retrieve
     */
    @Override
    public JsonObject buildRetrieveMatcher(AcsNbiRequest nbiRequest) throws SxaVertxException{
        // Use "_id" whenever possible
        if (nbiRequest.body.containsField(AcsConstants.FIELD_NAME_ID)) {
            return new JsonObject()
                    .putString(AcsConstants.FIELD_NAME_ID, nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID))
                    .putString(AcsConstants.FIELD_NAME_ORG_ID, nbiRequest.body.getString(AcsConstants.FIELD_NAME_ORG_ID)
                    );
        }

        // Build the matcher
        JsonObject matcher = new JsonObject();
        String fullName = nbiRequest.body.getString(Subscriber.FIELD_NAME_NAME);
        if (fullName != null) {
            matcher.putString(Subscriber.FIELD_NAME_NAME, fullName);
        }
        String customId = nbiRequest.body.getString(Subscriber.FIELD_NAME_CUSTOM_ID);
        if (customId != null) {
            matcher.putString(Subscriber.FIELD_NAME_CUSTOM_ID, customId);
        }
        String email = nbiRequest.body.getString(Subscriber.FIELD_NAME_LOCATIONS_CONTACTS_EMAIL);
        String phoneNumber = nbiRequest.body.getString(Subscriber.FIELD_NAME_LOCATIONS_CONTACTS_PHONE);
        if (phoneNumber != null || email != null) {
            JsonObject elemMatch = new JsonObject();
            if (phoneNumber != null) {
                elemMatch.putString(Subscriber.FIELD_NAME_LOCATIONS_CONTACTS_PHONE, phoneNumber);
            }
            if (email != null) {
                elemMatch.putString(Subscriber.FIELD_NAME_LOCATIONS_CONTACTS_EMAIL, email);
            }
            matcher.putObject(
                    Subscriber.FIELD_NAME_LOCATIONS + "." + Subscriber.FIELD_NAME_LOCATIONS_CONTACTS,
                    new JsonObject().putObject("$elemMatch", elemMatch)
            );
        }
        if (matcher.size() == 0) {
            log.error("Cannot build index matcher for request:\n" + nbiRequest.body.encodePrettily());
            throw NO_QUERY_FILTER_FOUND;
        }

        // Add orgId
        matcher.putString(AcsConstants.FIELD_NAME_ORG_ID, nbiRequest.body.getString(AcsConstants.FIELD_NAME_ORG_ID));

        return matcher;
    }

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_SUBSCRIBER;
    }

    /**
     * Build MongoDB Query Keys for Retrieve.
     *
     * Default to null (return everything)
     */
    private static final JsonObject INTERNAL_QUERY_KEYS = null;
    private static final JsonObject EXTERNAL_QUERY_KEYS = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ORG_ID, 0)
            .putNumber(Subscriber.FIELD_NAME_APP_DATA, 0);
    @Override
    public JsonObject buildRetrieveQueryKeys(AcsNbiRequest nbiRequest) {
        if (nbiRequest.bInternalRequest) {
            return INTERNAL_QUERY_KEYS;
        } else {
            return EXTERNAL_QUERY_KEYS;
        }
    }

    /**
     * Get a Matcher for an array of device id strings
     * 
     * @param orgId 
     * @param deviceIdArray
     */
    public JsonObject getDeviceMatcherByDeviceIdArray(String orgId, JsonArray deviceIdArray)
            throws SxaVertxException {
        JsonArray allRegIDs = new JsonArray();
        JsonArray allFSANs = new JsonArray();
        JsonArray allInternalIDs = new JsonArray();

        for (int j = 0; j < deviceIdArray.size(); j ++) {
            String deviceId = deviceIdArray.get(j);
            switch (Subscriber.getDeviceIdStringType(orgId, deviceId)) {
                case INTERNAL_ID:
                    // Found an internal device id
                    allInternalIDs.add(deviceId);
                    break;

                case FSAN:
                    // Found an FSAN
                    allFSANs.add(deviceId);
                    break;

                case REG_ID:
                    // Found a RegId
                    allRegIDs.add(deviceId);
                    break;

                default:
                    throw new SxaVertxException("Invalid Device Id String " + deviceId + "!");
            }
        }

        // Build matcher
        JsonObject matcher;
        JsonObject fsanMatcher = null;
        JsonObject regIdMatcher = null;
        JsonObject internalIdMatcher = null;
        int subMatcherCount = 0;

        if (allFSANs.size() > 0) {
            subMatcherCount ++;
            if (allFSANs.size() == 1) {
                fsanMatcher = new JsonObject().putString(Cpe.DB_FIELD_NAME_SN, allFSANs.get(0).toString());
            } else {
                fsanMatcher = new JsonObject().putObject(
                        Cpe.DB_FIELD_NAME_SN,
                        new JsonObject().putArray(
                                VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN,
                                allFSANs
                        )
                );
            }
        }
        if (allRegIDs.size() > 0) {
            subMatcherCount ++;
            if (allRegIDs.size() == 1) {
                regIdMatcher = new JsonObject().putString(Cpe.DB_FIELD_NAME_REGISTRATION_ID, allRegIDs.get(0).toString());
            } else {
                regIdMatcher = new JsonObject().putObject(
                        Cpe.DB_FIELD_NAME_REGISTRATION_ID,
                        new JsonObject().putArray(
                                VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN,
                                allRegIDs
                        )
                );
            }
        }
        if (allInternalIDs.size() > 0) {
            subMatcherCount ++;
            if (allInternalIDs.size() == 1) {
                internalIdMatcher = new JsonObject().putString(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID,
                        allInternalIDs.get(0).toString());
            } else {
                internalIdMatcher = new JsonObject().putObject(
                        VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID,
                        new JsonObject().putArray(
                                VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN,
                                allInternalIDs
                        )
                );
            }
        }

        if (subMatcherCount > 1) {
            // need to do "$or"
            JsonArray matcherArray = new JsonArray();
            if (regIdMatcher != null) {
                matcherArray.add(regIdMatcher);
            }
            if (fsanMatcher != null) {
                matcherArray.add(fsanMatcher);
            }
            if (internalIdMatcher != null) {
                matcherArray.add(internalIdMatcher);
            }
            matcher = new JsonObject().putArray(
                    VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR,
                    matcherArray
            );
        } else {
            if (regIdMatcher != null) {
                matcher = regIdMatcher;
            } else if (fsanMatcher != null) {
                matcher = fsanMatcher;
            } else {
                matcher = internalIdMatcher;
            }
        }

        matcher.putString(AcsConstants.FIELD_NAME_ORG_ID, orgId);

        return matcher;
    }

    /**
     * When returning an HTTP "409 Conflict" error, call this method to get more error details.
     *
     * @param nbiRequest
     * @param crudType
     * @param otherRecord
     */
    @Override
    public JsonObject getConflictDetails(
            AcsNbiRequest nbiRequest,
            AcsApiCrudTypeEnum crudType,
            JsonObject otherRecord) {
        JsonObject details = super.getConflictDetails(nbiRequest, crudType, otherRecord);

        if (details != null &&
                !details.equals(CONFLICT) &&
                details.containsField(AcsConstants.FIELD_NAME_ERROR)) {
            String error = details.getString(AcsConstants.FIELD_NAME_ERROR);

            if (error.contains(Subscriber.FIELD_NAME_CUSTOM_ID)) {
                if (nbiRequest.bInternalRequest) {
                    // Convert "customId" to "account number" for internal requests
                    return ACCT_NUMBER_IN_USE;
                }
            } else if (error.contains(Subscriber.FIELD_NAME_LOCATIONS + "." + Subscriber.FIELD_NAME_LOCATIONS_DEVICES)) {
                // Device(s) association conflict
                SubscriberRequestTracker reqTracker = nbiRequest.getServiceData();
                if (reqTracker.allDeviceIdStrings.size() > 1) {
                    return MULTI_DEVICE_ALREADY_ASSOCIATED;
                } else {
                    String deviceId = reqTracker.allDeviceIdStrings.get(0);
                    Subscriber.DeviceIdStringType deviceIdStringType =
                            Subscriber.getDeviceIdStringType(reqTracker.orgId, deviceId);
                    details.putString(
                            AcsConstants.FIELD_NAME_ERROR,
                            deviceIdStringType.typeString + " " + reqTracker.allDeviceIdStrings.get(0)
                                    + " is already associated with another Subscriber!"
                    );
                }
            }
        }

        // Return the error details returned from super
        return details;
    }

    /**
     * When deleting a single record, allow finding the record (and save it as request body)) before deleting it.
     *
     * Disabled by Default.
     *
     * Only applicable when bulk delete is disallowed.
     */
    @Override
    public boolean bFindOneBeforeDelete() {
        return true;
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
    @Override
    public boolean processToBeDeletedRecord(
            final AcsNbiRequest nbiRequest,
            final JsonObject matcher,
            final JsonObject record) {
        if (record == null)
            return false;

        /**
         * Update Search engine
         */
        SxaStagerApiUtils.deleteSubscriber(
                record.getString(AcsConstants.FIELD_NAME_ID),
                new Handler<Boolean>() {
                    @Override
                    public void handle(Boolean jbossDeleteResult) {
                        if (Boolean.TRUE.equals(jbossDeleteResult)) {
                            postDeleteJbossResultHandler(nbiRequest, matcher, record);
                        } else {
                            nbiRequest.sendResponse(
                                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                    FAILED_TO_UPDATED_SEARCH_ENGINE
                            );
                        }
                    }
                }
        );
        return true;
    }

    /**
     * Handler called after calling the SXA JBoss Delete API.
     *
     * @param nbiRequest
     * @param matcher
     * @param record
     */
    public void postDeleteJbossResultHandler(
            final AcsNbiRequest nbiRequest,
            final JsonObject matcher,
            final JsonObject record) {
        /**
         * Delete service plan(s) if any
         */
        try {
            VertxMongoUtils.deleteWithMatcher(
                    vertx.eventBus(),
                    ServicePlan.DB_COLLECTION_NAME,
                    new JsonObject().putString(
                            ServicePlan.FIELD_NAME_SUBSCRIBER_ID,
                            record.getString(AcsConstants.FIELD_NAME_ID)
                    ),
                    new Handler<Message<JsonObject>>() {
                        @Override
                        public void handle(Message<JsonObject> result) {
                            if (result == null) {
                                nbiRequest.sendResponse(
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                        MONGODB_TIMED_OUT);
                            } else {
                                log.debug("Deleted "
                                        + result.body().getField(VertxMongoUtils.MOD_MONGO_FIELD_NAME_NUMBER)
                                        + " service plan(s) for Subscriber "
                                        + record.getString(Subscriber.FIELD_NAME_NAME));

                                // Build an array with all device id string
                                final JsonArray allDeviceIdStrings = Subscriber.getAllDeviceIdStrings(record);
                                if (allDeviceIdStrings.size() > 0) {
                                    SubscriberRequestTracker reqTracker = new SubscriberRequestTracker();
                                    reqTracker.oldRecord = record;
                                    reqTracker.orgId = record.getString(AcsConstants.FIELD_NAME_ORG_ID);
                                    reqTracker.subscriberName = record.getString(AcsConstants.FIELD_NAME_NAME);
                                    nbiRequest.serviceData = reqTracker;
                                    disassociateDevice(
                                            nbiRequest,
                                            record.getString(AcsConstants.FIELD_NAME_ORG_ID),
                                            allDeviceIdStrings,
                                            null,
                                            new Handler<Long>() {
                                                @Override
                                                public void handle(Long disassociateResult) {
                                                    if (disassociateResult == null) {
                                                        // Timed out
                                                        nbiRequest.sendResponse(
                                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                MONGODB_TIMED_OUT
                                                        );
                                                    } else {
                                                        log.debug("Disassociated " + disassociateResult
                                                                + " device(s) from subscriber "
                                                                + record.getString(Subscriber.FIELD_NAME_NAME));

                                                        // Done
                                                        doDeleteNow(nbiRequest, matcher);
                                                    }
                                                }
                                            }
                                    );
                                }
                            }
                        }
                    }
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }
    }
}
