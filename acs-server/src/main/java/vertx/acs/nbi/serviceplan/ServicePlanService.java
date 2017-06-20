package vertx.acs.nbi.serviceplan;

import io.vertx.ext.mongo.MongoClient;
import vertx.VertxException;
import vertx.VertxMongoUtils;
import vertx.acs.cache.PassiveWorkflowCache;
import vertx.acs.nbi.AbstractAcNbiCrudService;
import vertx.acs.nbi.model.AcsNbiRequest;
import vertx.model.*;
import vertx.util.AcsApiUtils;
import vertx.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

/**
 * Project:  844e_mvp
 *
 * @author: ronyang
 */
public class ServicePlanService extends AbstractAcNbiCrudService {
    /**
     * Inner Class for Request Tracker
     */
    public static class ServerPlanReqTracker {
        String orgId = null;
        JsonObject device = null;
        String newRecordId = null;          // for create only
        JsonObject oldRecord = null;        // for update only
        boolean bDeviceIdChanged = false;    // for update only
    }

    /**
     * Static Errors
     */
    public static final JsonObject CANNOT_ASSIGN_MULTIPLE_SERVICE_PLANS = new JsonObject().put(
            AcsConstants.FIELD_NAME_ERROR,
            "Cannot Assign Multiple Service Plans to the Same Device!"
    );

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     * <p/>
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_SERVICE_PLAN;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     */
    @Override
    public String getDbCollectionName() {
        return ServicePlan.DB_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     * <p/>
     * If more than one index fields are defined, they are considered as "AND" relation, i.e. all index fields
     * combined together to represent the unique index.
     */
    @Override
    public String[] getIndexFieldName() {
        return ServicePlan.INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return ServicePlan.EDITABLE_FIELDS;
    }

    /**
     * Static Query Keys that define the interesting DB fields when querying CPE devices collection.
     */
    public static JsonObject DEVICE_QUERY_KEYS = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ID, 1)
            .put(AcsConstants.FIELD_NAME_ORG_ID, 1)
            .put(CpeDeviceType.FIELD_NAME_MANUFACTURER, 1)
            .put(CpeDeviceType.FIELD_NAME_OUI, 1)
            .put(CpeDeviceType.FIELD_NAME_SW_VER, 1)
            .put(CpeDeviceType.FIELD_NAME_HW_VER, 1)
            .put(CpeDeviceType.FIELD_NAME_MODEL_NAME, 1)
            .put(Cpe.DB_FIELD_NAME_SN, 1)
            .put(Cpe.DB_FIELD_NAME_CONNREQ_URL, 1)
            .put(Cpe.DB_FIELD_NAME_CONNREQ_USERNAME, 1)
            .put(Cpe.DB_FIELD_NAME_CONNREQ_PASSWORD, 1);

    /**
     * Validate an NBI Request.
     * <p/>
     * Must be implemented by actual services.
     * <p/>
     * If authentication is needed, this is the right place to implement.
     * <p/>
     * If validation is completed, returns true.
     * If validation is not completed (for example pending a further DB query callback), returns false.
     *
     * @param nbiRequest
     * @param crudType   Type of the CRUD operation.
     * @return boolean
     * @throws vertx.VertxException
     */
    @Override
    public boolean validate(final AcsNbiRequest nbiRequest, final AcsApiCrudTypeEnum crudType) throws VertxException {
        switch (crudType) {
            case Create:
            case Update:
                ServicePlan.validate(nbiRequest.body, dialPlanCache);

                // Create/Save Request Tracker
                final ServerPlanReqTracker reqTracker = new ServerPlanReqTracker();
                nbiRequest.serviceData = reqTracker;
                reqTracker.orgId = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ORG_ID);

                // extract Subscriber Id
                final String subscriberId = nbiRequest.body.getString(ServicePlan.FIELD_NAME_SUBSCRIBER_ID);

                /**
                 * Define a subscriber query result handler
                 */
                Handler subscriberQueryResultHandler =
                        new Handler<JsonObject>() {
                            @Override
                            public void handle(JsonObject result) {
                                if(result == null) {
                                    String error = "Cannot " + crudType.name() + " service plan because "
                                            + "subscriber id " + subscriberId + " is invalid!";
                                    nbiRequest.sendResponse(
                                            HttpResponseStatus.BAD_REQUEST,
                                            new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, error)
                                    );
                                } else if(result.equals(VertxMongoUtils.FIND_ONE_TIMED_OUT)) {
                                    nbiRequest.sendResponse(
                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                            MONGODB_TIMED_OUT
                                    );
                                } else {
                                    /**
                                     * Define a device query result handler
                                     */
                                    try {
                                        Handler deviceQueryResultHandler =

                                                new Handler<JsonObject>() {
                                                    @Override
                                                    public void handle(JsonObject result) {
                                                        if(result == null) {
                                                            /**
                                                             * No such device yet, i.e. pre provisioning
                                                             */
                                                            postValidation(nbiRequest, crudType);
                                                        } else if(result.equals(VertxMongoUtils.FIND_ONE_TIMED_OUT)) {
                                                            nbiRequest.sendResponse(
                                                                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                    MONGODB_TIMED_OUT
                                                            );
                                                        } else {
                                                            /**
                                                             * Found the device, save it for later use
                                                             */
                                                            reqTracker.device = result;

                                                            // Continue
                                                            postValidation(nbiRequest, crudType);
                                                        }
                                                    }
                                                };

                                        VertxMongoUtils.findOne(
                                                mongoClient,
                                                Cpe.CPE_COLLECTION_NAME,
                                                Subscriber.getDeviceMatcherByDeviceId(
                                                        reqTracker.orgId,
                                                        nbiRequest.body.getString(ServicePlan.FIELD_NAME_DEVICE_ID)
                                                ),
                                                deviceQueryResultHandler,
                                                DEVICE_QUERY_KEYS
                                        );
                                    } catch (VertxException e) {
                                        nbiRequest.sendResponse(
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                MONGODB_TIMED_OUT
                                        );
                                    }
                                }
                            }
                        };


                /**
                 * Make sure the Subscriber Id is valid
                 */
                VertxMongoUtils.findOne(
                        mongoClient,
                        Subscriber.DB_COLLECTION_NAME,
                        new JsonObject().put(AcsConstants.FIELD_NAME_ID, subscriberId),
                        subscriberQueryResultHandler,
                        null
                );
                return VALIDATION_PENDING_OR_FAILED;

            default:
                return VALIDATION_SUCCEEDED;
        }
    }

    /**
     * Skip non service fields Fields when updating.
     */
    public static Collection<String> UPDATE_COMPARE_SKIP_FIELDS = new ArrayList<String>() {{
        add(AcsConstants.FIELD_NAME_ID);
        add(AcsConstants.FIELD_NAME_ORG_ID);
        add(ServicePlan.FIELD_NAME_SUBSCRIBER_ID);
    }};

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
        ServerPlanReqTracker reqTracker = nbiRequest.getServiceData();
        reqTracker.oldRecord = oldRecord;

        // Look for device id change (i.e. replacement)
        String oldDeviceIdString = oldRecord.getString(ServicePlan.FIELD_NAME_DEVICE_ID);
        if (!newRecord.getString(ServicePlan.FIELD_NAME_DEVICE_ID).equals(oldDeviceIdString)) {
            reqTracker.bDeviceIdChanged = true;
        }

        return true;
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
        /**
         * Kick off device op to apply service plan (run at background)
         */
        ServerPlanReqTracker reqTracker = nbiRequest.getServiceData();
        if (httpResponseStatus.equals(HttpResponseStatus.OK) && reqTracker.device != null){
            reqTracker.newRecordId = id;
            applyServicePlan(
                    log,
                    vertx.eventBus(),
                    mongoClient,
                    nbiRequest.body,
                    passiveWorkflowCache,
                    reqTracker.orgId,
                    reqTracker.device
            );
        }

        return super.postCreate(nbiRequest, httpResponseStatus, id, error);
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
        /**
         * Kick off device op to apply service plan (run at background)
         */
        final ServerPlanReqTracker reqTracker = nbiRequest.getServiceData();
        if (responseStatus.equals(HttpResponseStatus.OK)) {
            if (reqTracker.device != null) {
                /**
                 * Apply service to the device that is currently associated
                 */
                applyServicePlan(
                        log,
                        vertx.eventBus(),
                        mongoClient,
                        nbiRequest.body,
                        passiveWorkflowCache,
                        reqTracker.orgId,
                        reqTracker.device
                );
            }
        }

        return super.postUpdate(nbiRequest, responseStatus, error);
    }

    /**
     * Disable the old device when modifying existing service plan with a different device id.
     * @param nbiRequest
     * @param reqTracker
     */
    public void disableOldDevice(final AcsNbiRequest nbiRequest, final ServerPlanReqTracker reqTracker) {
        try {
            Handler deviceQueryResultHandler =

                            new Handler<JsonObject>() {
                                @Override
                                public void handle(JsonObject result) {
                                    if(result == null) {
                                        // No such device
                                        sendUpdateResponse(nbiRequest, HttpResponseStatus.OK, null);
                                    } else if(result.equals(VertxMongoUtils.FIND_ONE_TIMED_OUT)) {
                                        nbiRequest.sendResponse(
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                MONGODB_TIMED_OUT
                                        );
                                    } else {
                                        // Found the old device, disable all services on it
                                        log.info("Disabling services on the previous device "
                                                + result.getString(AcsConstants.FIELD_NAME_ID));
                                        applyServicePlan(
                                                log,
                                                vertx.eventBus(),
                                                mongoClient,
                                                ServicePlan.SERVICE_PLAN_ALL_DISABLED,
                                                passiveWorkflowCache,
                                                reqTracker.orgId,
                                                reqTracker.device
                                        );
                                    }
                                }
                            };

            VertxMongoUtils.findOne(
                    mongoClient,
                    Cpe.CPE_COLLECTION_NAME,
                    Subscriber.getDeviceMatcherByDeviceId(
                            reqTracker.orgId,
                            reqTracker.oldRecord.getString(ServicePlan.FIELD_NAME_DEVICE_ID)
                    ),
                    deviceQueryResultHandler,
                    DEVICE_QUERY_KEYS
            );
        } catch (VertxException e) {
            nbiRequest.sendResponse(
                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    MONGODB_TIMED_OUT
            );
        }
    }

    /**
     * Update the service-plan provisioning status of a device.
     *
     * @param mongoClient
     * @param cpeKey
     * @param status
     */
    public static void updateDeviceServicePlanStatus(
            final MongoClient mongoClient,
            final String cpeKey,
            final String status,
            final Handler<Long> handler) {
        try {
            VertxMongoUtils.update(
                    mongoClient,
                    Cpe.CPE_COLLECTION_NAME,
                    cpeKey,
                    VertxMongoUtils.getUpdatesObject(
                            VertxMongoUtils.addSet(null, Cpe.DB_FIELD_NAME_SERVICE_PLAN_STATUS, status),
                            null,
                            null,
                            null,
                            null
                    ),
                    handler
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Util method to apply the service plan to a device.
     *
     * @param eventBus
     * @param servicePlan
     * @param passiveWorkflowCache
     * @param cpe
     */
    public static void applyServicePlan(
            final Logger log,
            final EventBus eventBus,
            final MongoClient mongoClient,
            final JsonObject servicePlan,
            PassiveWorkflowCache passiveWorkflowCache,
            final String orgId,
            final JsonObject cpe) {
        final String cpeKey = cpe.getString(AcsConstants.FIELD_NAME_ID);
        JsonObject videoServiceProfile = null;
        JsonObject voiceServiceProfile = null;

        TreeMap perOrgPassiveWorkflows = passiveWorkflowCache.getPerOrgTreeMap(orgId);
        if (perOrgPassiveWorkflows != null) {
            /**
             * Look up all passive workflows in the cache to find the service profiles
             */
            for (Object aPojo : perOrgPassiveWorkflows.values()) {
                Workflow aWorkflow = (Workflow) aPojo;
                if (aWorkflow == null || !orgId.equals(aWorkflow.orgId)) {
                    // OrgId does not match
                    continue;
                }

                if (aWorkflow.cpeMatcher == null    // No matcher (internal error)
                        || aWorkflow.matchCpe(cpe) == false) {  // Does not match the workflow
                    continue;
                }

                // Found a matching workflow
                // Now traverse all workflow actions in this workflow
                for (WorkflowAction anAction : aWorkflow.actionChain) {
                    if (anAction.services != null) {
                        for (int i = 0; i < anAction.services.size(); i++) {
                            JsonObject aService = anAction.services.getJsonObject(i);
                            switch (aService.getString(ConfigurationCategory.PARAM_NAME_SERVICE_NAME)) {
                                case ConfigurationCategory.VIDEO_SERVICE:
                                    videoServiceProfile = aService;
                                    break;

                                case ConfigurationCategory.VOICE_SERVICE:
                                    voiceServiceProfile = aService;
                                    break;

                                default:
                                    log.error("Unknown Service Name: "
                                            + aService.getString(ConfigurationCategory.PARAM_NAME_SERVICE_NAME) + "!");
                                    break;
                            }
                        }
                    }
                }

                if (videoServiceProfile != null && voiceServiceProfile != null) {
                    break;
                }
            }
        }

        if (videoServiceProfile == null && voiceServiceProfile == null) {
            log.info(cpeKey + ": Unable to find any matching discovery workflow that applies service profiles.");
            return;
        }

        /**
         * Send DeviceOp
         */
        JsonArray serviceArray = new JsonArray();
        if (videoServiceProfile != null) {
            serviceArray.add(videoServiceProfile);
        }
        if (voiceServiceProfile != null) {
            serviceArray.add(voiceServiceProfile);
        }
        JsonObject requestBody = new JsonObject()
                .put(CpeDeviceOp.FIELD_NAME_CPE_DB_OBJECT, cpe)
                .put(CpeDeviceOp.FIELD_NAME_SERVICES, serviceArray)
                .put(CpeDeviceOp.FIELD_NAME_SERVICE_PLAN, servicePlan)
                .put(CpeDeviceOp.FIELD_NAME_OPERATION, CpeDeviceOpTypeEnum.SetParameterValues.name());
        log.debug("Applying service(s) to " + cpeKey  + "...");
        AcsApiUtils.sendApiRequest(
                eventBus,
                AcsConstants.ACS_API_SERVICE_DEVICE_OP,
                orgId,
                AcsApiCrudTypeEnum.Create,
                requestBody,
                ExecPolicy.DEFAULT_DEVICE_OP_TIMEOUT + 10000,    // wait 10 extra seconds to get meaningful error messages
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        String error = null;
                        if (asyncResult.succeeded()) {
                            JsonObject deviceOpResult = asyncResult.result().body();
                            String deviceOpStatusCode =
                                    deviceOpResult == null?
                                            HttpResponseStatus.INTERNAL_SERVER_ERROR.toString()
                                            :
                                            deviceOpResult.getString(
                                                    AcsConstants.FIELD_NAME_STATUS_CODE,
                                                    HttpResponseStatus.INTERNAL_SERVER_ERROR.toString()
                                            );
                            if (HttpResponseStatus.OK.toString().equals(deviceOpStatusCode)) {
                                /**
                                 * Device Op Succeeded
                                 */
                            } else {
                                /**
                                 * Device Op Failed
                                 */
                                error = " due to Internal server error";
                                if (deviceOpResult.containsKey(AcsConstants.FIELD_NAME_ERROR)) {
                                    error += " (" + deviceOpResult.getString(AcsConstants.FIELD_NAME_ERROR) + ")";
                                }
                            }
                        } else {
                            /**
                             * Async Result Failed
                             */
                            error = asyncResult == null? "" : (" due to " + asyncResult.cause().getMessage());
                        }

                        if (error != null) {
                            log.error(cpeKey + "Failed to apply service(s)" + error);
                            updateDeviceServicePlanStatus(
                                    mongoClient,
                                    cpeKey,
                                    "Failed" + error,
                                    null
                            );
                        } else {
                            updateDeviceServicePlanStatus(
                                    mongoClient,
                                    cpeKey,
                                    "Succeeded",
                                    null
                            );
                        }

                        /**
                         * Save Event
                         */
                        Event.saveEvent(
                                mongoClient,
                                orgId,
                                cpe.getString(Cpe.DB_FIELD_NAME_SN),
                                error == null?
                                        EventTypeEnum.ApplyServicePlan
                                        :
                                        EventTypeEnum.ApplyServicePlanFailure,
                                EventSourceEnum.System,
                                error == null?
                                        null
                                        :
                                        new JsonObject().put("cause", error)
                        );
                    }
                }
        );
    }

    /**
     * Attach an existing service plan to a new device id without applying the service plan (DB change only).
     *
     * @param log
     * @param mongoClient
     * @param servicePlanId
     * @param deviceId
     */
    public static void attachServicePlanToDevice(
            final Logger log,
            final MongoClient mongoClient,
            final String servicePlanId,
            final String deviceId) {
        log.info("Attaching Service Plan " + servicePlanId + " to " + deviceId + "...");
        try {
            VertxMongoUtils.update(
                    mongoClient,
                    ServicePlan.DB_COLLECTION_NAME,
                    servicePlanId,
                    VertxMongoUtils.getUpdatesObject(
                            new JsonObject().put(
                                    ServicePlan.FIELD_NAME_DEVICE_ID,
                                    deviceId
                            ),
                            null,
                            null,
                            null,
                            null
                    ),
                    null
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
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
                details.containsKey(AcsConstants.FIELD_NAME_ERROR)) {
            String error = details.getString(AcsConstants.FIELD_NAME_ERROR);

            if (error.contains(ServicePlan.FIELD_NAME_DEVICE_ID)) {
                // Convert "customId" to "account number" for internal requests
                return CANNOT_ASSIGN_MULTIPLE_SERVICE_PLANS;
            }
        }

        // Return the error details returned from super
        return details;
    }
}
