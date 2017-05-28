package vertx.acs.worker.workflow;

import vertx.VertxException;
import vertx.VertxConstants;
import vertx.VertxMongoUtils;
import vertx.acs.cache.PassiveWorkflowCache;
import vertx.acs.utils.ReplacementUtils;
import vertx.model.*;
import vertx.util.*;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.platform.Verticle;

import java.util.TreeMap;

/**
 * Project:  cwmp
 *
 * Passive Workflow Worker Vertice.
 *
 * @author: ronyang
 */
public class PassiveWorkflowWorkerVertice extends Verticle{
    private Logger log = LoggerFactory.getLogger(this.getClass());

    // Async Redis Client Instance
    public RedisClient redisClient;

    // A Local Cache of all passive workflows
    PassiveWorkflowCache passiveWorkflowCache;

    // # of Current Outstanding Discovery Sessions
    int outstandingDiscoverSessions = 0;

    // Constants
    public static final int DEEP_DISCOVERY_TIMEOUT = 120000;    // 2 minutes
    public static final String FIELD_WORKFLOW_ID = "workflowId";
    public static final String FIELD_SKIP_WORKFLOWS = "skipWorkflows";

    /**
     * TR098 Object Path for Deep Discovery
     */
    private static final JsonArray DEEP_DISCOVER_PARAM_NAMES =
            new JsonArray().add(Cpe.INTERNET_GATEWAY_DEVICE_ROOT + ".");

    /**
     * Query Key when checking for replacements
     */
    public static final JsonObject DEVICE_REPLACEMENT_QUERY_KEY = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ID, 1)
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1)
            .putNumber(Cpe.DB_FIELD_NAME_REGISTRATION_ID, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_MODEL_NAME, 1)
            .putNumber(Cpe.DB_FIELD_NAME_TO_BE_REPLACED_BY, 1);

    /**
     * Start the worker
     */
    public void start() {
        /**
         * Initialize Redis Client
         */
        redisClient = new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);

        /**
         * Initialize Local passive workflow cache
         */
        passiveWorkflowCache = new PassiveWorkflowCache(
                vertx,
                AcsConstants.VERTX_ADDRESS_WORKFLOW_CRUD,
                Workflow.DB_COLLECTION_NAME,
                "passive-workflow"
        );

        /**
         * Start a 1-second timer to read the CPE-discovery queue from Redis
         */
        vertx.setPeriodic(
                1000,
                new Handler<Long>() {
                    @Override
                    public void handle(Long event) {
                        readFromQueue();
                    }
                }
        );
    }

    /**
     * Read the next CPE (if any) from Discovery Queue
     */
    public void readFromQueue() {
        if (outstandingDiscoverSessions >= AcsConfigProperties.MAX_DISCOVERY_SESSIONS_PER_VERTICE) {
            // Do not exceed the cap
            log.debug("Current # of outstanding discovery session(s): " + outstandingDiscoverSessions);
            return;
        }

        // Read from Redis
        CpeDiscoveryUtils.readFromQueue(redisClient, newCpeArrivalEventHandler);
    }

    /**
     * Handler to process read results of the CPE-discovery queue from Redis
     */
    Handler<String> newCpeArrivalEventHandler = new Handler<String>() {
        @Override
        public void handle(String rawString) {
            if (rawString == null) {
                return;
            }

            try {
                final JsonObject cpe = new JsonObject(rawString);
                final String orgId = cpe.getString(AcsConstants.FIELD_NAME_ORG_ID);
                final String cpeKey = cpe.getString(AcsConstants.FIELD_NAME_ID);
                final String sn = cpe.getString(Cpe.DB_FIELD_NAME_SN);
                final String regId = cpe.getString(Cpe.DB_FIELD_NAME_REGISTRATION_ID);

                /**
                 * Increase the # of outstanding sessions and try to read one more
                 */
                outstandingDiscoverSessions ++;
                readFromQueue();

                if (cpe.getBoolean("newDiscovery")) {
                    log.info("Received a new discover event for " + cpeKey);

                    /**
                     * Check For Replacements by querying the device collection
                     */
                    JsonObject deviceMatcher = new JsonObject()
                            .putString(AcsConstants.FIELD_NAME_ORG_ID, orgId)
                            .putObject(
                                    Cpe.DB_FIELD_NAME_SN,
                                    new JsonObject().putString(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_NOT_EQUAL, sn)
                            );
                    if (regId != null && regId.length() > 0) {
                        deviceMatcher.putArray(
                                VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR,
                                new JsonArray()
                                        .add(new JsonObject().putString(Cpe.DB_FIELD_NAME_TO_BE_REPLACED_BY, sn))
                                        .add(new JsonObject().putString(Cpe.DB_FIELD_NAME_REGISTRATION_ID, regId))
                        );
                    } else {
                        deviceMatcher.putString(Cpe.DB_FIELD_NAME_TO_BE_REPLACED_BY, sn);
                    }
                    VertxMongoUtils.findOne(
                            vertx.eventBus(),
                            Cpe.CPE_COLLECTION_NAME,
                            deviceMatcher,
                            new VertxMongoUtils.FindOneHandler(
                                    new Handler<JsonObject>() {
                                        @Override
                                        public void handle(JsonObject oldDevice) {
                                            if (VertxMongoUtils.FIND_ONE_TIMED_OUT.equals(oldDevice)
                                                    || oldDevice == null) {
                                                // Not replacing any existing device
                                                traverseAllWorkflows(orgId, cpeKey, cpe, null);
                                            } else {
                                                outstandingDiscoverSessions --;

                                                /**
                                                 * Decommission old device if old device and new device have the
                                                 * same regId.
                                                 */
                                                if (regId != null &&
                                                        regId.equals(
                                                                oldDevice.getString(Cpe.DB_FIELD_NAME_REGISTRATION_ID))
                                                        ) {
                                                    /**
                                                     * Save Decommissioned Event
                                                     */
                                                    Event.saveEvent(
                                                            vertx.eventBus(),
                                                            orgId,
                                                            oldDevice.getString(Cpe.DB_FIELD_NAME_SN),
                                                            EventTypeEnum.Decommissioned,
                                                            EventSourceEnum.System,
                                                            new JsonObject().putString(
                                                                    "replaced by",
                                                                    Cpe.getSnByCpeKey(cpeKey)
                                                            )
                                                    );

                                                    /**
                                                     * Mark old device as "Decommissioned" after querying the
                                                     * subscriber record.
                                                     */
                                                    markDeviceDecommissioned(orgId, regId, oldDevice);
                                                }

                                                String newDeviceModel = cpe.getString(CpeDeviceType.FIELD_NAME_MODEL_NAME);
                                                String oldDeviceModel = oldDevice.getString(CpeDeviceType.FIELD_NAME_MODEL_NAME);
                                                if (newDeviceModel.equals(oldDeviceModel)) {
                                                    // Replacing an existing device
                                                    ReplacementUtils.doReplacement(
                                                            vertx.eventBus(),
                                                            redisClient,
                                                            passiveWorkflowCache.getPerOrgTreeMap(orgId),
                                                            cpe,
                                                            orgId,
                                                            cpeKey,
                                                            oldDevice.getString(AcsConstants.FIELD_NAME_ID),
                                                            oldDevice.containsField(Cpe.DB_FIELD_NAME_TO_BE_REPLACED_BY)
                                                    );
                                                } else {
                                                    String error = "Model mismatch. (replacing an " + oldDeviceModel
                                                            + " with an " + newDeviceModel + ")";
                                                    log.error("Replacement Failed due to " + error);

                                                    /**
                                                     * Save Replacement Failure Event
                                                     */
                                                    Event.saveEvent(
                                                            vertx.eventBus(),
                                                            orgId,
                                                            Cpe.getSnByCpeKey(cpeKey),
                                                            EventTypeEnum.ReplacementFailure,
                                                            EventSourceEnum.System,
                                                            new JsonObject()
                                                                    .putString(
                                                                            "old device",
                                                                            oldDevice.getString(Cpe.DB_FIELD_NAME_SN)
                                                                    )
                                                                    .putString("cause", error)
                                                    );
                                                }
                                            }
                                        }
                                    }
                            ),
                            DEVICE_REPLACEMENT_QUERY_KEY
                    );
                } else {
                    /**
                     * Not a new discovery event (i.e. received bootstrap from an existing device.
                     *
                     * Do not check for replacement in this case
                     */
                    log.info("Received a re-discover event for " + cpeKey);
                    traverseAllWorkflows(orgId, cpeKey, cpe, null);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                log.error("Caught Exception while processing CPE-discovery event! (" + rawString + ")");
            }
        }
    };

    /**
     * Mark old device as "Decommissioned" after querying the
     * subscriber record.
     *
     * @param orgId
     * @param regId
     * @param device
     */
    public void markDeviceDecommissioned(
            String orgId,
            String regId,
            final JsonObject device) {
        try {
            VertxMongoUtils.findOne(
                    vertx.eventBus(),
                    Subscriber.DB_COLLECTION_NAME,
                    Subscriber.getDeviceMatcherByDeviceIdArray(orgId, new JsonArray().add(regId)),
                    new VertxMongoUtils.FindOneHandler(
                            new Handler<JsonObject>() {
                                @Override
                                public void handle(JsonObject subscriber) {
                                    JsonObject sets = VertxMongoUtils.addSet(
                                            null,
                                            Cpe.DB_FIELD_NAME_DECOMMISSIONED,
                                            true
                                    );
                                    if (subscriber != null && !subscriber.equals(VertxMongoUtils.FIND_ONE_TIMED_OUT)) {
                                        VertxMongoUtils.addSet(
                                                sets,
                                                Cpe.DB_FIELD_NAME_PREV_SUBSCRIBER,
                                                subscriber.getString(AcsConstants.FIELD_NAME_NAME)
                                        );
                                    }
                                    log.info("Marking " + device.getString(AcsConstants.FIELD_NAME_ID)
                                            + " as decommissioned. Previous Subscriber: "
                                            + sets.getString(Cpe.DB_FIELD_NAME_PREV_SUBSCRIBER));

                                    try {
                                        VertxMongoUtils.update(
                                                vertx.eventBus(),
                                                Cpe.CPE_COLLECTION_NAME,
                                                device.getString(AcsConstants.FIELD_NAME_ID),
                                                VertxMongoUtils.getUpdatesObject(
                                                        sets,
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
                            }
                    ),
                    null
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Traverse all passive workflows and execute all matching workflows
     *
     * @param orgId
     * @param cpeKey
     * @param cpe
     * @param skip A JSON Array that contains all workflows that have already been executed for this CPE
     */
    public void traverseAllWorkflows(final String orgId, final String cpeKey, final JsonObject cpe, JsonArray skip) {
        TreeMap perOrgTreeMap = passiveWorkflowCache.getPerOrgTreeMap(orgId);
        if (perOrgTreeMap != null) {
            for (Object aPojo : perOrgTreeMap.values()) {
                Workflow aWorkflow = (Workflow) aPojo;
                if (aWorkflow == null) {
                    continue;
                }

                if (skip != null && skip.contains(aWorkflow.id)) {
                    // Already executed this workflow for this CPE
                    continue;
                }

                log.debug(cpeKey + ": Checking workflow " + aWorkflow.id);
                if (!Workflow.STATE_IN_PROGRESS.equals(aWorkflow.state) // Workflow has been suspended
                        || aWorkflow.cpeMatcher == null    // No matcher (internal error)
                        || aWorkflow.matchCpe(cpe) == false  // Does not match the workflow
                        ) {
                    continue;
                }

                // Found a match
                new WorkflowCpeTracker(
                        vertx,
                        cpe.putString(FIELD_WORKFLOW_ID, aWorkflow.id)
                                .putArray(FIELD_SKIP_WORKFLOWS, skip == null ? new JsonArray() : skip.add(aWorkflow.id)),
                        aWorkflow,
                        cpeExecResultHandler
                );

                return;
            }
        }

        // No more matching workflow
        if (AcsConfigProperties.PERFORM_DEEP_DISCOVERY) {
            /**
             * Run Deep Discovery
             */
            // Convert action into API request
            JsonObject requestBody = new JsonObject().putObject(CpeDeviceOp.FIELD_NAME_CPE_DB_OBJECT, cpe);
            requestBody.putArray(CpeDeviceOp.FIELD_NAME_PARAM_NAMES, DEEP_DISCOVER_PARAM_NAMES);
            requestBody.putObject(CpeDeviceOp.FIELD_NAME_GET_OPTIONS, WorkflowAction.GET_LIVE_DATA);
            requestBody.putString(CpeDeviceOp.FIELD_NAME_OPERATION,
                    CpeDeviceOpTypeEnum.GetParameterValues.name());

            AcsApiUtils.sendApiRequest(
                    vertx.eventBus(),
                    AcsConstants.ACS_API_SERVICE_DEVICE_OP,
                    orgId,
                    AcsApiCrudTypeEnum.Create,
                    requestBody,
                    DEEP_DISCOVERY_TIMEOUT,
                    new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                            if (asyncResult.succeeded()) {
                                /**
                                 * Deep Discovery succeeded.
                                 *
                                 * Check to see if this triggers any workflow.
                                 */
                                log.info("Deep discovery for CPE " + cpeKey + " succeeded.");
                            } else {
                                /**
                                 * In case of failure, simply delete this CPE from DB so it will be
                                 * re-discovered on the next inform.
                                 */
                                log.error("Failed to perform deep discovery for CPE " + cpeKey + "!");
                                try {
                                    VertxMongoUtils.delete(
                                            vertx.eventBus(),
                                            Cpe.CPE_COLLECTION_NAME,
                                            cpeKey,
                                            null
                                    );
                                } catch (VertxException e) {
                                    e.printStackTrace();
                                }
                            }

                            outstandingDiscoverSessions--;
                        }
                    }
            );
        } else {
            outstandingDiscoverSessions--;
        }
    }

    /**
     * Result Handler after the executing this workflow against a single CPE
     */
    public Handler<JsonObject> cpeExecResultHandler = new Handler<JsonObject>() {
        @Override
        public void handle(JsonObject cpeExecResult) {
            // Try to execute the next workflow
            traverseAllWorkflows(
                    cpeExecResult.getString(AcsConstants.FIELD_NAME_ORG_ID),
                    cpeExecResult.getString(AcsConstants.FIELD_NAME_ID),
                    cpeExecResult,
                    cpeExecResult.getArray(FIELD_SKIP_WORKFLOWS).add(cpeExecResult.getString(FIELD_WORKFLOW_ID))
            );
        }
    };

}
