package vertx.acs.worker.workflow;

import vertx.VertxException;
import vertx.VertxJsonUtils;
import vertx.VertxMongoUtils;
import vertx.model.*;
import vertx.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Project:  cwmp
 *
 * This class tracks the workflow progress for a single CPE device.
 *
 * @author: ronyang
 */
public class WorkflowCpeTracker {
    private static Logger log = LoggerFactory.getLogger(WorkflowCpeTracker.class.getName());

    /**
     * DB Field Names
     */
    public static final String FIELD_NAME_ACTION_RESULTS = "actionResults";
    public static final String FIELD_NAME_STATE = "state";
    public static final String FIELD_NAME_START = "start";
    public static final String FIELD_NAME_END = "end";
    public static final String FIELD_NAME_FAILURE_SUMMARY = "failureSummary";

    /**
     * States
     */
    public static final String STATE_FAILED = "3. Failed";
    public static final String STATE_IN_PROGRESS = "2. In Progress";
    public static final String STATE_SUCCEEDED = "1. Succeeded";
    public static final String STATE_PENDING = "Pending";

    /**
     * Misc.
     */
    public static final String FIELD_NAME_RACE_CONDITION = "raceCondition";

    /**
     * Internal Failure JSON Object
     */
    public static final JsonObject INTERNAL_FAILURE =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, "Internal Failure");
    public static final String EXPECTATION_FAILED =
            "The value(s) read from target device are different from the expected value(s)!";
    /**
     * Skip Status Code Field when comparing get results.
     */
    public static Collection<String> SKIP_STATUS_CODE = new ArrayList<String>() {{
        add(AcsConstants.FIELD_NAME_STATUS_CODE);
    }};

    // CPE DB JSON Object
    public JsonObject cpe;
    public String cpeId;

    // The action chain (a local copy for this CPE device)
    public LinkedList<WorkflowAction> actionChain;

    // Parent Workflow (read only)
    public Workflow workflow;
    public String cpeDbWorkflowExecPrefix;

    // Vert.x
    public Vertx vertx;

    // Async Handler to be called at the of the action chain, or on failures
    Handler<JsonObject> finalHandler;

    public int currentActionIndex = 0;

    public boolean inProgress = false;

    /**
     * Constructor.
     *
     * @param vertx
     * @param cpe
     * @param workflow
     * @param finalHandler
     */
    public WorkflowCpeTracker(
            Vertx vertx,
            final JsonObject cpe,
            final Workflow workflow,
            final Handler<JsonObject> finalHandler) {
        this.vertx = vertx;
        this.finalHandler = finalHandler;
        this.cpe = cpe;
        this.cpeId = cpe.getString(AcsConstants.FIELD_NAME_ID);
        this.workflow = workflow;

        if (Workflow.STATE_SUSPENDED.equals(workflow.state)) {
            log.info("Skip executing workflow " + workflow.id + " for CPE " + cpeId + " as workflow is suspended.");
            return;
        }

        /**
         * Create a local copy of the action chain
         */
        actionChain = new LinkedList<>();
        for (int i = 0; i < workflow.actionChain.size(); i++) {
            WorkflowAction anAction = workflow.actionChain.get(i).copy();
            actionChain.add(anAction);
            log.info("Adding a " + anAction.actionEnum.name() + " action.");
        }

        // CPE Matcher
        JsonObject matcher = new JsonObject()
                .put(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, cpeId)
                .put(Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "." + workflow.id, VertxMongoUtils.EXISTS_FALSE);

        // Update CPE DB as we start
        cpeDbWorkflowExecPrefix = Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "." + workflow.id + ".";
        try {
            VertxMongoUtils.updateWithMatcher(
                    vertx.eventBus(),
                    Cpe.CPE_COLLECTION_NAME,
                    matcher,
                    VertxMongoUtils.getUpdatesObject(
                            // sets
                            VertxMongoUtils.addSet(
                                    null,
                                    cpeDbWorkflowExecPrefix + FIELD_NAME_STATE,
                                    STATE_IN_PROGRESS
                            ),
                            // unsets
                            null,
                            // timestamps
                            VertxMongoUtils.addTimeStamp(
                                    null,
                                    cpeDbWorkflowExecPrefix + FIELD_NAME_START),
                            // pulls
                            null,
                            // pushes
                            null
                    ),
                    new Handler<Long> () {
                        @Override
                        public void handle(Long result) {
                            if (result == null) {
                                log.error("Failed to add workflow log into CPE record for " + cpeId + "!");
                                finalHandler.handle(
                                        cpe.putString(AcsConstants.FIELD_NAME_STATUS_CODE,
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR.toString()
                                        )
                                );
                            } else if (result != 1) {
                                // This is probably due to a race condition that the workflow has already been executed
                                // against this CPE
                                log.info("Workflow " + workflow.id + " has already been started for CPE " + cpeId);
                                finalHandler.handle(cpe.putBoolean(FIELD_NAME_RACE_CONDITION, true));
                            } else {
                                // Start execution
                                log.info("Start executing workflow " + workflow.id + " towards CPE " + cpeId);
                                inProgress = true;
                                doFirstAction();
                            }
                        }
                    }
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Execute the first action
     */
    public void doFirstAction() {
        // Start the execution immediately
        final WorkflowAction firstAction = actionChain.removeFirst();
        currentActionIndex ++;

        WorkflowAction.doAction(
                vertx,
                cpe,
                workflow.id,
                firstAction,
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        String statusCode = null;
                        String failureSummary = null;
                        JsonObject dbSets = null;
                        JsonObject timestamp = null;
                        JsonObject actionResult = new JsonObject()
                                .putString(WorkflowAction.FIELD_NAME_ACTION_TYPE, firstAction.actionEnum.typeString);

                        if (WorkflowActionEnum.DELAY.equals(firstAction.actionEnum)) {
                            // Delay timer is up
                            statusCode = HttpResponseStatus.OK.toString();
                        } else {
                            if (asyncResult.succeeded()) {
                                // received reply from API server
                                JsonObject result = asyncResult.result().body();
                                log.info("AsyncResult for CPE " + cpeId + " Action " + firstAction.actionEnum.name()
                                        + "\n" + result.encodePrettily());
                                statusCode = result.getString(AcsConstants.FIELD_NAME_STATUS_CODE);

                                if (HttpResponseStatus.OK.equals(statusCode)) {
                                    /**
                                     * Further Check the result if action is "Get Parameter Values"
                                     */
                                    if (firstAction.actionEnum.equals(WorkflowActionEnum.GET_PARAMETER_VALUES)) {
                                        result.removeField(AcsConstants.FIELD_NAME_STATUS_CODE);
                                        if (!VertxJsonUtils.compare(
                                                result,
                                                firstAction.expectedParamValues,
                                                SKIP_STATUS_CODE)) {
                                            log.error(cpeId + ": Action "
                                                    + WorkflowActionEnum.GET_PARAMETER_VALUES.typeString
                                                    + " failed due to parameter value mismatch!\nExpecting:\n"
                                                    + firstAction.expectedParamValues.encodePrettily()
                                                    + "\nActual Results:\n" + result.encodePrettily());
                                            statusCode = HttpResponseStatus.EXPECTATION_FAILED.toString();
                                        }
                                        failureSummary = EXPECTATION_FAILED;
                                        result.putString(AcsConstants.FIELD_NAME_STATUS_CODE, statusCode);
                                    }
                                } else {
                                    failureSummary = result.getString(AcsConstants.FIELD_NAME_ERROR);
                                }

                                actionResult.putObject(AcsConstants.FIELD_NAME_RESULT, result);
                            } else {
                                log.info("AsyncResult for CPE " + cpeId + " Action " + firstAction.actionEnum.name()
                                        + ": Action failed due to " + asyncResult.cause() + "!");

                                statusCode = HttpResponseStatus.BAD_GATEWAY.toString();
                                failureSummary = INTERNAL_FAILURE.getString(AcsConstants.FIELD_NAME_ERROR);
                                actionResult.putObject(AcsConstants.FIELD_NAME_RESULT, INTERNAL_FAILURE);
                            }
                        }
                        dbSets = VertxMongoUtils.addSet(
                                dbSets,
                                cpeDbWorkflowExecPrefix + FIELD_NAME_ACTION_RESULTS + "." + currentActionIndex,
                                actionResult
                        );

                        /**
                         * Also Save Device Event
                         */
                        EventTypeEnum eventType = firstAction.actionEnum.eventType;
                        JsonObject eventSummary = null;
                        // Extract file type if any
                        AcsFileType fileType =
                                firstAction.file == null?
                                        AcsFileType.Unknown
                                        :
                                        AcsFileType.getAcsFileType(
                                                firstAction.file.getString(AcsFile.FIELD_NAME_TYPE)
                                        );
                        if (eventType != null) {
                            eventSummary = new JsonObject()
                                    .putString(
                                            "workflow",
                                            workflow.rawJsonObject.getString(AcsConstants.FIELD_NAME_NAME)
                                    );

                            switch (firstAction.actionEnum) {
                                case DOWNLOAD_CONFIG_FILE:
                                case DOWNLOAD_FW_IMAGE:
                                    // Extract/Add file name
                                    eventSummary.putString(
                                            fileType.typeString + " name",
                                            firstAction.file.getString(AcsConstants.FIELD_NAME_NAME)
                                    );

                                    // Extract/Add Version if any
                                    if (firstAction.file.containsField(AcsFile.FIELD_NAME_VERSION)) {
                                        eventSummary.putString(
                                                AcsFile.FIELD_NAME_VERSION,
                                                firstAction.file.getString(AcsFile.FIELD_NAME_VERSION)
                                        );
                                    }

                                    if (AcsFileType.SipConfigFile.equals(fileType)) {
                                        eventType = EventTypeEnum.SipConfigFileDownload;
                                    }
                                    break;

                                case APPLY_CONFIG_PROFILE:
                                    // Add profile name
                                    eventSummary.putString("profile name", firstAction.profileName);
                                    break;
                            }
                        }

                        if (!HttpResponseStatus.OK.toString().equals(statusCode)) {
                            dbSets = VertxMongoUtils.addSet(
                                    dbSets,
                                    cpeDbWorkflowExecPrefix + FIELD_NAME_STATE,
                                    STATE_FAILED
                            );

                            // Set Failure event type
                            eventType = firstAction.actionEnum.failureEventType;
                            if (AcsFileType.SipConfigFile.equals(fileType)) {
                                eventType = EventTypeEnum.SipConfigFileDownloadFailure;
                            }

                            if (failureSummary != null) {
                                if (eventSummary != null) {
                                    eventSummary.putString("failure", failureSummary);
                                }
                                dbSets = VertxMongoUtils.addSet(
                                        dbSets,
                                        cpeDbWorkflowExecPrefix + FIELD_NAME_FAILURE_SUMMARY,
                                        failureSummary
                                );
                            }
                            timestamp = VertxMongoUtils.addTimeStamp(null, cpeDbWorkflowExecPrefix + FIELD_NAME_END);
                        } else {
                            if (actionChain.size() ==0) {
                                dbSets = VertxMongoUtils.addSet(
                                        dbSets,
                                        cpeDbWorkflowExecPrefix + FIELD_NAME_STATE,
                                        STATE_SUCCEEDED
                                );
                                timestamp = VertxMongoUtils.addTimeStamp(null, cpeDbWorkflowExecPrefix + FIELD_NAME_END);
                            }
                        }

                        // Save Event
                        if (eventType != null) {
                            Event.saveEvent(
                                    vertx.eventBus(),
                                    workflow.orgId,
                                    cpe.getString(Cpe.DB_FIELD_NAME_SN),
                                    eventType,
                                    EventSourceEnum.System,
                                    eventSummary
                            );
                        }

                        // Update CPE DB
                        JsonObject dbUnsets = null;
                        if (EventTypeEnum.ConfigFileDownload.equals(eventType)) {
                            dbUnsets = VertxMongoUtils.addUnset(null, Cpe.DB_FIELD_NAME_PARAM_VALUES);
                            dbUnsets = VertxMongoUtils.addUnset(null, Cpe.DB_FIELD_NAME_PARAM_ATTRIBUTES);
                        }
                        try {
                            final String finalStatusCode = statusCode;
                            VertxMongoUtils.update(
                                    vertx.eventBus(),
                                    Cpe.CPE_COLLECTION_NAME,
                                    cpeId,
                                    VertxMongoUtils.getUpdatesObject(
                                            // sets
                                            dbSets,
                                            // unsets
                                            dbUnsets,
                                            // timestamps
                                            timestamp,
                                            // pulls
                                            null,
                                            // pushes
                                            null
                                    ),
                                    new Handler<Long> () {
                                        @Override
                                        public void handle(Long event) {
                                            if (!HttpResponseStatus.OK.toString().equals(finalStatusCode)) {
                                                // Failed, call the final handler with error status code
                                                finalHandler.handle(cpe.putString(AcsConstants.FIELD_NAME_STATUS_CODE, finalStatusCode));
                                            } else {
                                                if (actionChain.size() ==0) {
                                                    // We are done
                                                    finalHandler.handle(cpe.putString(AcsConstants.FIELD_NAME_STATUS_CODE, finalStatusCode));
                                                } else {
                                                    // kick off the next action if any
                                                    doFirstAction();
                                                }
                                            }
                                        }
                                    }
                            );
                        } catch (Exception e) {
                            log.error(cpeId + ": Failed to update CPE DB upon completion of action !"
                                    + firstAction.actionEnum.typeString);
                            e.printStackTrace();
                        }
                    }
                }
        );
    }
}
