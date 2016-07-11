package vertx2.acs.nbi.workflow;

import vertx2.VertxException;
import vertx2.VertxJsonUtils;
import vertx2.VertxMongoUtils;
import vertx2.acs.nbi.AbstractAcNbiCrudService;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.acs.worker.workflow.WorkflowCpeTracker;
import vertx2.model.AcsApiCrudTypeEnum;
import vertx2.model.Cpe;
import vertx2.model.CpeDeviceType;
import vertx2.model.Workflow;
import vertx2.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;

/**
 * Project:  cwmp ACS API
 *
 * Workflow Execution Logs Web Service Implementation.
 *
 * @author: ronyang
 */
public class WorkflowExecLogsService extends AbstractAcNbiCrudService {
    /**
     * Constants
     */
    public static final String FIELD_NAME_WORKFLOW_ID = "workflowId";
    public static final VertxException CUD_NOT_ALLOWED =
            new VertxException("Create/Update/Delete Operations are not allowed!");
    public static final VertxException INVALID_STATE =
            new VertxException("Invalid Workflow Exec Log State! (must be \"Failed\" or \"In Progress\" or \"Succeeded\")");

    /**
     * Allowed Query Parameter Name/Type Pairs
     */
    private static final HashMap<String, VertxJsonUtils.JsonFieldType> QUERY_PARAMETER_NAME_TYPE_PAIRS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(WorkflowCpeTracker.FIELD_NAME_STATE, VertxJsonUtils.JsonFieldType.String);

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_WORKFLOW_EXEC_LOG;
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
     * All Device Type Attributes are index fields.
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
        return null;
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
    public static final VertxJsonUtils.JsonFieldValidator MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_WORKFLOW_ID, VertxJsonUtils.JsonFieldType.String);
    public static final VertxJsonUtils.JsonFieldValidator OPTIONAL_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(WorkflowCpeTracker.FIELD_NAME_STATE, VertxJsonUtils.JsonFieldType.String);
    public boolean validate(final AcsNbiRequest nbiRequest, final AcsApiCrudTypeEnum crudType) throws VertxException {
        switch (crudType) {
            case Retrieve:
                VertxJsonUtils.validateFields(nbiRequest.body, MANDATORY_FIELDS, OPTIONAL_FIELDS);

                /**
                 * Let us retrieve the workflow (by id) first
                 */
                final String workflowId = nbiRequest.body.getString(FIELD_NAME_WORKFLOW_ID);
                try {
                    VertxMongoUtils.findOne(
                            vertx.eventBus(),
                            Workflow.DB_COLLECTION_NAME,
                            new JsonObject().putString(AcsConstants.FIELD_NAME_ID, workflowId),
                            new VertxMongoUtils.FindOneHandler(
                                    new Handler<JsonObject>() {
                                        @Override
                                        public void handle(JsonObject result) {
                                            if (result == null) {
                                                log.error("Unable to find workflow " + workflowId + "!");
                                                nbiRequest.sendResponse(HttpResponseStatus.NOT_FOUND);
                                            } else if (result.equals(VertxMongoUtils.FIND_ONE_TIMED_OUT)) {
                                                nbiRequest.sendResponse(
                                                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                        MONGODB_TIMED_OUT
                                                );
                                            } else {
                                                /**
                                                 * Found it.
                                                 *
                                                 * Save the Workflow POJO (after conversion)
                                                 */
                                                try {
                                                    Workflow workflow = Workflow.validateJsonObject(result);

                                                    nbiRequest.serviceData = workflow;
                                                    // Continue
                                                    postValidation(nbiRequest, crudType);
                                                } catch (VertxException e) {
                                                    e.printStackTrace();
                                                    log.error("Failed to convert workflow JSON Object to POJO!\n"
                                                            + result);
                                                    nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                            getServerInternalErrorWithDetails());
                                                }
                                            }
                                        }
                                    }
                            ),
                            null
                    );
                } catch (Exception ex) {
                    // This should never happen
                    ex.printStackTrace();
                    nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            getServerInternalErrorWithDetails());
                }
                return VALIDATION_PENDING_OR_FAILED;

            default:
                throw CUD_NOT_ALLOWED;
        }
    }

    /**
     * Build MongoDB Query Keys for Retrieve.
     *
     * Default to null (return everything)
     */
    private static final JsonObject QUERY_KEY = new JsonObject()
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1)
            .putNumber(Cpe.DB_FIELD_NAME_IP_ADDRESS, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_MODEL_NAME, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_SW_VER, 1);
    public JsonObject buildRetrieveQueryKeys(AcsNbiRequest nbiRequest) {
        return QUERY_KEY.copy()
                .putNumber(
                        Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "." + nbiRequest.body.getString(FIELD_NAME_WORKFLOW_ID),
                        1
                );
    }

    /**
     * Build MongoDB Matcher for Retrieve
     */
    public JsonObject buildRetrieveMatcher(AcsNbiRequest nbiRequest) throws VertxException{
        /**
         * Get the workflow POJO
         */
        Workflow workflow = (Workflow)nbiRequest.serviceData;

        // Get the state filter if any
        String stateFilter = nbiRequest.body.getString(WorkflowCpeTracker.FIELD_NAME_STATE);

        JsonObject matcher;

        if (stateFilter == null) {
            /**
             * Retrieve all records
             */
            if (workflow.isActive()) {
                if (Workflow.STATE_COMPLETED.equals(workflow.state)) {
                    // Completed Active Workflow
                    matcher = new JsonObject()
                            .putObject(
                                    Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "." + nbiRequest.body.getString(FIELD_NAME_WORKFLOW_ID),
                                    VertxMongoUtils.EXISTS
                            );

                } else {
                    // Incomplete Active Workflow
                    matcher = workflow.getMatcher(null, false);
                }
            } else {
                // Passive workflow
                JsonObject started = new JsonObject()
                                .putObject(
                                        Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "." + nbiRequest.body.getString(FIELD_NAME_WORKFLOW_ID),
                                        VertxMongoUtils.EXISTS
                                );
                JsonObject pending = workflow.getMatcher(null, true);

                // Query with $or to include all pending and started devices.
                matcher = new JsonObject().putArray(
                        VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR,
                        new JsonArray()
                                .add(started)
                                .add(pending)
                );
            }
        } else {
            /**
             * Filter by execution state
             */
            if (WorkflowCpeTracker.STATE_PENDING.equals(stateFilter)) {
                matcher = workflow.getMatcher(null, true);
            } else {
                matcher = new JsonObject()
                        .putObject(
                                Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "." + nbiRequest.body.getString(FIELD_NAME_WORKFLOW_ID),
                                VertxMongoUtils.EXISTS
                        );

                // Also allow filtering by state
                if (stateFilter != null) {
                    String internalStateString = null;
                    if (stateFilter.equals(WorkflowCpeTracker.STATE_FAILED.substring(3))) {
                        internalStateString = WorkflowCpeTracker.STATE_FAILED;
                    } else if (stateFilter.equals(WorkflowCpeTracker.STATE_IN_PROGRESS.substring(3))) {
                        internalStateString = WorkflowCpeTracker.STATE_IN_PROGRESS;
                    } else if (stateFilter.equals(WorkflowCpeTracker.STATE_SUCCEEDED.substring(3))) {
                        internalStateString = WorkflowCpeTracker.STATE_SUCCEEDED;
                    } else if (stateFilter.equals(WorkflowCpeTracker.STATE_PENDING)) {
                        internalStateString = WorkflowCpeTracker.STATE_PENDING;
                    }

                    if (internalStateString != null) {
                        matcher.putString(
                                Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "."
                                        + nbiRequest.body.getString(FIELD_NAME_WORKFLOW_ID) + "."
                                        + WorkflowCpeTracker.FIELD_NAME_STATE,
                                internalStateString
                        );
                    } else {
                        log.error("Invalid Workflow Exec Log State " + stateFilter + "!");
                        throw INVALID_STATE;
                    }
                }
            }
        }

        return matcher;
    }


    /**
     * For bulk query, get the "sort" JSON Object on how to sort the results.
     *
     * @param nbiRequest
     * @return  Default to null (let MongoDB to sort it)
     */
    public JsonObject getDefaultQuerySort(AcsNbiRequest nbiRequest) {
        return new JsonObject().putNumber(
                Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "."
                        + nbiRequest.body.getString(FIELD_NAME_WORKFLOW_ID) + "."
                        + WorkflowCpeTracker.FIELD_NAME_STATE,
                -1
        );
    }

    /**
     * Get all the supported query parameter name/type pairs.
     *
     * @param crudType
     */
    @Override
    public HashMap<String, VertxJsonUtils.JsonFieldType> getQueryParameterNameTypePairs(AcsApiCrudTypeEnum crudType) {
        return QUERY_PARAMETER_NAME_TYPE_PAIRS;
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
        if (queryResults != null && queryResults.size() > 0) {
            String workflowId = nbiRequest.body.getString(FIELD_NAME_WORKFLOW_ID);

            JsonArray newResults = new JsonArray();
            for (int i=0; i < queryResults.size(); i ++) {
                // Convert the timestamp JSON Object to string
                JsonObject aRecord = queryResults.get(i);

                // No need to include ID
                aRecord.removeField(AcsConstants.FIELD_NAME_ID);

                // Pull the exec logs up to top level
                JsonObject execLog = aRecord.getObject(Cpe.DB_FIELD_NAME_WORKFLOW_EXEC);
                if (execLog != null) {
                    execLog = execLog.getObject(workflowId);
                    if (execLog == null) {
                        aRecord.putString(WorkflowCpeTracker.FIELD_NAME_STATE, WorkflowCpeTracker.STATE_PENDING);
                    } else {
                        String state = execLog.getString(WorkflowCpeTracker.FIELD_NAME_STATE);
                        if (state != null) {
                            aRecord.putString(WorkflowCpeTracker.FIELD_NAME_STATE, state.substring(3));
                        } else {
                            aRecord.putString(WorkflowCpeTracker.FIELD_NAME_STATE, WorkflowCpeTracker.STATE_PENDING);
                        }

                        String failureSummary = execLog.getString(WorkflowCpeTracker.FIELD_NAME_FAILURE_SUMMARY);
                        if (failureSummary != null) {
                            aRecord.putString(WorkflowCpeTracker.FIELD_NAME_FAILURE_SUMMARY, failureSummary);
                        }

                        if (execLog.getObject(WorkflowCpeTracker.FIELD_NAME_START) != null) {
                            aRecord.putString(
                                    WorkflowCpeTracker.FIELD_NAME_START,
                                    execLog.getObject(WorkflowCpeTracker.FIELD_NAME_START)
                                            .getString(VertxMongoUtils.MOD_MONGO_DATE)
                            );
                        }

                        if (execLog.getObject(WorkflowCpeTracker.FIELD_NAME_END) != null) {
                            aRecord.putString(
                                    WorkflowCpeTracker.FIELD_NAME_END,
                                    execLog.getObject(WorkflowCpeTracker.FIELD_NAME_END)
                                            .getString(VertxMongoUtils.MOD_MONGO_DATE)
                            );
                        }
                    }
                } else {
                    aRecord.putString(WorkflowCpeTracker.FIELD_NAME_STATE, WorkflowCpeTracker.STATE_PENDING);
                }
                aRecord.removeField(Cpe.DB_FIELD_NAME_WORKFLOW_EXEC);
                newResults.add(aRecord);
            }

            // update the query results with converted results
            return newResults;
        } else {
            return super.postRetrieve(nbiRequest, queryResults, moreExists);
        }
    }
}
