package vertx2.model;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.VertxMongoUtils;
import vertx2.CcException;
import vertx2.cache.ConfigurationProfileCache;
import vertx2.cache.GroupCache;
import vertx2.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.LinkedList;

/**
 * Project:  SXA-CC
 *
 * Workflow Struct Model Definition.
 *
 * See http://wiki.calix.local/display/Compass/CCNG+-+ACS+API#CCNG-ACSAPI-ACSAPI-"WorkFlow"Struct for
 * more details.
 *
 * @author: ronyang
 */
public class Workflow {
    private static final Logger log = LoggerFactory.getLogger(Workflow.class.getName());

    /**
     * MongoDB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "sxacc-workflows";

    /**
     * Field Names
     */
    public static final String FIELD_NAME_ACTIONS = "actions";
    public static final String FIELD_NAME_INITIAL_TRIGGER = "initialTrigger";
    public static final String FIELD_NAME_EXEC_POLICY = "execPolicy";
    public static final String FIELD_NAME_GROUPS = "groups";
    public static final String FIELD_NAME_CPES = "cpes";
    public static final String FIELD_NAME_CPE_MATCHER = "cpeMatcher";
    public static final String FIELD_NAME_EXECUTE_ONCE_PER_CPE = "executeOncePerCpe";
    public static final String FIELD_NAME_STATE = "state";
    public static final String FIELD_NAME_START = "start";
    public static final String FIELD_NAME_END = "end";
    public static final String FIELD_NAME_TOTAL_COUNT = "totalCount";
    public static final String FIELD_NAME_SUCCESS_COUNT = "successCount";
    public static final String FIELD_NAME_FAILURE_COUNT = "failureCount";
    public static final String FIELD_NAME_IN_PROGRESS_COUNT = "inProgressCount";
    public static final String FIELD_NAME_PENDING_COUNT = "pendingCount";

    /**
     * States
     */
    public static final String STATE_SCHEDULED = "Scheduled";
    public static final String STATE_IN_PROGRESS = "In Progress";
    public static final String STATE_COMPLETED = "Completed";
    public static final String STATE_SUSPENDED = "Suspended";

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator mandatoryFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_EXEC_POLICY, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_ACTIONS, VertxJsonUtils.JsonFieldType.JsonArray);

    public static final VertxJsonUtils.JsonFieldValidator optionalFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_GROUPS, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_CPES, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_CPE_MATCHER, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_EXECUTE_ONCE_PER_CPE, VertxJsonUtils.JsonFieldType.Boolean)
                    .append(FIELD_NAME_STATE, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_START, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_END, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_TOTAL_COUNT, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_SUCCESS_COUNT, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_FAILURE_COUNT, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_IN_PROGRESS_COUNT, VertxJsonUtils.JsonFieldType.Integer)
                    .append(AcsConstants.FIELD_NAME_CREATE_TIME, VertxJsonUtils.JsonFieldType.DateTime);

    /**
     * Static Exceptions
     */
    public static final CcException MISSING_PARAM = new CcException("Missing Mandatory Parameter(s)!");
    public static final CcException INVALID_PARAM = new CcException("Invalid Parameter(s)!");

    /**
     * POJO Attributes
     */
    // Raw JSON Object
    public JsonObject  rawJsonObject;

    // Org Id
    public String orgId = null;

    // ID String
    public String id = null;

    // Action Chain (organized as a Linked List)
    public LinkedList<WorkflowAction> actionChain = new LinkedList<>();

    // CPE IDs
    public JsonArray cpeIdStrings = null;

    // CPE Group IDs
    public JsonArray groupIdStrings = null;

    // CPE Matcher
    public JsonObject cpeMatcher = null;

    // Exec Policy
    public ExecPolicy execPolicy = null;

    // State
    public String state = null;

    // Validation Pending Count
    public int validationPendingCount = 0;
    // Validation Response Sent or not
    public boolean bValidationResponseSent = false;

    /**
     * Default Empty Constructor.
     */
    public Workflow() {
    }

    /**
     * Constructor by a raw JSON Object.
     *
     * @param rawJsonObject
     */
    public Workflow(JsonObject rawJsonObject) throws CcException{
        this.rawJsonObject = rawJsonObject;
    }

    /**
     * Static Validation Method.
     *
     * @param jsonObject
     * @throws vertx2.CcException
     */
    public static Workflow validateJsonObject(JsonObject jsonObject) throws SxaVertxException {
        return validateJsonObject(jsonObject, null);
    }

    /**
     * Static Validation Method.
     *
     * @param jsonObject
     * @throws vertx2.CcException
     */
    public static Workflow validateJsonObject(
            JsonObject jsonObject,
            ConfigurationProfileCache configurationProfileCache
    ) throws SxaVertxException {
        /**
         * Call standard field validator util method.
         */
        VertxJsonUtils.validateFields(jsonObject, mandatoryFields, optionalFields);

        Workflow workflow = new Workflow(jsonObject);

        /**
         * Check id string
         */
        workflow.id = jsonObject.getString(AcsConstants.FIELD_NAME_ID);

        /**
         * Extract orgId
         */
        workflow.orgId = jsonObject.getString(AcsConstants.FIELD_NAME_ORG_ID);

        /**
         * Extract state
         */
        workflow.state = jsonObject.getString(FIELD_NAME_STATE);

        /**
         * Check Action Chain
         */
        JsonArray actionArray = jsonObject.getArray(FIELD_NAME_ACTIONS);
        if (actionArray.size() == 0) {
            log.error("No action specified!");
            throw INVALID_PARAM;
        }
        for (int i = 0; i < actionArray.size(); i ++) {
            WorkflowAction anAction = new WorkflowAction(actionArray.<JsonObject>get(i), configurationProfileCache);
            workflow.actionChain.add(anAction);
        }
        //log.debug("Action chain contains " + actionArray.size() + " action(s).");

        /**
         * Check Exec Policy
         */
        workflow.execPolicy = new ExecPolicy(jsonObject.getObject(FIELD_NAME_EXEC_POLICY));

        /**
         * Check Initial Trigger
         */
        if (workflow.execPolicy.initialTrigger == null) {
            log.error("Missing initial trigger!");
            throw INVALID_PARAM;
        }
        switch (workflow.execPolicy.initialTrigger.triggerType) {
            case MAINTENANCE_WINDOW:
                if (workflow.execPolicy.maintenanceWindow == null) {
                    log.error("The initial trigger is "
                            + workflow.execPolicy.initialTrigger.triggerType.typeString
                            + ", but no window specified in execPolicy!");
                    throw INVALID_PARAM;
                }
                break;
        }

        // Retrieve CPE Matcher if any
        if (jsonObject.containsField(FIELD_NAME_CPE_MATCHER)) {
            workflow.cpeMatcher = new JsonObject(jsonObject.getString(FIELD_NAME_CPE_MATCHER));
        }
        // Retrieve CPE ID Strings if any
        workflow.cpeIdStrings = jsonObject.getArray(FIELD_NAME_CPES);
        // Retrieve Group ID Strings if any
        workflow.groupIdStrings = jsonObject.getArray(FIELD_NAME_GROUPS);
        if (workflow.cpeIdStrings == null && workflow.groupIdStrings == null) {
            log.error("Both " + FIELD_NAME_CPES + " and " + FIELD_NAME_GROUPS + " are missing!");
            throw MISSING_PARAM;
        }

        return workflow;
    }

    /**
     * Check if the given workflow is an active workflow or passive.
     *
     * If the workflow can start immediately or wait for a maintenance window, it is an active workflow.
     * Otherwise it is passive.
     */
    public boolean isActive() {
        if (execPolicy.initialTrigger.triggerType.equals(WorkflowTrigger.TriggerTypeEnum.CPE_EVENT)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Get the initial delay in # of seconds.
     *
     * Applicable for active workflows only.
     *
     * @return
     */
    public long getInitialDelay() {
         switch (execPolicy.initialTrigger.triggerType) {
            case MAINTENANCE_WINDOW:
                return execPolicy.maintenanceWindow.timeTillNextOpen();

            case FIXED_DELAY:
                return execPolicy.initialDelay;

            default:
                log.error("Not an active workflow!");
                return 0;
        }
    }

    /**
     * Set ID String.
     *
     * @param id
     */
    public void setId(String id) {
        rawJsonObject.putString(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, id);
        this.id = id;
    }

    /**
     * Update workflow state.
     *
     * @param vertx
     * @param id
     * @param newState
     * @param customHandler
     */
    public static void changeState(
            Vertx vertx,
            String id,
            String newState,
            Handler<Long> customHandler) {
        try {
            VertxMongoUtils.update(
                    vertx.eventBus(),
                    Workflow.DB_COLLECTION_NAME,
                    id,
                    VertxMongoUtils.getUpdatesObject(
                            new JsonObject().putString(Workflow.FIELD_NAME_STATE, newState),
                            null,
                            null,
                            null,
                            null
                    ),
                    customHandler
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }

    }

    /**
     * Build a query matcher for a given workflow
     *
     * @param groupCache
     * @param bPendingOnly  Pending only (execution not yet started)
     */
    public JsonObject getMatcher(
            GroupCache groupCache,
            boolean bPendingOnly) {
        if (cpeMatcher == null) {
            // Build a matcher for all CPEs that this workflow will be executed against
            cpeMatcher = new JsonObject().putString(AcsConstants.FIELD_NAME_ORG_ID, orgId);
            JsonArray or = new JsonArray();
            if (cpeIdStrings != null && cpeIdStrings.size() > 0) {
                or.add(
                        new JsonObject().putObject(
                                AcsConstants.FIELD_NAME_ID,
                                new JsonObject().putArray(
                                        VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN,
                                        cpeIdStrings
                                )
                        )
                );
            }
            if (groupIdStrings != null) {
                for (int i = 0; i < groupIdStrings.size(); i++) {
                    CpeGroup group = groupCache.get((String)groupIdStrings.get(i));
                    if (group != null) {
                        or.add(group.cpeFilter);
                    } else {
                        log.error("Unable to find group " + groupIdStrings.get(i) + "!");
                    }
                }
            }

            if (or.size() == 0) {
                log.error("Cannot build matcher for workflow " + id + "!");
                return null;
            }

            cpeMatcher.putArray(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR, or);
        }

        // Pending only (execution not yet started)?
        if (bPendingOnly) {
            JsonObject pendingMatcher = cpeMatcher.copy();
            pendingMatcher.putObject(
                    Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "." + id + "." + Workflow.FIELD_NAME_STATE,
                    VertxMongoUtils.EXISTS_FALSE
            );

            if (!isActive()) {
                // For passive workflows, CPE device is only considered pending if discovered after
                // the workflow was created
                pendingMatcher.putObject(
                        Cpe.DB_FIELD_NAME_LAST_DISCOVER_TIME,
                        new JsonObject()
                                .putObject(
                                        VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_GREATER_THAN,
                                        rawJsonObject.getObject(AcsConstants.FIELD_NAME_CREATE_TIME)
                                )
                );
            }
            return pendingMatcher;
        } else {
            return cpeMatcher;
        }
    }

    /**
     * get a query matcher by workflow execution state.
     *
     * @param state
     * @return
     */
    public JsonObject getMatcherByState(String state) {
        return new JsonObject().putString(
                Cpe.DB_FIELD_NAME_WORKFLOW_EXEC + "." + id + "." + Workflow.FIELD_NAME_STATE,
                state
        );
    }

    /**
     * Test if a given CPE matches the matcher of this workflow.
     */
    public boolean matchCpe(JsonObject cpe) {
        // Try to match the CPE against the CPE matcher of this workflow
        JsonArray subMatcherArray = cpeMatcher.getArray(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_OR);
        boolean bMatched = false;
        for (int i =0; i < subMatcherArray.size(); i ++) {
            JsonObject subMatcher = subMatcherArray.get(i);
            if (subMatcher.containsField(AcsConstants.FIELD_NAME_ID)) {
                // List of CPE ID Strings
                bMatched = subMatcher.getArray(AcsConstants.FIELD_NAME_ID)
                        .contains(cpe.getString(AcsConstants.FIELD_NAME_ID));
            } else {
                // Group Matcher
                bMatched = CpeGroup.match(cpe, subMatcher);
            }

            if (bMatched) {
                break;
            }
        }

        return bMatched;
    }
}
