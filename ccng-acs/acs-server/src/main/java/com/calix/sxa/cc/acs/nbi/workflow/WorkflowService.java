package com.calix.sxa.cc.acs.nbi.workflow;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxConstants;
import com.calix.sxa.VertxMongoUtils;
import com.calix.sxa.cc.acs.nbi.AbstractAcNbiCrudService;
import com.calix.sxa.cc.acs.nbi.model.AcsNbiRequest;
import com.calix.sxa.cc.acs.worker.workflow.ActiveWorkflowTaskWorker;
import com.calix.sxa.cc.acs.worker.workflow.WorkflowCpeTracker;
import com.calix.sxa.cc.cache.ConfigurationProfileCache;
import com.calix.sxa.cc.cache.GroupCache;
import com.calix.sxa.cc.model.*;
import com.calix.sxa.cc.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.java.redis.RedisClient;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Project:  SXA-CC ACS API
 *
 * Workflow Web Service Implementation.
 *
 * @author: jqin
 */
public class WorkflowService extends AbstractAcNbiCrudService {
    /**
     * Constants
     */
    public static final String SUSPEND = "suspend";
    public static final String RESUME = "resume";


    /**
     * Static Error Object Constants
     */
    public static final JsonObject MISSING_ID_ON_DELETE = new JsonObject()
            .putString(AcsConstants.FIELD_NAME_ERROR, "Workflow ID String is required on deletions!");
    public static final JsonObject CANNOT_UPDATE_COMPLETED =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR,
                    "Workflow has already completed thus cannot be modified/edited!");
    public static final JsonObject CANNOT_UPDATE_IN_PROGRESS =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR,
                    "Workflow is in-progress thus cannot be modified/edited! (must suspend first)");
    public static final JsonObject CANNOT_RESUME_IN_PROGRESS =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR,
                    "Workflow is in-progress thus cannot be resumed right now!");
    public static final JsonObject CANNOT_RESUME_SCHEDULED =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR,
                    "Workflow is scheduled for the next maintenance window thus cannot be resumed right now!");
    public static final JsonObject ALREADY_SUSPENDED =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR,
                    "Workflow has already been suspended!");
    public static final JsonObject CANNOT_SUSPEND_COMPLETED =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR,
                    "Workflow has already completed thus cannot be suspended!");
    public static final JsonObject CANNOT_RESUME_COMPLETED =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR,
                    "Workflow has already completed thus cannot be resumed!");

    // Async Redis Client Instance
    RedisClient redisClient;

    /**
     * Start the service
     */
    @Override
    public void start(Vertx vertx) {
        super.start(vertx);

        /**
         * Initialize Redis Client
         */
        redisClient = new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);

        /**
         * Initialize Configuration Profile Cache
         */
        configurationProfileCache = new ConfigurationProfileCache(
                vertx,
                AcsConstants.VERTX_ADDRESS_ACS_CONFIG_PROFILE_CRUD,
                ConfigurationProfile.DB_COLLECTION_NAME,
                ConfigurationProfile.class.getSimpleName()
        );

        /**
         * Initialize Group Cache
         */
        groupCache = new GroupCache(
                vertx,
                AcsConstants.VERTX_ADDRESS_ACS_GROUP_CRUD,
                CpeGroup.DB_COLLECTION_NAME,
                CpeGroup.class.getSimpleName()
        );
    }

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_WORKFLOW;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return Workflow.DB_COLLECTION_NAME;
    }

    /**
     * Index Fields
     */
    private static final String[] INDEX_FIELDS = {
            AcsConstants.FIELD_NAME_ORG_ID,
            AcsConstants.FIELD_NAME_NAME
    };

    /**
     * Editable Fields
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{
        add(AcsConstants.FIELD_NAME_NAME);
        add(AcsConstants.FIELD_NAME_DESCRIPTION);
        add(Workflow.FIELD_NAME_ACTIONS);
        add(Workflow.FIELD_NAME_CPES);
        add(Workflow.FIELD_NAME_EXEC_POLICY);
        add(Workflow.FIELD_NAME_EXECUTE_ONCE_PER_CPE);
        add(Workflow.FIELD_NAME_GROUPS);
        add(Workflow.FIELD_NAME_INITIAL_TRIGGER);
        add(RESUME);
        add(SUSPEND);
    }};

    /**
     * Static CcExceptions and Errors
     */
    public static final SxaVertxException INVALID_SUSPEND_RESUME_SUSPEND_REQUEST =
            new SxaVertxException("Invalid Suspend/Resume/Suspend Request!");
    public static final JsonObject INVALID_CPE_ID = new JsonObject().putString(
            AcsConstants.FIELD_NAME_ERROR, "Invalid Device ID(s)!"
    );
    public static final SxaVertxException INVALID_GROUP_ID = new SxaVertxException("Invalid Device Group ID(s)!");
    public static final SxaVertxException APPLY_MULTIPLE_VIDEO_SERVICE = new SxaVertxException(
            "Cannot apply multiple video services!"
    );
    public static final SxaVertxException APPLY_MULTIPLE_VOICE_SERVICE = new SxaVertxException(
            "Cannot apply multiple voice services!"
    );
    public static final SxaVertxException APPLY_SERVICE_PROFILE_TO_GPON_DEVICES = new SxaVertxException(
            "Service Profiles cannot be applied to 8xxG Models!"
    );
    public static final SxaVertxException APPLY_SERVICE_PROFILE_WITHOUT_MODELS = new SxaVertxException(
            "When applying Service Profiles via Workflows, the device group must explicitly match on one or more 844E Models!"
    );

    public static final SxaVertxException DOWNLOAD_SW_IMAGE_TO_GPON_DEVICES = new SxaVertxException(
            "Cannot download SW/FW Images to 8xxG Models!"
    );
    public static final SxaVertxException DOWNLOAD_SW_IMAGE_WITHOUT_MODELS = new SxaVertxException(
            "When downloading SW/FW Images via Workflows, the device group must explicitly match on one or more 844E Models!"
    );

    public static final JsonObject NO_MATCHING_CPE_FOUND = new JsonObject().putString(
            AcsConstants.FIELD_NAME_ERROR, "No Matching Device found for the given device group(s)!"
    );
    public static final JsonObject INVALID_IMAGE_ID = new JsonObject().putString(
            AcsConstants.FIELD_NAME_ERROR, "Invalid Image ID!"
    );
    public static final JsonObject INVALID_PROFILE_ID = new JsonObject().putString(
            AcsConstants.FIELD_NAME_ERROR, "Invalid Profile ID!"
    );

    /**
     * All Device Type Attributes are index fields.
     */
    @Override
    public String[] getIndexFieldName() {
        return INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return EDITABLE_FIELDS;
    }

    /**
     * Mod Mongo Query Keys that asks for id only
     */
    public static final JsonObject FIND_KEY_ID_ONLY = new JsonObject().putNumber(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, 1);

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
     * @throws com.calix.sxa.SxaVertxException
     */
    public boolean validate(final AcsNbiRequest nbiRequest, final AcsApiCrudTypeEnum crudType) throws SxaVertxException {
        switch (crudType) {
            case Update:
                // Check for suspend/suspend/resume requests
                if (isSuspendRequest(nbiRequest) || isResumeRequest(nbiRequest)) {
                    return true;
                }

                // Fall Through to exercise the following common logic that is shared with Create
            case Create:
                /**
                 * For Create/Update, we will need to check the groupId/cpeId and/or fileId/profileId referred from
                 * this workflow exist.
                 */
                final Workflow workflow = Workflow.validateJsonObject(nbiRequest.body, configurationProfileCache);
                nbiRequest.serviceData = workflow;

                // Validate CPE ID(s) if any
                if (workflow.cpeIdStrings != null) {
                    subValidation(
                            nbiRequest,
                            crudType,
                            workflow,
                            Cpe.CPE_COLLECTION_NAME,
                            workflow.cpeIdStrings,
                            null,
                            INVALID_CPE_ID
                    );
                }

                // Retrieve/Validate File ID(s) if any
                boolean bHasServiceProfile = false;
                boolean bHasImageDownload = false;
                boolean bHasVideoService = false;
                boolean bHasVoiceService = false;
                JsonArray allVLANs = new JsonArray();
                for (WorkflowAction action : workflow.actionChain) {
                    if (action.fileId != null) {
                        subValidation(
                                nbiRequest,
                                crudType,
                                workflow,
                                AcsFile.DB_COLLECTION_NAME,
                                null,
                                action.fileId,
                                INVALID_IMAGE_ID
                        );
                    }

                    // Check for service profile and SW Image Download which require 844E model
                    if (action.services != null && action.services.size() > 0) {
                        for (int i = 0; i < action.services.size(); i ++) {
                            JsonObject aService = action.services.get(i);
                            String serviceName = aService.getString(ConfigurationCategory.PARAM_NAME_SERVICE_NAME);
                            if (ConfigurationCategory.VIDEO_SERVICE.equals(serviceName)) {
                                if (bHasVideoService) {
                                    throw APPLY_MULTIPLE_VIDEO_SERVICE;
                                } else {
                                    bHasVideoService = true;
                                }
                            } else if (ConfigurationCategory.VOICE_SERVICE.equals(serviceName)) {
                                if (bHasVoiceService) {
                                    throw APPLY_MULTIPLE_VOICE_SERVICE;
                                } else {
                                    bHasVoiceService = true;
                                }
                            }

                            // Check for VLAN ID conflicts
                            int vlan = aService.getInteger("X_000631_VlanMuxID");
                            if (allVLANs.contains(vlan)) {
                                throw new SxaVertxException(
                                        "Trying to configure multiple services using "
                                        + ((vlan == -1)? "the untagged VLAN" : ("VLAN " + vlan))
                                        + "!"
                                );
                            }
                        }
                        bHasServiceProfile = true;
                    } else if (action.actionEnum.equals(WorkflowActionEnum.DOWNLOAD_FW_IMAGE)) {
                        bHasImageDownload = true;
                    }
                }

                // Validate Group ID(s) if any
                if (workflow.groupIdStrings != null) {
                    for (int i = 0; i < workflow.groupIdStrings.size(); i ++) {
                        String groupId = workflow.groupIdStrings.get(i);
                        CpeGroup group = groupCache.getGroupById(groupId);
                        if (group == null) {
                            log.error("Invalid Group Id " + groupId + "!");
                            throw INVALID_GROUP_ID;
                        }

                        /**
                         * If applying service profiles, group filter must specify 844E models.
                         */
                        /*
                        Commented out due to SXACC-1263
                        if (bHasServiceProfile || bHasImageDownload) {
                            if (group.bGroupMatchOnGponModel(false)) {
                                workflow.bValidationResponseSent = true;
                                if (bHasServiceProfile) {
                                    throw APPLY_SERVICE_PROFILE_TO_GPON_DEVICES;
                                }
                                if (bHasImageDownload) {
                                    throw DOWNLOAD_SW_IMAGE_TO_GPON_DEVICES;
                                }
                            } else if (group.bGroupMatchOnGponModel(true)) {
                                workflow.bValidationResponseSent = true;
                                if (bHasServiceProfile) {
                                    throw APPLY_SERVICE_PROFILE_WITHOUT_MODELS;
                                }
                                if (bHasImageDownload) {
                                    throw DOWNLOAD_SW_IMAGE_WITHOUT_MODELS;
                                }
                            }
                        }
                        */
                    }
                }

                // Build CPE Matcher
                JsonObject matcher = workflow.getMatcher(groupCache, false);
                nbiRequest.body.putString(Workflow.FIELD_NAME_CPE_MATCHER, matcher.encode());

                // Create Time
                JsonObject currDateTime = VertxMongoUtils.getDateObject();
                nbiRequest.body.putObject(AcsConstants.FIELD_NAME_CREATE_TIME, currDateTime);
                if (!workflow.isActive()) {
                    // Passive workflows start right away
                    nbiRequest.body.putObject(Workflow.FIELD_NAME_START, currDateTime);
                }

                if (workflow.validationPendingCount == 0) {
                    // No pending validation
                    completeSubValidation(nbiRequest, crudType, workflow, true, null);
                }
                break;

            case Delete:
            case Retrieve:
                return true;
        }

        // Validation pending
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
     * @throws com.calix.sxa.SxaVertxException  if one or more index fields are missing.
     */
    @Override
    public JsonObject buildIndexMatcher(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType)
            throws SxaVertxException{
        if (crudType.equals(AcsApiCrudTypeEnum.Update)) {
            // Check for suspend/resume requests
            if (isSuspendRequest(nbiRequest) || isResumeRequest(nbiRequest)) {
                return null;
            }
        }

        return super.buildIndexMatcher(nbiRequest, crudType);
    }
    /**
     * Perform a sub validation (cpeId/groupId/etc).
     *
     * @param nbiRequest
     * @param crudType
     * @param workflow
     * @param dbCollectionName
     * @param idArray
     * @param singleId
     * @param error     Predefined Error to be used if validation fails.
     */
    public void subValidation(
            final AcsNbiRequest nbiRequest,
            final AcsApiCrudTypeEnum crudType,
            final Workflow workflow,
            final String dbCollectionName,
            JsonArray idArray,
            final String singleId,
            final JsonObject error)
            throws SxaVertxException {
        workflow.validationPendingCount ++;

        final int batchSize;

        // Build matcher
        JsonObject matcher = new JsonObject();
        if (idArray != null) {
            matcher.putObject(
                    VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID,
                    new JsonObject().putArray(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN, idArray)
            );
            batchSize = idArray.size();
        } else {
            matcher.putString(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, singleId);
            batchSize = 1;
        }

        // Determine Find Key
        JsonObject keys = FIND_KEY_ID_ONLY;
        if (dbCollectionName.equals(AcsFile.DB_COLLECTION_NAME)) {
            keys = null;
        }

        // mongo example: db.inventory.find( { qty: { $in: [ 5, 15 ] } } )
        VertxMongoUtils.find(
                vertx.eventBus(),
                dbCollectionName,
                matcher,
                new VertxMongoUtils.FindHandler(
                        new Handler<JsonArray>() {
                            @Override
                            public void handle(JsonArray queryResult) {
                                boolean bSucceeded = false;
                                // Check for MongoDB timed out
                                if (!VertxMongoUtils.FIND_TIMED_OUT.equals(queryResult) &&
                                        queryResult != null && queryResult.size() == batchSize) {
                                    // Found all the id(s)
                                    bSucceeded = true;

                                    if (dbCollectionName.equals(AcsFile.DB_COLLECTION_NAME)) {
                                        // Save the AcsFile struct into workflow
                                        JsonArray actions = nbiRequest.body.getArray(Workflow.FIELD_NAME_ACTIONS);
                                        for (int i = 0; i < actions.size(); i ++) {
                                            JsonObject anAction = actions.get(i);
                                            if (singleId.equals(anAction.getString(WorkflowAction.FIELD_NAME_FILE_ID))){
                                                JsonObject fileStruct = queryResult.get(0);
                                                // Save the actual file struct (except the file content if any) in cache
                                                fileStruct.removeField(AcsFile.FIELD_NAME_TEXT_CONTENT);
                                                fileStruct.removeField(AcsFile.FIELD_NAME_BINARY_CONTENT);
                                                anAction.putObject(WorkflowAction.FIELD_NAME_FILE_STRUCT, fileStruct);
                                            }
                                        }
                                    }
                                }
                                completeSubValidation(nbiRequest, crudType, workflow, bSucceeded, error);
                            }
                        }
                ),
                keys,
                batchSize
        );
    }

    /**
     * Completing a sub validation (cpeId/groupId/etc).
     *
     * @param nbiRequest
     * @param crudType
     * @param workflow
     * @param bSucceeded
     * @param error
     */
    public void completeSubValidation(
            final AcsNbiRequest nbiRequest,
            final AcsApiCrudTypeEnum crudType,
            Workflow workflow,
            boolean bSucceeded,
            JsonObject error) {
        // Have we already sent response for this workflow validation?
        if (workflow.bValidationResponseSent) {
            // Yes. Do nothing
            return;
        }

        // Succeeded?
        if (bSucceeded) {
            workflow.validationPendingCount --;
            if (workflow.validationPendingCount <= 0) {
                // No more pending sub validation
                if (!workflow.isActive()) {
                    // Passive workflows cannot have a fixed total count up front
                    nbiRequest.body.putString(Workflow.FIELD_NAME_STATE, Workflow.STATE_IN_PROGRESS);
                    // Resume the CRUD Operation
                    postValidation(nbiRequest, crudType);
                } else {
                    checkTotalCount(nbiRequest, crudType, workflow);
                }
            }
        } else {
            workflow.bValidationResponseSent = true;
            log.error("Validation failed due to " + error.encode() + "!\n" + nbiRequest.body.encodePrettily());
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, error);
        }
    }

    /**
     * Check total count prior to persisting the workflow.
     *
     * This is the last step of the validation.
     *
     * @param nbiRequest
     * @param crudType
     * @param workflow
     */
    public void checkTotalCount(
            final AcsNbiRequest nbiRequest,
            final AcsApiCrudTypeEnum crudType,
            Workflow workflow) {
        workflow.id = UUID.randomUUID().toString();

        // For create, initialize "state" to "pending"
        if (crudType.equals(AcsApiCrudTypeEnum.Create)) {
            nbiRequest.body.putString(Workflow.FIELD_NAME_STATE, Workflow.STATE_SCHEDULED);
            nbiRequest.body.putString(AcsConstants.FIELD_NAME_ID, workflow.id);
        }

        int totalCount = 0;

        // Query all matching CPEs
        try {
            VertxMongoUtils.count(
                    vertx.eventBus(),
                    Cpe.CPE_COLLECTION_NAME,
                    workflow.getMatcher(groupCache, false),
                    new Handler<Long>() {
                        @Override
                        public void handle(Long count) {
                            if (count == null || count < 0) {
                                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                        getServerInternalErrorWithDetails());
                            } else {
                                if (count == 0) {
                                    nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, NO_MATCHING_CPE_FOUND);
                                } else {
                                    // Add total count
                                    nbiRequest.body.putNumber(Workflow.FIELD_NAME_TOTAL_COUNT, count);

                                    // Resume the CRUD Operation
                                    postValidation(nbiRequest, crudType);
                                }
                            }
                        }
                    }
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Additional Validation for updates after the find-before-update query result is in.
     * @param newRecord
     * @param oldRecord
     *
     * @return  An error string if failed, or null if succeeded.
     */
    public static final List<String> SKIP_FIELDS = new ArrayList<String>() {{
        add(AcsConstants.FIELD_NAME_ID);
        add(AcsConstants.FIELD_NAME_NAME);
        add(AcsConstants.FIELD_NAME_DESCRIPTION);
        add(Workflow.FIELD_NAME_STATE);
    }};

    /**
     * Additional Validation for updates after the find-before-update query result is in.
     *
     * @param nbiRequest
     * @param newRecord
     * @param oldRecord
     *
     * @return  An error string if failed, or null if succeeded.
     */
    @Override
    public boolean additionalFindBeforeUpdateQueryResultValidation(
            AcsNbiRequest nbiRequest, JsonObject newRecord, JsonObject oldRecord) {
        String currentState = oldRecord.getString(Workflow.FIELD_NAME_STATE);

        if (currentState == null) {
            log.error("Fatal error: workflow has no state!\n" + oldRecord.encodePrettily());
            nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, getServerInternalErrorWithDetails());
            return VALIDATION_PENDING_OR_FAILED;
        }

        // Convert to Workflow POJO
        Workflow workflow;
        try {
            workflow = Workflow.validateJsonObject(oldRecord);
        } catch (SxaVertxException e) {
            nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, getServerInternalErrorWithDetails());
            log.error("Failed to create workflow pojo!\n" + oldRecord.encodePrettily());
            return VALIDATION_PENDING_OR_FAILED;
        }
        nbiRequest.serviceData = workflow;

        JsonObject error = null;
        boolean pending = false;

        // Check for suspend/resume requests
        if (isSuspendRequest(nbiRequest)) {
            switch (currentState) {
                case Workflow.STATE_IN_PROGRESS:
                case Workflow.STATE_SCHEDULED:
                    doSuspend(
                            nbiRequest,
                            AcsApiCrudTypeEnum.Update,
                            workflow,
                            currentState
                    );
                    if (workflow.isActive()) {
                        pending = true;
                    }
                    break;

                case Workflow.STATE_SUSPENDED:
                    error = ALREADY_SUSPENDED;
                    break;

                case Workflow.STATE_COMPLETED:
                    error = CANNOT_SUSPEND_COMPLETED;
                    break;
            }
        } else if (isResumeRequest(nbiRequest)) {
            switch (currentState) {
                case Workflow.STATE_SUSPENDED:
                    doResume(nbiRequest, workflow);
                    if (workflow.isActive()) {
                        pending = true;
                    }
                    break;

                case Workflow.STATE_IN_PROGRESS:
                    error = CANNOT_RESUME_IN_PROGRESS;
                    break;

                case Workflow.STATE_SCHEDULED:
                    error = CANNOT_RESUME_SCHEDULED;
                    break;

                case Workflow.STATE_COMPLETED:
                    error = CANNOT_RESUME_COMPLETED;
                    break;
            }
        } else {
            /**
             * For ordinary updates, Only name and description and state are editable once the execution has started
             *
             * Let us compare all other fields
             */
            switch (currentState) {
                case Workflow.STATE_SUSPENDED:
                case Workflow.STATE_SCHEDULED:
                    /**
                     * TODO: check for exec policy (maintenance window) change
                     */
                    break;

                case Workflow.STATE_IN_PROGRESS:
                    error = CANNOT_UPDATE_IN_PROGRESS;
                    break;

                case Workflow.STATE_COMPLETED:
                    error = CANNOT_UPDATE_COMPLETED;
                    break;
            }
        }

        if (error != null) {
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, error);
            return VALIDATION_PENDING_OR_FAILED;
        } else if (pending) {
            return VALIDATION_PENDING_OR_FAILED;
        } else {
            return VALIDATION_SUCCEEDED;
        }
    }

    /**
     * Is the request a resume request? 
     * @param nbiRequest
     * @return
     */
    public boolean isResumeRequest(AcsNbiRequest nbiRequest) {
        return RESUME.equals(getUpdateTypeFromUrlPathParams(nbiRequest));
    }

    /**
     * Is the request a suspend request?
     * @param nbiRequest
     * @return
     */
    public boolean isSuspendRequest(AcsNbiRequest nbiRequest) {
        return SUSPEND.equals(getUpdateTypeFromUrlPathParams(nbiRequest));
    }

    /**
     * Get the Update Type from URL Path Parameter
     */
    public String getUpdateTypeFromUrlPathParams(AcsNbiRequest nbiRequest) {
        if (nbiRequest.urlPathParams != null && nbiRequest.urlPathParams.length > 1) {
            return nbiRequest.urlPathParams[1];
        } else {
            return null;
        }
    }

    /**
     * Resume a workflow that has been suspended before.
     *
     * @param nbiRequest
     * @param workflow
     */
    public void doResume(
            final AcsNbiRequest nbiRequest,
            final Workflow workflow) {
        log.info("Resuming workflow " + workflow.id + " (state: " + workflow.state + ")");
        if (workflow.isActive()) {
            /**
             * Active Workflow. Enqueue the sub task to Redis
             */
            if (workflow.execPolicy.maintenanceWindow.isOpen()) {
                workflow.state = Workflow.STATE_IN_PROGRESS;
            } else {
                workflow.state = Workflow.STATE_SCHEDULED;
            }
            enqueueWorkflowTask(
                    nbiRequest,
                    workflow,
                    AcsApiCrudTypeEnum.Update
            );
        } else {
            // Passive workflow. Just change the state and publish the Update event
            nbiRequest.body = workflow.rawJsonObject.putString(Workflow.FIELD_NAME_STATE, Workflow.STATE_IN_PROGRESS);
        }
    }

    /**
     * Execute an suspend request.
     *
     * @param nbiRequest
     * @param crudType
     * @param workflow
     * @param currentState
     */
    public void doSuspend(
            final AcsNbiRequest nbiRequest,
            final AcsApiCrudTypeEnum crudType,
            final Workflow workflow,
            final String currentState) {
        /**
         * Send suspend request to worker
         */
        log.info("Suspending workflow " + workflow.id + " (state: " + currentState + ")");

        if (!workflow.isActive()) {
            // For Passive workflows, simply publish the update and worker will get it
            nbiRequest.body = workflow.rawJsonObject.putString(Workflow.FIELD_NAME_STATE, Workflow.STATE_SUSPENDED);
            return;
        }

        // timeout in 30 seconds if in-progress or 2 second if not
        long timeout = 2000;
        if (currentState.equals(Workflow.STATE_IN_PROGRESS)) {
            timeout = 30000;
        }
        vertx.eventBus().sendWithTimeout(
                AcsConstants.VERTX_ADDRESS_WORKFLOW_SUSPEND + "." + workflow.id,
                ActiveWorkflowTaskWorker.REMOTE_REQUEST_SUSPEND,
                timeout,
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        if (asyncResult.failed()) {
                            if (currentState.equals(Workflow.STATE_IN_PROGRESS)) {
                                log.error("Failed to get response back from workflow worker for an suspend request!"
                                        + "workflow id:" + workflow.id);
                            }

                            // no worker working on this workflow
                            //log.info("Deleting pending Resque task for this workflow if any...");
                            /**
                             * Delete pending Resque task for this workflow if any
                             */
                            ActiveWorkflowTaskWorker.removeTaskByWorkflowId(
                                    workflow.id,
                                    redisClient,
                                    new Handler<Long>() {
                                        @Override
                                        public void handle(Long numberOfTaskDeleted) {
                                            if (numberOfTaskDeleted == null) {
                                                log.info("No task found for workflow " + workflow.id);
                                            } else if (numberOfTaskDeleted == 1) {
                                                log.info("Deleted pending task for workflow " + workflow.id);
                                            } else {
                                                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                        getServerInternalErrorWithDetails());
                                                log.error("Failed to delete pending task for workflow " + workflow.id + "!");
                                                return;
                                            }

                                            if (crudType.equals(AcsApiCrudTypeEnum.Delete)) {
                                                // Delete it
                                                deleteFromDb(nbiRequest, workflow.id);
                                            } else {
                                                // Update task state
                                                Workflow.changeState(
                                                        vertx,
                                                        workflow.id,
                                                        Workflow.STATE_SUSPENDED,
                                                        new ChangeStateHandler(nbiRequest, workflow.id)
                                                );
                                            }
                                        }
                                    }
                            );
                        } else {
                            // succeeded
                            log.info("Received reply for the suspend request for workflow " + workflow.id);
                            if (crudType.equals(AcsApiCrudTypeEnum.Delete)) {
                                // Delete it
                                deleteFromDb(nbiRequest, workflow.id);
                            } else {
                                // Update task state
                                Workflow.changeState(
                                        vertx,
                                        workflow.id,
                                        Workflow.STATE_SUSPENDED,
                                        new ChangeStateHandler(nbiRequest, workflow.id)
                                );
                            }
                        }
                    }
                }
        );
    }

    /**
     * Inner Handler class for change-state operation.
     */
    public class ChangeStateHandler implements Handler<Long> {
        AcsNbiRequest nbiRequest;
        String workflowId;

        // Constructor
        public ChangeStateHandler(AcsNbiRequest nbiRequest, String workflowId) {
            this.nbiRequest = nbiRequest;
            this.workflowId = workflowId;
        }

        // Handler Body
        @Override
        public void handle(Long updateResult) {
            if (updateResult == null || updateResult != 1) {
                /**
                 * Log the error message upon failures
                 */
                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, getServerInternalErrorWithDetails());
                log.error("Failed to update workflow state!");
            } else {
                nbiRequest.sendResponse(HttpResponseStatus.OK);
            }
        }
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
        return AcsConstants.VERTX_ADDRESS_WORKFLOW_CRUD;
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
        if (HttpResponseStatus.OK.equals(httpResponseStatus)) {
            final Workflow workflow = (Workflow) nbiRequest.serviceData;

            if (workflow.isActive() && nbiRequest.body.getInteger(Workflow.FIELD_NAME_TOTAL_COUNT) > 0) {
                workflow.setId(id);
                /**
                 * Enqueue a new Resque task to Redis
                 */
                enqueueWorkflowTask(nbiRequest, workflow, AcsApiCrudTypeEnum.Create);
                return true;
            } else {
                super.postCreate(nbiRequest, httpResponseStatus, id, error);
                return false;
            }
        } else {
            super.postCreate(nbiRequest, httpResponseStatus, id, error);
            return false;
        }
    }

    /**
     * Enqueue the initial task for a workflow (into Resque/Redis).
     *
     * @param nbiRequest
     * @param workflow
     */
    public void enqueueWorkflowTask(
            final AcsNbiRequest nbiRequest,
            final Workflow workflow,
            final AcsApiCrudTypeEnum crudType) {
        if (workflow.isActive()) {
            /**
             * Enqueue the sub task to Redis
             */
            ActiveWorkflowTaskWorker.enqueue(
                    workflow,
                    1,
                    workflow.getInitialDelay(),
                    redisClient,
                    new Handler<Message<JsonObject>>() {
                        @Override
                        public void handle(Message<JsonObject> enqueueResult) {
                            JsonObject result = enqueueResult.body();
                            if (result == null ||
                                    !("ok".equals(result.getString("status")))
                                    ) {
                                log.error("Received unexpected result when en-queuing a new workflow task!\n"
                                        + "Workflow:\n" + workflow.rawJsonObject.encodePrettily()
                                        + "\nEnqueue Result:\n"
                                        + (result == null ? "(null)" : result.encodePrettily()));
                                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                        getServerInternalErrorWithDetails());
                            } else {
                                log.info("Successfully en-queued a new workflow task to redis.");

                                if (crudType.equals(AcsApiCrudTypeEnum.Update)) {
                                    /**
                                     * Change workflow state due to resume action
                                     */
                                    Workflow.changeState(
                                            vertx,
                                            workflow.id,
                                            workflow.state,
                                            new Handler<Long>() {
                                                @Override
                                                public void handle(Long changeStateResult) {
                                                    if (changeStateResult == null || changeStateResult != 1) {
                                                        log.error("Received unexpected result when changing workflow "
                                                                + "state to "
                                                                + workflow.state + ". Workflow:\n"
                                                                + workflow.rawJsonObject.encodePrettily());
                                                        nbiRequest.sendResponse(
                                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                getServerInternalErrorWithDetails()
                                                        );
                                                    } else {
                                                        log.info("Successfully changed workflow state.");
                                                        nbiRequest.sendResponse(HttpResponseStatus.OK);
                                                    }
                                                }
                                            }
                                    );
                                } else {
                                    sendCreateResponse(nbiRequest, HttpResponseStatus.OK, workflow.id, null);
                                }
                            }
                        }
                    }
            );
        }
    }

    /**
     * Find Result Handler (used by delete)
     */
    public class FindBeforeDeleteResultHandler extends VertxMongoUtils.FindOneHandler{
        AcsNbiRequest nbiRequest;

        /**
         * Constructor that requires an AcsNbiRequest POJO
         */
        public FindBeforeDeleteResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
        }

        /**
         * The handler method body.
         * @param jsonObjectMessage
         */
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            // Call super
            super.handle(jsonObjectMessage);

            // Any match found?
            if (queryResult != null && queryResult.containsField(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID)) {
                // Found existing document
                // Extract the id
                String workflowId = queryResult.getString(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID);
                log.info("Deleting workflow " + workflowId
                        + "(name: " + queryResult.getString(AcsConstants.FIELD_NAME_NAME) + ")");

                if (workflowId == null) {
                    nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, MISSING_ID_ON_DELETE);
                    return;
                }
                nbiRequest.body.putString(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, workflowId);

                Workflow workflow;
                try {
                    // Convert to Workflow POJO
                    workflow = Workflow.validateJsonObject(queryResult);
                } catch (SxaVertxException e) {
                    nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, getServerInternalErrorWithDetails());
                    log.error("Failed to create workflow pojo!\n" + queryResult.encodePrettily());
                    return;
                }
                nbiRequest.serviceData = workflow;

                if (Workflow.STATE_COMPLETED.equals(workflow.state)
                        || Workflow.STATE_SUSPENDED.equals(workflow.state)
                        || !workflow.isActive()) {
                    /**
                     * No need to suspend the workflow if completed or already suspended or passive
                     *
                     * Just delete it from MongoDB
                     */
                    deleteFromDb(nbiRequest, workflowId);
                } else {
                    /**
                     * Suspend the workflow prior to deleting it
                     */
                    doSuspend(
                            nbiRequest,
                            AcsApiCrudTypeEnum.Delete,
                            workflow,
                            queryResult.getString(Workflow.FIELD_NAME_STATE)
                    );
                }
            } else {
                log.error("No match found for delete with matcher " + jsonObjectMessage.body().encode());
                nbiRequest.sendResponse(HttpResponseStatus.NOT_FOUND, NO_MATCH_FOUND);
            }
        }
    }

    /**
     * Perform the actual Delete.
     *
     * @param nbiRequest
     */
    @Override
    public void doDelete(AcsNbiRequest nbiRequest) {
        /**
         * Query the DB to get the workflow's current state
         */
        // Build matcher by index fields
        // Get "_id"

        // The "_id" is mandatory for deleting a single entry
        String id = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
        if (id == null) {
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, MISSING_ID_OR_FILTER);
            return;
        }
        JsonObject matcher = new JsonObject().putString(AcsConstants.FIELD_NAME_ID, id);

        /**
         * First Query with the index field, so we only overwrite existing document
         */
        try {
            VertxMongoUtils.findOne(
                    vertx.eventBus(),
                    getDbCollectionName(),
                    matcher,
                    new FindBeforeDeleteResultHandler(nbiRequest),
                    null        // null query key means returns everything
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
            nbiRequest.sendResponse(
                    HttpResponseStatus.BAD_REQUEST,
                    NO_MATCH_FOUND);
        }
    }

    /**
     * Delete workflow from the MongoDB (last step of the delete operation).
     *
     * @param nbiRequest
     * @param workflowId
     */
    public void deleteFromDb(AcsNbiRequest nbiRequest, String workflowId) {
        log.info("Deleting workflow " + workflowId + " from DB...");
        try {
            VertxMongoUtils.delete(
                    vertx.eventBus(),
                    getDbCollectionName(),
                    workflowId,
                    getMongoDeleteHandler(nbiRequest)
            );
        } catch (Exception e) {
            e.printStackTrace();
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, BAD_REQUEST);
        }
    }

    /**
     * Override default Post Delete Handler to delete all per-CPE workflow action results
     *
     * @param nbiRequest
     * @param bSucceeded
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    @Override
    public boolean postDelete(AcsNbiRequest nbiRequest, boolean bSucceeded) {
        if (bSucceeded) {
            String unset = Cpe.DB_FIELD_NAME_WORKFLOW_EXEC;
            String id = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
            if (id != null) {
                unset += "." + id;
            }
            /**
             * Delete all the execution logs from CPE collection
             */
            try {
                VertxMongoUtils.updateWithMatcher(
                        vertx.eventBus(),
                        Cpe.CPE_COLLECTION_NAME,
                        // Matcher
                        new JsonObject().putObject(unset, VertxMongoUtils.EXISTS),
                        // Updates
                        VertxMongoUtils.getUpdatesObject(
                                // sets
                                null,
                                // unsets
                                VertxMongoUtils.addUnset(null, unset),
                                // timestamps
                                null,
                                // pulls
                                null,
                                // pushes
                                null
                        ),
                        VertxMongoUtils.DEFAULT_MULTI_TIMEOUT,
                        null
                );
            } catch (SxaVertxException e) {
                e.printStackTrace();
            }
        }

        return super.postDelete(nbiRequest, bSucceeded);
    }

    /**
     * Build MongoDB Query Keys for Retrieve.
     *
     * Default to null (return everything)
     */
    private static final JsonObject QUERY_KEY_BRIEF = new JsonObject()
            .putNumber(Workflow.FIELD_NAME_ACTIONS + "." + WorkflowAction.FIELD_NAME_PARAMETER_VALUES, 0)
            .putNumber(Workflow.FIELD_NAME_GROUPS, 0)
            .putNumber(Workflow.FIELD_NAME_CPES, 0)
            .putNumber(Workflow.FIELD_NAME_EXEC_POLICY, 0)
            .putNumber(AcsConstants.FIELD_NAME_ORG_ID, 0);
    private static final JsonObject QUERY_KEY_DEFAULT = new JsonObject()
            .putNumber(Workflow.FIELD_NAME_CPE_MATCHER, 0);
    public JsonObject buildRetrieveQueryKeys(AcsNbiRequest nbiRequest) {
        if (nbiRequest.getQueryBrief()) {
            return QUERY_KEY_BRIEF;
        } else {
            return QUERY_KEY_DEFAULT;
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
    @Override
    public JsonArray postRetrieve(final AcsNbiRequest nbiRequest, JsonArray queryResults, boolean moreExists) {
        if (nbiRequest.body.containsField(AcsConstants.FIELD_NAME_ID) &&
                queryResults != null && queryResults.size() == 1) {
            final JsonObject response = (JsonObject)queryResults.get(0);

            // Convert to Workflow POJO and get matcher
            final Workflow workflow;
            try {
                // Convert to Workflow POJO
                workflow = Workflow.validateJsonObject(response);
            } catch (SxaVertxException e) {
                log.error("Failed to create workflow pojo!\n" + queryResults.encodePrettily());
                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, getServerInternalErrorWithDetails());
                return null;
            }
            final String workflowId = workflow.id;

            /**
             * Remove fields that are not applicable
             */
            response.removeField(Workflow.FIELD_NAME_TOTAL_COUNT);
            response.removeField(Workflow.FIELD_NAME_SUCCESS_COUNT);
            response.removeField(Workflow.FIELD_NAME_FAILURE_COUNT);
            response.removeField(Workflow.FIELD_NAME_IN_PROGRESS_COUNT);
            JsonArray actions = response.getArray(Workflow.FIELD_NAME_ACTIONS);
            if (actions != null) {
                for (int i = 0; i < actions.size(); i ++) {
                    JsonObject anAction = actions.get(i);
                    anAction.removeField(WorkflowAction.FIELD_NAME_FILE_STRUCT);
                    if (anAction.containsField(WorkflowAction.FIELD_NAME_PROFILE_ID)) {
                        anAction.removeField(WorkflowAction.FIELD_NAME_PARAMETER_VALUES);
                    }
                }
            }

            /**
             * Run additional query to get counts
             */
            // Query Result Handler
            final Handler<Long> countHandler = new Handler<Long>() {
                @Override
                public void handle(Long count) {
                    JsonObject matcher = null;
                    if (count != null) {
                        if (!response.containsField(Workflow.FIELD_NAME_SUCCESS_COUNT)) {
                            // Received success count
                            response.putNumber(Workflow.FIELD_NAME_SUCCESS_COUNT, count);
                            // Query for failure count
                            matcher = workflow.getMatcherByState(WorkflowCpeTracker.STATE_FAILED);
                        } else if (!response.containsField(Workflow.FIELD_NAME_FAILURE_COUNT)) {
                            // Received failure count
                            response.putNumber(Workflow.FIELD_NAME_FAILURE_COUNT, count);
                            // Query for in-progress count
                            matcher = workflow.getMatcherByState(WorkflowCpeTracker.STATE_IN_PROGRESS);
                        } else if (!response.containsField(Workflow.FIELD_NAME_IN_PROGRESS_COUNT)) {
                            // Received In-Progress count
                            response.putNumber(Workflow.FIELD_NAME_IN_PROGRESS_COUNT, count);

                            // Try to get pending count if applicable
                            if (workflow.isActive() && workflow.state.equals(Workflow.STATE_COMPLETED)) {
                                /**
                                 * Pending Count for Completed Active Workflows is 0
                                 */
                                response.putNumber(Workflow.FIELD_NAME_PENDING_COUNT, 0);
                            } else {
                                // Query for pending count
                                matcher = workflow.getMatcher(groupCache, true);
                            }
                        } else {
                            // Received pending count
                            response.putNumber(Workflow.FIELD_NAME_PENDING_COUNT, count);
                        }

                        if (matcher != null) {
                            // Keep querying
                            try {
                                VertxMongoUtils.count(
                                        vertx.eventBus(),
                                        Cpe.CPE_COLLECTION_NAME,
                                        matcher,
                                        this
                                );
                            } catch (SxaVertxException e) {
                                // This should never happen
                                log.error("Caught exception " + e.getMessage()
                                        + " when querying counts for workflow " + workflowId
                                        + " with matcher " + matcher + "!");
                                nbiRequest.sendResponse(
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                        getServerInternalErrorWithDetails()
                                );
                            }
                        } else {
                            // Got all individual counts
                            // Calculate total count
                            response.putNumber(
                                    Workflow.FIELD_NAME_TOTAL_COUNT,
                                    response.getLong(Workflow.FIELD_NAME_SUCCESS_COUNT)
                                            + response.getLong(Workflow.FIELD_NAME_FAILURE_COUNT)
                                            + response.getLong(Workflow.FIELD_NAME_IN_PROGRESS_COUNT)
                                            + response.getLong(Workflow.FIELD_NAME_PENDING_COUNT)
                            );
                            nbiRequest.sendResponse(HttpResponseStatus.OK, response);
                        }
                    }
                }
            };

            // Get the first count (success)
            try {
                VertxMongoUtils.count(
                        vertx.eventBus(),
                        Cpe.CPE_COLLECTION_NAME,
                        workflow.getMatcherByState(WorkflowCpeTracker.STATE_SUCCEEDED),
                        countHandler
                );
            } catch (SxaVertxException e) {
                // This should never happen
                log.error("Caught exception " + e.getMessage() + " when querying counts for workflow " + workflow.id);
                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, getServerInternalErrorWithDetails());
                return null;
            }

            /**
             * Return null to delay the response
             */
            return null;
        } else {
            return super.postRetrieve(nbiRequest, queryResults, moreExists);
        }
    }

    /**
     * Get the list of field names that contain MongoDB "$date" timestamp.
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    private static final String[] DATE_TIME_FIELD_NAME = {
            Workflow.FIELD_NAME_START,
            Workflow.FIELD_NAME_END,
            AcsConstants.FIELD_NAME_CREATE_TIME
    };
    public String[] getDateTimeFieldName() {
        return DATE_TIME_FIELD_NAME;
    }

    /**
     * Notify other server instances about this CRUD operation.
     *
     * @param nbiRequest
     * @param crudType
     */
    @Override
    public void publishCrudEvent(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) {
        if (nbiRequest.serviceData == null) {
            log.error("Internal Error: no workflow POJO found in service data!");
            return;
        }

        Workflow workflow = (Workflow)nbiRequest.serviceData;
        if (workflow == null || workflow.isActive()) {
            return;
        }

        /**
         * Only publish CRUD events for passsive workflows
         */
        String address = getPublishCrudEventsAddress();
        if (address != null) {
            // Build a new Event with CRUD Type
            nbiRequest.body.putString(AcsConstants.FIELD_NAME_ACS_CRUD_TYPE, crudType.name());

            // Send it
            vertx.eventBus().publish(address, nbiRequest.body);
        }
    }

}
