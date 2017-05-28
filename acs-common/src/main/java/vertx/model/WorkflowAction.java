package vertx.model;

import vertx.VertxException;
import vertx.VertxJsonUtils;
import vertx.CcException;
import vertx.cache.ConfigurationProfileCache;
import vertx.util.AcsApiUtils;
import vertx.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * Workflow Action Struct Model Definition.
 *
 * See http://wiki.calix.local/display/Compass/CCNG+-+ACS+API#CCNG-ACSAPI-ACSAPI-"WorkFlowAction"Struct for
 * more details.
 *
 * @author: ronyang
 */
public class WorkflowAction {
    private static final Logger log = LoggerFactory.getLogger(WorkflowAction.class.getName());

    /**
     * Field Names
     */
    public static final String FIELD_NAME_ACTION_TYPE = "actionType";
    public static final String FIELD_NAME_PROFILE_ID = "profileId";
    public static final String FIELD_NAME_PROFILE_NAME = "profileName";
    public static final String FIELD_NAME_FILE_ID = "fileId";
    public static final String FIELD_NAME_FILE_STRUCT = "fileStruct";
    public static final String FIELD_NAME_PARAMETER_NAMES = "parameterNames";
    public static final String FIELD_NAME_EXPECTED_PARAMETER_VALUES = "expectedParameterValues";
    public static final String FIELD_NAME_PARAMETER_VALUES = ConfigurationProfile.FIELD_NAME_PARAMETER_VALUES;
    public static final String FIELD_NAME_SERVICES = ConfigurationProfile.FIELD_NAME_SERVICES;
    public static final String FIELD_NAME_DYNAMIC_OBJECTS = ConfigurationProfile.FIELD_NAME_DYNAMIC_OBJECTS;
    public static final String FIELD_NAME_DELAY = "delay";

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator mandatoryFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_ACTION_TYPE, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator optionalFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_PROFILE_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_PROFILE_NAME, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_FILE_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_FILE_STRUCT, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_PARAMETER_NAMES, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_EXPECTED_PARAMETER_VALUES, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_PARAMETER_VALUES, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_SERVICES, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_DYNAMIC_OBJECTS, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_DELAY, VertxJsonUtils.JsonFieldType.Integer);

    /**
     * Static Exceptions
     */
    public static final CcException INVALID_ACTION_TYPE = new CcException("Invalid Workflow Action Type!");
    public static final CcException MISSING_ACTION_PARAM = new CcException("Missing Action Specific Parameter!");
    public static final CcException INVALID_ACTION_PARAM = new CcException("Invalid Workflow Action Parameter!");
    public static final CcException INVALID_PROFILE_ID = new CcException("Invalid Profile Id!");
    public static final CcException INTERNAL_ERROR_MISSING_PROFILE_CACHE =
            new CcException("Internal Error (profile cache is null)!");

    /**
     * POJO Attributes
     */
    // Raw JSON Object
    public JsonObject  rawJsonObject;

    // Action Type
    public WorkflowActionEnum actionEnum = WorkflowActionEnum.INVALID;

    // For "Apply Xxx Profile":
    public String profileId;
    public String profileName;
    // For "Set Parameter Values" and "Apply Xxx Profile":
    public JsonObject paramValues;
    public JsonArray services;
    public JsonArray dynamicObjects;
    // For "Get Parameter Values":
    public JsonObject expectedParamValues;
    public JsonArray  paramNames;
    // Fro "Delay":
    public int delay;
    // For download:
    public String fileId;
    public JsonObject file;

    /**
     * Default Empty Constructor.
     */
    public WorkflowAction () {
    }

    /**
     * Constructor by a raw JSON Object and a ConfigurationProfileCache.
     *
     * @param jsonObject
     *
     * @throws vertx.VertxException
     */
    public WorkflowAction (JsonObject jsonObject) throws VertxException {
        this(jsonObject, null);
    }

    /**
     * Constructor by a raw JSON Object and a ConfigurationProfileCache.
     *
     * @param jsonObject
     * @param configurationProfileCache
     *
     * @throws vertx.VertxException
     */
    public WorkflowAction (
            JsonObject jsonObject,
            ConfigurationProfileCache configurationProfileCache
    ) throws VertxException {
        this.rawJsonObject = jsonObject;
        
        /**
         * Call standard field validator util method.
         */
        VertxJsonUtils.validateFields(jsonObject, mandatoryFields, optionalFields);

        /**
         * Check Action Type and convert to Action Type Enum
         */
        actionEnum = WorkflowActionEnum.getWorkflowActionEnum(jsonObject.getString(FIELD_NAME_ACTION_TYPE));
        switch (actionEnum) {
            case INVALID:
                throw INVALID_ACTION_TYPE;

            case SET_PARAMETER_VALUES:
                paramValues = jsonObject.getObject(FIELD_NAME_PARAMETER_VALUES);
                if (paramValues == null) {
                    log.error("Missing " + FIELD_NAME_PARAMETER_VALUES + "!");
                    throw MISSING_ACTION_PARAM;
                }
                break;

            case GET_PARAMETER_VALUES:
                paramNames = jsonObject.getArray(FIELD_NAME_PARAMETER_NAMES);
                expectedParamValues = jsonObject.getObject(FIELD_NAME_EXPECTED_PARAMETER_VALUES);
                if (paramNames == null || expectedParamValues == null) {

                    log.error("Missing " + FIELD_NAME_PARAMETER_NAMES + " or "
                            + FIELD_NAME_EXPECTED_PARAMETER_VALUES + "!");
                    throw MISSING_ACTION_PARAM;
                }
                break;

            case DOWNLOAD_CONFIG_FILE:
            case DOWNLOAD_FW_IMAGE:
                fileId = jsonObject.getString(FIELD_NAME_FILE_ID);
                if (fileId == null) {
                    log.error("Missing " + FIELD_NAME_FILE_ID + "!");
                    throw MISSING_ACTION_PARAM;
                }
                file = jsonObject.getObject(FIELD_NAME_FILE_STRUCT);
                break;

            case DELAY:
                if (jsonObject.getInteger(FIELD_NAME_DELAY) == null) {
                    log.error("Missing " + FIELD_NAME_DELAY + "!");
                    throw MISSING_ACTION_PARAM;
                }
                delay = jsonObject.getInteger(FIELD_NAME_DELAY);
                if (delay < 0) {
                    log.error("Invalid " + FIELD_NAME_DELAY + "! (" + delay + ")");
                    throw INVALID_ACTION_PARAM;
                }
                break;

            case APPLY_CONFIG_PROFILE:
                profileId = jsonObject.getString(FIELD_NAME_PROFILE_ID);
                if (profileId == null) {
                    log.error("Missing " + FIELD_NAME_PROFILE_ID + "!");
                    throw MISSING_ACTION_PARAM;
                }

                /**
                 * Get Profile Name from cache if needed
                 */
                profileName = jsonObject.getString(FIELD_NAME_PROFILE_NAME);
                if (profileName == null && configurationProfileCache != null) {
                    JsonObject profile = (JsonObject) configurationProfileCache.hashMap.get(profileId);
                    if (profile != null) {
                        profileName = profile.getString(AcsConstants.FIELD_NAME_NAME);
                        jsonObject.putString(FIELD_NAME_PROFILE_NAME, profileName);
                    }
                }

                /**
                 * Traditional Parameter Values
                 */
                paramValues = jsonObject.getObject(FIELD_NAME_PARAMETER_VALUES);
                if (paramValues == null && configurationProfileCache != null) {
                    JsonObject profile = (JsonObject) configurationProfileCache.hashMap.get(profileId);
                    if (profile == null) {
                        log.error("Invalid Profile Id " + profileId + "!");
                        throw INVALID_PROFILE_ID;
                    }

                    paramValues = profile.getObject(ConfigurationProfile.FIELD_NAME_PARAMETER_VALUES);
                    if (paramValues != null) {
                        jsonObject.putObject(FIELD_NAME_PARAMETER_VALUES, paramValues);
                    }
                }

                /**
                 * Services
                 */
                services = jsonObject.getArray(FIELD_NAME_SERVICES);
                if (services == null && configurationProfileCache != null) {
                    JsonObject profile = (JsonObject) configurationProfileCache.hashMap.get(profileId);
                    if (profile == null) {
                        log.error("Invalid Profile Id " + profileId + "!");
                        throw INVALID_PROFILE_ID;
                    }

                    services = profile.getArray(ConfigurationProfile.FIELD_NAME_SERVICES);
                    if (services != null) {
                        jsonObject.putArray(FIELD_NAME_SERVICES, services);
                    }
                }

                /**
                 * Dynamic Objects (for example "DNS Host Mapping" objects)
                 */
                dynamicObjects = jsonObject.getArray(FIELD_NAME_DYNAMIC_OBJECTS);
                if (dynamicObjects == null && configurationProfileCache != null) {
                    JsonObject profile = (JsonObject) configurationProfileCache.hashMap.get(profileId);
                    if (profile == null) {
                        log.error("Invalid Profile Id " + profileId + "!");
                        throw INVALID_PROFILE_ID;
                    }

                    dynamicObjects = profile.getArray(ConfigurationProfile.FIELD_NAME_DYNAMIC_OBJECTS);
                    if (dynamicObjects != null) {
                        jsonObject.putArray(FIELD_NAME_DYNAMIC_OBJECTS, dynamicObjects);
                    }
                }
                break;

            case APPLY_PERFORMANCE_PROFILE:
            case APPLY_SERVICE_PROFILE:
            case APPLY_NOTIFICATION_PROFILE:
                log.error("Action " + actionEnum.name() + " is not supported right now!");
                throw INVALID_ACTION_TYPE;
        }
    }

    /**
     * For GetParameterValues Action, always get the live data
     */
    public static final JsonObject GET_LIVE_DATA =
            new JsonObject().putBoolean(CpeDeviceOp.FIELD_NAME_GET_OPTIONS_LIVE, true);

    /**
     * Convert this action into a device-op API request, send the request to ACS API
     * via Vert.x event bus.
     *
     * If the next action is specified, do the next action if this action succeeds.
     *
     * @param vertx
     * @param cpeDbObject
     * @param correlationId
     * @param action
     * @param resultHandler
     */
    private static final JsonObject EXEC_POLICY_DOWNLOAD_TIMEOUT =
            new JsonObject().putNumber(ExecPolicy.FIELD_NAME_TIMEOUT,
                    ExecPolicy.DEFAULT_DOWNLOAD_TIMEOUT);
    public static void doAction(
            Vertx vertx,
            JsonObject cpeDbObject,
            String correlationId,
            WorkflowAction action,
            final Handler<AsyncResult<Message<JsonObject>>> resultHandler
    ) {
        long timeout = ExecPolicy.DEFAULT_DEVICE_OP_TIMEOUT;

        /**
         * Convert action into API request
         */
        JsonObject requestBody = new JsonObject().putObject(CpeDeviceOp.FIELD_NAME_CPE_DB_OBJECT, cpeDbObject);

        if (correlationId != null) {
            requestBody.putString(CpeDeviceOp.FIELD_NAME_CORRELATION_ID, correlationId);
        }
        switch (action.actionEnum) {
            case APPLY_CONFIG_PROFILE:
            case SET_PARAMETER_VALUES:
                if (action.paramValues != null) {
                    requestBody.putObject(CpeDeviceOp.FIELD_NAME_PARAM_VALUES, action.paramValues);
                }
                if (action.services != null) {
                    requestBody.putArray(CpeDeviceOp.FIELD_NAME_SERVICES, action.services);
                }
                if (action.dynamicObjects != null) {
                    requestBody.putArray(CpeDeviceOp.FIELD_NAME_DYNAMIC_OBJECTS, action.dynamicObjects);
                }
                requestBody.putString(CpeDeviceOp.FIELD_NAME_OPERATION,
                        CpeDeviceOpTypeEnum.SetParameterValues.name());
                break;

            case GET_PARAMETER_VALUES:
                requestBody.putArray(CpeDeviceOp.FIELD_NAME_PARAM_NAMES, action.paramNames);
                requestBody.putObject(CpeDeviceOp.FIELD_NAME_GET_OPTIONS, GET_LIVE_DATA);
                requestBody.putString(CpeDeviceOp.FIELD_NAME_OPERATION,
                        CpeDeviceOpTypeEnum.GetParameterValues.name());
                break;

            case DELAY:
                vertx.setTimer(
                        action.delay * 1000,
                        new Handler<Long>() {

                            public void handle(Long aLong) {
                                resultHandler.handle(null);
                            }
                        }
                );
                return;

            case DOWNLOAD_FW_IMAGE:
            case DOWNLOAD_CONFIG_FILE:
                if (action.file == null) {
                    log.error("Action " + action.actionEnum.name() + " had no file struct!");
                    requestBody.putString(CpeDeviceOp.FIELD_NAME_ID, action.fileId);
                } else {
                    // Add the entire File Struct
                    requestBody.putObject(CpeDeviceOp.FIELD_NAME_FILE_STRUCT, action.file);

                    if (AcsFileType.ConfigFile.typeString.equals(action.file.getString(AcsFile.FIELD_NAME_TYPE))) {
                        // Indicate this is a golden config file
                        requestBody.putBoolean(CpeDeviceOp.FIELD_NAME_GOLDEN_CONFIG_FILE, true);
                    }
                }

                // Timeout
                timeout = ExecPolicy.DEFAULT_DOWNLOAD_TIMEOUT;
                requestBody.putObject(CpeDeviceOp.FIELD_NAME_EXEC_POLICY, EXEC_POLICY_DOWNLOAD_TIMEOUT);

                // Send the request either way
                requestBody.putString(CpeDeviceOp.FIELD_NAME_OPERATION, CpeDeviceOpTypeEnum.Download.name());
                break;

            case APPLY_PERFORMANCE_PROFILE:
            case APPLY_SERVICE_PROFILE:
            case APPLY_NOTIFICATION_PROFILE:
            default:
                /**
                 * TODO
                 */
                break;
        }

        /**
         * Send the device-op request
         */
        AcsApiUtils.sendApiRequest(
                vertx.eventBus(),
                AcsConstants.ACS_API_SERVICE_DEVICE_OP,
                cpeDbObject.getString(AcsConstants.FIELD_NAME_ORG_ID),
                AcsApiCrudTypeEnum.Create,
                requestBody,
                timeout + 10000,    // wait 10 extra seconds to get meaningful error messages
                resultHandler
        );
    }

    /**
     * Make a Copy of this WorkflowAction.
     * @return
     */
    public WorkflowAction copy() {
        WorkflowAction dest = null;
        try {
            dest = new WorkflowAction(rawJsonObject, null);
        } catch (VertxException e) {
            e.printStackTrace();
        }

        return dest;
    }
}
