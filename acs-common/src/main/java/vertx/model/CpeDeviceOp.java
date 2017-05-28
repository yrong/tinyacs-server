package vertx.model;

import vertx.VertxException;
import vertx.VertxJsonUtils;
import vertx.CcException;
import vertx.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Project:  SXA CC
 *
 * CPE Device Operation Constant Definitions and Utils
 *
 * @author: ronyang
 */
public class CpeDeviceOp {
    private static final Logger log = LoggerFactory.getLogger(CpeDeviceOp.class.getName());

    /**
     * MongoDB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "acs-device-ops";

    /**
     * Device Op JSON Field Names
     */
    public static final String FIELD_NAME_DEVICE_OP = "deviceOp";
    public static final String FIELD_NAME_ID = AcsConstants.FIELD_NAME_ID;
    public static final String FIELD_NAME_DESCRIPTION = AcsConstants.FIELD_NAME_DESCRIPTION;
    public static final String FIELD_NAME_OPERATION = "operation";
    public static final String FIELD_NAME_CPE_DB_OBJECT = "cpeDbObject";    // internal use only
    public static final String FIELD_NAME_CORRELATION_ID = "correlationId";
    public static final String FIELD_NAME_CALLBACK_URL = "callbackUrl";
    public static final String FIELD_NAME_EXEC_POLICY = "execPolicy";
    public static final String FIELD_NAME_OBJECT_NAME = "objectName";
    public static final String FIELD_NAME_PARAM_VALUES = AcsConstants.FIELD_NAME_PARAM_VALUES;
    public static final String FIELD_NAME_DYNAMIC_PATH_ALLOW_OVERWRITE = "allowOverwrite";
    public static final String FIELD_NAME_SERVICES = ConfigurationProfile.FIELD_NAME_SERVICES;
    public static final String FIELD_NAME_DYNAMIC_OBJECTS = ConfigurationProfile.FIELD_NAME_DYNAMIC_OBJECTS;
    public static final String FIELD_NAME_SERVICE_PLAN = "servicePlan";
    public static final String FIELD_NAME_STATUS_CODE = AcsConstants.FIELD_NAME_STATUS_CODE;
    public static final String FIELD_NAME_PARAM_NAMES = "parameterNames";
    public static final String FIELD_NAME_PARAM_ATTRIBUTES = "parameterAttributes";
    public static final String FIELD_NAME_STATE = "state";
    public static final String FIELD_NAME_RESULT = "result";
    public static final String FIELD_NAME_NEW_OBJECT_INDEX = "newObjectIndex";
    public static final String FIELD_NAME_SUCCEEDED = "succeeded";
    public static final String FIELD_NAME_ERROR = "error";
    public static final String FIELD_NAME_CONN_REQ_FAILED = "connReqFailed";
    public static final String FIELD_NAME_FAULT = "fault";
    public static final String FIELD_NAME_FAULT_CODE = "code";
    public static final String FIELD_NAME_FAULT_MESSAGE = "message";
    public static final String FIELD_NAME_INVALID_PARAM_NAMES = "invalidParameterNames";
    public static final String FIELD_NAME_INTERNAL_SN = "internalSn";
    public static final String FIELD_NAME_FILE_TYPE = "fileType";
    public static final String FIELD_NAME_FILE_STRUCT = "fileStruct";
    public static final String FIELD_NAME_INTERNAL_FILE_ID = "internalFileId";
    public static final String FIELD_NAME_CSR_USERNAME = "csrUsername";
    public static final String FIELD_NAME_FILE_URL = "url";
    public static final String FIELD_NAME_USERNAME = AcsConstants.FIELD_NAME_USERNAME;
    public static final String FIELD_NAME_PASSWORD = AcsConstants.FIELD_NAME_PASSWORD;
    public static final String FIELD_NAME_VERSION = AcsFile.FIELD_NAME_VERSION;
    public static final String FIELD_NAME_DIAG_TYPE = "diagType";
    public static final String FIELD_NAME_DIAG_PARAMS = "diagParameters";
    public static final String FIELD_NAME_GOLDEN_CONFIG_FILE = "bGoldenConfigFile";
    public static final String FIELD_NAME_ASYNCHRONOUS = "asynchronous";
    public static final String FIELD_NAME_GET_OPTIONS = "getOptions";
    public static final String FIELD_NAME_CHUNKS_RETURNED = "chunks";
    public static final String FIELD_NAME_GET_OPTIONS_LIVE = "liveData";
    public static final String FIELD_NAME_GET_OPTIONS_CACHED = "cachedData";


    /**
     * Field Name Arrays that mandatory by one or more device op type
     */
    public static final String[] FIELD_NAME_ARRAY_PARAM_NAMES = new String[] {
            FIELD_NAME_PARAM_NAMES
    };
    public static final String[] FIELD_NAME_ARRAY_PARAM_VALUES = new String[] {
            FIELD_NAME_PARAM_VALUES
    };
    public static final String[] FIELD_NAME_ARRAY_PARAM_ATTRIBUTES = new String[] {
            FIELD_NAME_PARAM_ATTRIBUTES
    };
    public static final String[] FIELD_NAME_ARRAY_OBJECT_NAME = new String[] {
            FIELD_NAME_OBJECT_NAME
    };
    public static final String[] FIELD_NAME_ARRAY_FILE_DOWNLOAD = new String[] {
            FIELD_NAME_FILE_TYPE
    };
    public static final String[] FIELD_NAME_ARRAY_FILE_UPLOAD = new String[] {
            FIELD_NAME_FILE_TYPE
    };
    public static final String[] FIELD_NAME_ARRAY_DIAG = new String[] {
            FIELD_NAME_DIAG_TYPE,
            FIELD_NAME_DIAG_PARAMS
    };
    public static final Map<CpeDeviceOpTypeEnum, String[]> MANDATORY_ARG_NAMES_BY_OP_TYPE =
            initOpTypeToArgNameArrayHashMap();

    /**
     * Device Op State Constant Strings
     */
    public static final String CPE_DEVICE_OP_STATE_INVALID_REQUEST = "Invalid-Request";
    public static final String CPE_DEVICE_OP_STATE_PENDING = "Pending";
    public static final String CPE_DEVICE_OP_STATE_INTERNAL_ERROR = "Internal-Server-Error";
    public static final String CPE_DEVICE_OP_STATE_IN_PROGRESS = "In-Progress";
    public static final String CPE_DEVICE_OP_STATE_SUCCEEDED = "Succeeded";
    public static final String CPE_DEVICE_OP_STATE_FAILED = "Failed";

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator mandatoryFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_OPERATION, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator optionalFields =
            new VertxJsonUtils.JsonFieldValidator(FIELD_NAME_OBJECT_NAME, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsConstants.FIELD_NAME_CPE_ID, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_CPE_DB_OBJECT, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_CORRELATION_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_PARAM_NAMES, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_PARAM_ATTRIBUTES, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_PARAM_VALUES, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_DYNAMIC_PATH_ALLOW_OVERWRITE, VertxJsonUtils.JsonFieldType.Boolean)
                    .append(FIELD_NAME_SERVICES, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_DYNAMIC_OBJECTS, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_SERVICE_PLAN, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_CALLBACK_URL, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_STATE, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_RESULT, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_GET_OPTIONS, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_CHUNKS_RETURNED, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_FILE_TYPE, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_FILE_URL, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_FILE_STRUCT, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_USERNAME, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_PASSWORD, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_EXEC_POLICY, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_DIAG_TYPE, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_DIAG_PARAMS, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_GOLDEN_CONFIG_FILE, VertxJsonUtils.JsonFieldType.Boolean)
                    .append(FIELD_NAME_ASYNCHRONOUS, VertxJsonUtils.JsonFieldType.Boolean)
                    .append(FIELD_NAME_STATUS_CODE, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_INTERNAL_FILE_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_CSR_USERNAME, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_VERSION, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_INTERNAL_SN, VertxJsonUtils.JsonFieldType.Integer);

    /**
     * Initialize <CpeDeviceOpTypeEnum, Arg_Names_Array> HashMap
     */
    public static Map<CpeDeviceOpTypeEnum, String[]> initOpTypeToArgNameArrayHashMap() {
        Map<CpeDeviceOpTypeEnum, String[]>  map = new HashMap<>();
        map.put(CpeDeviceOpTypeEnum.GetParameterNames, FIELD_NAME_ARRAY_PARAM_NAMES);
        map.put(CpeDeviceOpTypeEnum.GetParameterValues, FIELD_NAME_ARRAY_PARAM_NAMES);
        map.put(CpeDeviceOpTypeEnum.SetParameterValues, null);
        map.put(CpeDeviceOpTypeEnum.SetParameterAttributes, FIELD_NAME_ARRAY_PARAM_ATTRIBUTES);
        map.put(CpeDeviceOpTypeEnum.GetParameterAttributes, FIELD_NAME_ARRAY_PARAM_NAMES);
        map.put(CpeDeviceOpTypeEnum.AddObject, FIELD_NAME_ARRAY_OBJECT_NAME);
        map.put(CpeDeviceOpTypeEnum.DeleteObject, FIELD_NAME_ARRAY_OBJECT_NAME);
        map.put(CpeDeviceOpTypeEnum.Download, null);
        map.put(CpeDeviceOpTypeEnum.Upload, FIELD_NAME_ARRAY_FILE_UPLOAD);
        map.put(CpeDeviceOpTypeEnum.Reboot, null);
        map.put(CpeDeviceOpTypeEnum.FactoryReset, null);
        map.put(CpeDeviceOpTypeEnum.Diagnostics, FIELD_NAME_ARRAY_DIAG);

        return map;
    }

    /**
     * Validate a given Device Op JSON Object.
     *
     * @param deviceOp

     * @return  true if the Device Op JSON Object is valid, or false.
     */
    public static boolean isValid(JsonObject deviceOp) {
        try {
            /**
             * Call the common validation method first
             */
            VertxJsonUtils.validateFields(deviceOp, mandatoryFields, optionalFields);
        } catch (VertxException ex) {
            log.error("Invalid Device Op!\n" + deviceOp.encodePrettily());
            return false;
        }

        /**
         * Further validate it based on operation type
         */
        try {
            validateByOpType(deviceOp, getOperationType(deviceOp));
        } catch (CcException e) {
            log.error("Invalidate Operation " + deviceOp.getString(FIELD_NAME_OPERATION));
            return false;
        }

        return true;
    }

    /**
     * Extract the device op type from a JsonDBObject.
     *
     * @param deviceOp
     * @return
     * @throws Exception
     */
    public static CpeDeviceOpTypeEnum getOperationType(JsonObject deviceOp) throws CcException {
        return getOperationType(deviceOp.getString(FIELD_NAME_OPERATION));
    }

    /**
     * Get the device op type enum from a string.
     *
     * @param operationName
     * @return
     * @throws Exception
     */
    public static CpeDeviceOpTypeEnum getOperationType(String operationName) throws CcException {
        CpeDeviceOpTypeEnum operationType = CpeDeviceOpTypeEnum.getDeviceOpTypeEnumByString(operationName);
        if ((operationType != null)) {
            return operationType;
        } else {
            throw new CcException("Invalid Operation Type " + operationName);
        }
    }

    /**
     * Further Validate by Operation Type
     */
    public static void validateByOpType(JsonObject deviceOpJsonObject, CpeDeviceOpTypeEnum operationType)
        throws CcException {
        /**
         * Get a list of all the field names required for this specific op type
         */
        String[] argNames = MANDATORY_ARG_NAMES_BY_OP_TYPE.get(operationType);

        /**
         * Make sure all the required fields are present
         */
        if (argNames != null) {
            for (String arg : argNames) {
                if (!deviceOpJsonObject.containsField(arg)) {
                    throw new CcException("Missing mandatory field " + arg + "!");
                }
            }
        }
    }

    /**
     * Private Util to do the actual JSON Object -> Array conversion.
     * @param src
     * @param dest
     * @param newFieldName
     * @return
     */
    private static JsonArray jsonObjToArray(JsonObject src, JsonArray dest, String newFieldName) {
        if (dest == null) {
            dest = new JsonArray();
        }

        for (String fieldName : src.getFieldNames()) {
            if (src.getField(fieldName) instanceof JsonObject) {
                // The field is a JSON Object, dig in
                jsonObjToArray(src.getObject(fieldName), dest, newFieldName);
            } else {
                // The field is a parameter value or attribute
                dest.addObject(new JsonObject()
                        .putString("name", fieldName)
                        .putString(newFieldName, src.getField(fieldName).toString())
                );
            }
        }

        return dest;
    }

    /**
     * Save the Device Op to MongoDB.
     *
     * @param deviceOp  The raw/original device op JSON Object
     * @param state     State of the device op
     * @param result    Result of the device op
     */
    public static void persistDeviceOp(EventBus eventBus, JsonObject deviceOp, String state, JsonObject result) {
        persistDeviceOp(
                eventBus,
                deviceOp.putString(FIELD_NAME_STATE, state)
                        .putObject(FIELD_NAME_RESULT, result)
        );
    }


    /**
     * Save the Device Op to MongoDB.
     *
     * @param deviceOp  The Device Op JSON Object may already have state/result fields.
     */
    public static void persistDeviceOp(EventBus eventBus, JsonObject deviceOp) {
        /*
        Skip persisting device ops for now.
        try {
            VertxMongoUtils.save(eventBus, DB_COLLECTION_NAME, deviceOp, null);
        } catch (VertxException e) {
            e.printStackTrace();
        }
        */
    }

    /**
     * Get the Device Ops Redis List Key by CPE ID String.
     *
     * @param cpeId
     */
    public static String getDeviceOpsRedisListKeyByCpeId(String cpeId) {
        return AcsConstants.REDIS_KEY_DEVICE_OP_PREFIX + cpeId;
    }

    /**
     * Get Device Op Timeout from a Device Op JSON Object.
     * @param deviceOp
     * @return
     */
    public static int getTimeout(JsonObject deviceOp) {
        JsonObject execPolicyJsonObject = deviceOp.getObject(FIELD_NAME_EXEC_POLICY);
        if (execPolicyJsonObject != null) {
            return execPolicyJsonObject.getInteger(ExecPolicy.FIELD_NAME_TIMEOUT, ExecPolicy.DEFAULT_DEVICE_OP_TIMEOUT);
        } else {
            // Get default timeout by op type
            try {
                CpeDeviceOpTypeEnum opType = getOperationType(deviceOp);

                switch (opType) {
                    case Download:
                    case Reboot:
                    case FactoryReset:
                        return ExecPolicy.DEFAULT_DOWNLOAD_TIMEOUT;
                }
            } catch (CcException e) {
                return ExecPolicy.DEFAULT_DEVICE_OP_TIMEOUT;
            }
        }

        return ExecPolicy.DEFAULT_DEVICE_OP_TIMEOUT;
    }
}
