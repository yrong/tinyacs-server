package vertx2.acs.nbi.deviceop;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.VertxRedisUtils;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.model.*;
import vertx2.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.UUID;

/**
 * Project:  SXA-CC ACS NBI
 *
 * Device Operation Data Model.
 *
 * @author: ronyang
 */
public class DeviceOp extends CpeDeviceOp{
    private static final Logger log = LoggerFactory.getLogger(DeviceOp.class.getName());

    /**
     * Field Name Constants that are only applicable to NBI (i.e. not applicable to CPE server)
     */
    public static final String FIELD_NAME_GET_OPTIONS_STREAMING_DATA = "streamingData";
    public static final String FIELD_NAME_GET_OPTIONS_STREAMING_INTERVAL = "streamingInterval";
    public static final int DEFAULT_STREAMING_DATA_INTERVAL = 5;    // default streaming data interval is 5 seconds

    /**
     * Internal Enum for the Get Options
     */
    public static enum GetOptionsEnum {
        CachedDataOnly,
        LiveDataOnly,
        Both,
        Null
    }

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator mandatoryFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_OPERATION, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator optionalFields =
            new VertxJsonUtils.JsonFieldValidator(FIELD_NAME_GET_OPTIONS, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_OBJECT_NAME, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_PARAM_NAMES, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_PARAM_ATTRIBUTES, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_PARAM_VALUES, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_SERVICES, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_DYNAMIC_OBJECTS, VertxJsonUtils.JsonFieldType.JsonArray)
                    .append(FIELD_NAME_SERVICE_PLAN, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_CALLBACK_URL, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_CORRELATION_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_DIAG_TYPE, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_DIAG_PARAMS, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_GOLDEN_CONFIG_FILE, VertxJsonUtils.JsonFieldType.Boolean)
                    .append(FIELD_NAME_ASYNCHRONOUS, VertxJsonUtils.JsonFieldType.Boolean)
                    .append(FIELD_NAME_FILE_TYPE, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_FILE_STRUCT, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_FILE_URL, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_USERNAME, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_PASSWORD, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_EXEC_POLICY, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_INTERNAL_FILE_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_CSR_USERNAME, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsConstants.FIELD_NAME_CPE_ID, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_CPE_DB_OBJECT, VertxJsonUtils.JsonFieldType.JsonObject);

    public static final VertxJsonUtils.JsonFieldValidator getOptionsMandatoryFields =
            new VertxJsonUtils.JsonFieldValidator();

    public static final VertxJsonUtils.JsonFieldValidator getOptionsOptionalFields =
            new VertxJsonUtils.JsonFieldValidator(FIELD_NAME_GET_OPTIONS_LIVE, VertxJsonUtils.JsonFieldType.Boolean)
                    .append(FIELD_NAME_GET_OPTIONS_CACHED, VertxJsonUtils.JsonFieldType.Boolean);

    /**
     * Static Query Key used when querying CPE devices collection.
     *
     * Only the CPE "_id" and device id attributes are interesting to us.
     */
    private static final JsonObject DEFAULT_QUERY_KEYS_CACHED = new JsonObject()
            .putNumber("_id", 1)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_URL, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_USERNAME, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_PASSWORD, 1)
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1);

    private static final JsonObject DEFAULT_QUERY_KEYS_LIVE = new JsonObject()
            .putNumber("_id", 1)
            .putNumber(AcsConstants.FIELD_NAME_ORG_ID, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_URL, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_USERNAME, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_PASSWORD, 1)
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1)
            .putNumber(Cpe.DB_FIELD_NAME_LAST_INFORM_TIME, 1)
            .putNumber(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_INTERVAL, 1);

    /**
     * Non-Static Variables 
     */
    public Vertx vertx;
    public String cpeIdString = null;
    public JsonObject cpeDbObject = null;
    public JsonObject deviceOpJsonObject;
    public CpeDeviceOpTypeEnum operationType;
    public GetOptionsEnum getOptionsEnum = null;
    public int chunkIndex = 0;
    public long timeout = ExecPolicy.DEFAULT_DEVICE_OP_TIMEOUT;
    public int maxRetries = ExecPolicy.DEFAULT_DEVICE_OP_MAX_RETRIES;
    public int failedAttempts = 0;
    public long retryInterval = 0;
    public Long internalSn = null;
    public AcsNbiRequest nbiRequest;
    public JsonObject matcher = null;
    public JsonObject queryKeys = DEFAULT_QUERY_KEYS_LIVE;
    public Long callbackTimer = null;
    public JsonObject cachedData = null;
    public boolean bResponseSent = false;
    public String redisString = null;

    /**
     * Constructor by a DeviceOp JSON Object.
     *
     * @param vertx
     * @param deviceOpJsonObject
     * @param nbiRequest
     * @param internalDeviceOpSn
     * @param internalCallbackUrl
     */
    public DeviceOp(
            Vertx vertx,
            JsonObject deviceOpJsonObject,
            AcsNbiRequest nbiRequest,
            long internalDeviceOpSn,
            String internalCallbackUrl) throws SxaVertxException{
        this.vertx = vertx;
        this.deviceOpJsonObject = deviceOpJsonObject;
        this.nbiRequest = nbiRequest;
        this.internalSn = new Long(internalDeviceOpSn);

        /**
         * Validate all mandatory/optional field names/types
         */
        try {
            /**
             * Call the common validation method first
             */
            VertxJsonUtils.validateFields(deviceOpJsonObject, mandatoryFields, optionalFields);
        } catch (SxaVertxException ex) {
            throw new SxaVertxException("Invalid Device Op! Caught exception " + ex.getMessage());
        }

        /**
         * Further validate it based on operation type
         */
        try {
            operationType = getOperationType(deviceOpJsonObject);
            validateByOpType(deviceOpJsonObject, operationType);
        } catch (SxaVertxException e) {
            e.printStackTrace();
            throw new SxaVertxException("Invalidate Operation " + deviceOpJsonObject.getString(FIELD_NAME_OPERATION) + "!");
        }

        /**
         * Validate "getOptions"
         */
        JsonObject getOptions = deviceOpJsonObject.getObject(FIELD_NAME_GET_OPTIONS);
        if (getOptions != null) {
            try {
                //VertxJsonUtils.validateFields(getOptions, getOptionsMandatoryFields, getOptionsOptionalFields);
                boolean live = getOptions.getBoolean(FIELD_NAME_GET_OPTIONS_LIVE, false);
                boolean cached = getOptions.getBoolean(FIELD_NAME_GET_OPTIONS_CACHED, false);

                if (live == true) {
                    if (cached == true) {
                        getOptionsEnum = GetOptionsEnum.Both;
                    } else {
                        getOptionsEnum = GetOptionsEnum.LiveDataOnly;
                    }
                } else {
                    getOptionsEnum = GetOptionsEnum.CachedDataOnly;
                }
            } catch (Exception e) {
                throw new SxaVertxException("Invalid Device Op! Caught exception when validating \""
                        + FIELD_NAME_GET_OPTIONS + "\"" + e.getMessage());
            }
        }

        /**
         * Extract exec policy related attributes
         */
        JsonObject execPolicyJsonObject = deviceOpJsonObject.getObject(FIELD_NAME_EXEC_POLICY);
        if (execPolicyJsonObject != null) {
            ExecPolicy execPolicy = new ExecPolicy(execPolicyJsonObject);
            timeout = execPolicy.timeout;
            maxRetries = execPolicy.maxRetries;
            retryInterval = execPolicy.retryInterval;
        }

        /**
         * If no callback URL is specified by the client (i.e. "synchronous" op), pass the internal device
         * op SN to CPE server for internal callback purpose.
         */
        if (!deviceOpJsonObject.containsField(CpeDeviceOp.FIELD_NAME_CALLBACK_URL)) {
            // Internal Callback URl contains an internal Device Op SN
            deviceOpJsonObject.putString(CpeDeviceOp.FIELD_NAME_CALLBACK_URL, internalCallbackUrl);
            deviceOpJsonObject.putNumber(CpeDeviceOp.FIELD_NAME_INTERNAL_SN, internalSn);
        }

        /**
         * Extract CPE DB Object if any
         */
        cpeDbObject = deviceOpJsonObject.getObject(FIELD_NAME_CPE_DB_OBJECT);
        if (cpeDbObject == null) {
            /**
             * Get Match by CPE Identifier and Org Id
             */
            matcher = CpeIdentifier.getCpeMatcher(
                    deviceOpJsonObject.getString(AcsConstants.FIELD_NAME_ORG_ID),
                    deviceOpJsonObject.getObject(AcsConstants.FIELD_NAME_CPE_ID)
            );

            cpeIdString = matcher.encode();
        } else {
            cpeIdString = cpeDbObject.getString(AcsConstants.FIELD_NAME_ID);
        }

        /**
         * Determine Query Keys by "getOptions" and operation type
         */
        if (operationType == CpeDeviceOpTypeEnum.GetParameterValues ||
                operationType == CpeDeviceOpTypeEnum.GetParameterAttributes) {
            // Options for "GetXxx" Operations
            if (getOptionsEnum != GetOptionsEnum.LiveDataOnly) {
                // Cached data is needed.
                queryKeys = DEFAULT_QUERY_KEYS_CACHED.copy();
                JsonArray paramNames = deviceOpJsonObject.getArray(FIELD_NAME_PARAM_NAMES);
                for (int i = 0; i < paramNames.size(); i ++) {
                    String paramName = paramNames.get(i);

                    // truncate the trailing '.' if any
                    if (paramName.endsWith(".")) {
                        paramName = paramName.substring(0, paramName.length() - 1);
                    }

                    // add the parameter names into the query keys
                    if (operationType == CpeDeviceOpTypeEnum.GetParameterValues) {
                        queryKeys.putNumber(Cpe.DB_FIELD_NAME_PARAM_VALUES + "." + paramName, 1);
                    } else {
                        queryKeys.putNumber(Cpe.DB_FIELD_NAME_PARAM_ATTRIBUTES + "." + paramName, 1);
                    }
                }
            }
        }

        /**
         * Create an UUID
         */
        deviceOpJsonObject.putString("_id", UUID.randomUUID().toString());
    }

    /**
     * Set the Callback Timer id
     *
     * @param timerId
     */
    public void setCallbackTimer(long timerId) {
        callbackTimer = timerId;
    }

    /**
     * Store this device op into Redis Queue.
     *
     * @param redisClient
     */
    public void storeIntoRedisQueue(RedisClient redisClient) {
        /**
         * Store the new deviceOp into a Redis List which will be retrieved by the CPE Server.
         */
        redisString = deviceOpJsonObject.toString();
        VertxRedisUtils.rpush(
                redisClient,
                getDeviceOpsRedisListKeyByCpeId(cpeIdString),
                redisString,
                new Handler<Long>() {
                    @Override
                    public void handle(Long listLength) {
                        if (listLength > 0) {
                            /**
                             * After device op is stored into Redis, try send a connection-request to
                             * the CPE if needed.
                             */
                            if (log.isDebugEnabled()) {
                                log.debug("Successfully stored new deviceOp into Redis for CPE " + cpeIdString
                                        + ":\n" + deviceOpJsonObject.encodePrettily());
                            }
                        } else {
                            sendResponse(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                DeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                                "Failed to store new deviceOp for CPE " + cpeIdString + "!",
                                null);
                        }
                    }
                }
        );
    }

    /**
     * Remove from Redis Queue.
     *
     * @param redisClient
     */
    public void removeFromRedisQueue(RedisClient redisClient) {
        if (redisString != null) {
            VertxRedisUtils.lrem(
                    redisClient,
                    getDeviceOpsRedisListKeyByCpeId(cpeIdString),
                    redisString,
                    new Handler<Long>() {
                        @Override
                        public void handle(Long aLong) {
                            if (aLong == 1) {
                                log.debug("Successfully removed deviceOp " + internalSn + " from Redis (for CPE "
                                        + cpeIdString + ")");
                            }
                        }
                    }
            );
        }
    }

    /**
     * Send this device-op request to CPE Server directly via event bus.
     *
     * @param cpeServer
     * @param handler
     */
    public void sendToCpeServerViaEventBus(
            String cpeServer,
            Handler<AsyncResult<Message<JsonObject>>> handler) {
        vertx.eventBus().sendWithTimeout(
                AcsConstants.VERTX_ADDRESS_ACS_DEVICE_OP_REQUEST_PREFIX + cpeServer,
                new JsonObject()
                        .putString(AcsConstants.FIELD_NAME_ID, cpeIdString)
                        .putObject(DeviceOp.FIELD_NAME_DEVICE_OP, deviceOpJsonObject),
                timeout * 1000,
                handler
        );
    }

    /**
     * Send Response and also persist this device op to MongoDB.
     *
     * @param responseStatus    Standard (HTTP) Response Status Code
     * @param state             State string
     * @param error             Optional Error String
     * @param result            Optional Result
     */
    public void sendResponse(final HttpResponseStatus responseStatus, String state, String error, JsonObject result) {
        // Have we already sent response for this device op request?
        if (bResponseSent == true) {
            log.info(cpeIdString  + ": deviceOp response already sent.");
            return;
        }
        bResponseSent = true;

        deviceOpJsonObject.putString(FIELD_NAME_STATE, state);
        if (error != null) {
            log.error(cpeIdString + ": internalSN " + internalSn + ": " + error + "!");
            deviceOpJsonObject.putString(FIELD_NAME_ERROR, error);
        }
        if (result != null) {
            deviceOpJsonObject.putObject(FIELD_NAME_RESULT, result);
        }

        // Cancel timer if any
        if (callbackTimer != null) {
            vertx.cancelTimer(callbackTimer);
        }

        /**
         * Add download URL/Credentials if uploading log file succeeded
         */
        AcsFileType fileType = AcsFileType.getAcsFileTypeByDeviceOpFileType(
                deviceOpJsonObject.getString(FIELD_NAME_FILE_TYPE));
        if (operationType.equals(CpeDeviceOpTypeEnum.Upload) &&
                AcsFileType.LogFile.equals(fileType) &&
                HttpResponseStatus.OK.equals(responseStatus)) {
            result.putString(
                    AcsFile.FIELD_NAME_DOWNLOAD_URL,
                    AcsFile.getDownloadUrl(result.getString(CpeDeviceOp.FIELD_NAME_INTERNAL_FILE_ID))
            ).putString(
                    AcsFile.FIELD_NAME_USERNAME,
                    result.getString(AcsFile.FIELD_NAME_USERNAME)
            ).putString(
                    AcsFile.FIELD_NAME_PASSWORD,
                    result.getString(AcsFile.FIELD_NAME_PASSWORD)
            );
        }

        /**
         * HTTP Response Body
         */
        if (error != null) {
            nbiRequest.sendResponse(responseStatus,
                    new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, error));
        } else if (result != null) {
            nbiRequest.sendResponse(responseStatus, result);
        } else {
            nbiRequest.sendResponse(responseStatus);
        }

        /**
         * Save Device Event if needed
         */
        EventTypeEnum eventType = null;
        EventSourceEnum eventSource = EventSourceEnum.System;
        JsonObject eventDetails = null;
        switch (operationType) {
            case Reboot:
                eventType = error == null? EventTypeEnum.Reboot : EventTypeEnum.RebootFailure;
                eventSource = EventSourceEnum.Operator;
                break;

            case FactoryReset:
                eventType = error == null? EventTypeEnum.FactoryReset : EventTypeEnum.FactoryResetFailure;
                eventSource = EventSourceEnum.Operator;
                break;

            case Upload:
                if (AcsFileType.LogFile.equals(fileType)) {
                    // Do not save event for uploading log files
                    eventType = null;
                } else {
                    // Uploading Config Files
                    String csrUser = deviceOpJsonObject.getString(FIELD_NAME_CSR_USERNAME);
                    if (csrUser != null) {
                        eventSource = EventSourceEnum.Operator;
                        if (error == null) {
                            eventType = EventTypeEnum.ManualConfigFileBackup;
                        } else {
                            eventType = EventTypeEnum.ManualConfigFileBackupFailure;
                        }
                    } else {
                        if (error == null) {
                            eventType = EventTypeEnum.AutoConfigFileBackup;
                        } else {
                            eventType = EventTypeEnum.AutoConfigFileBackupFailure;
                        }
                    }
                }
                break;

            case Download:
                if (!deviceOpJsonObject.containsField(CpeDeviceOp.FIELD_NAME_FILE_STRUCT)) {
                    eventType = error == null? EventTypeEnum.Restore : EventTypeEnum.RestoreFailure;
                } else {
                    /**
                     * Workflow triggered. Not handled in here
                     */
                }
                break;
        }
        if (eventType != null) {
            Event.saveEvent(
                    vertx.eventBus(),
                    cpeDbObject.getString(AcsConstants.FIELD_NAME_ORG_ID),
                    cpeDbObject.getString(Cpe.DB_FIELD_NAME_SN),
                    eventType,
                    eventSource,
                    eventDetails
            );
        }

        /**
         * Save it
         */
        //persistDeviceOp(vertx.eventBus(), deviceOpJsonObject);

        /**
         * Also remove from Redis if stored it before
         */
    }
}
