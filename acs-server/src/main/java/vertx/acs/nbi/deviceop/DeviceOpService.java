package vertx.acs.nbi.deviceop;

import vertx.VertxException;
import vertx.VertxConstants;
import vertx.VertxMongoUtils;
import vertx.VertxUtils;
import vertx.acs.nbi.AbstractAcNbiCrudService;
import vertx.acs.nbi.model.AcsNbiRequest;
import vertx.connreq.ConnectionRequestConstants;
import vertx.connreq.ConnectionRequestFsm;
import vertx.model.*;
import vertx.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.java.redis.RedisClient;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.impl.JsonObjectMessage;
import io.vertx.core.json.JsonObject;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Project:  cwmp ACS API
 *
 * Device Operation Web Service Implementation.
 *
 * @author: ronyang
 */
public class DeviceOpService extends AbstractAcNbiCrudService {
    /**
     * Event Bus Address for internal callbacks (used by "synchronous" device ops)
     */
    private final String internalCallbackAddress =
            VertxUtils.getPublicVertxEventBusAddress(AcsConstants.VERTX_ADDRESS_ACS_API_CALLBACK + "." + getServiceName());
    private final String internalCallbackUrl =
            VertxUtils.getVertxEventBusUrl(AcsConstants.VERTX_ADDRESS_ACS_API_CALLBACK + "." + getServiceName());


    // Async Redis Client Instance
    public RedisClient redisClient;

    /**
     * Internal Device Op SN (only applicable to this vertice)
     */
    long internalDeviceOpSn = 0;

    /**
     * An internal hash map that keeps track of the mapping of device-op-sn --> AcsNbiRequest
     */
    public Map<Long, AcsNbiRequest> mapDeviceOpToNbiRequest = new HashMap<>();

    /**
     * Start the service
     */
    @Override
    public void start(Vertx vertx) {
        super.start(vertx);

        /**
         * Register API Callback Handler
         */
        log.info("Registering internal callback handler for vertx address " + internalCallbackAddress);
        vertx.eventBus().registerHandler(internalCallbackAddress, internalCallbackHandler);

        /**
         * Initialize Redis Client
         */
        redisClient = new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);
    }

    /**
     * Stop the service
     */
    @Override
    public void stop(Vertx vertx) {
        super.stop(vertx);

        /**
         * Un-Register API Callback Handler
         */
        log.info("Un-Registering internal callback handler for vertx address " + internalCallbackAddress);
        vertx.eventBus().unregisterHandler(internalCallbackAddress, internalCallbackHandler);
    }

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_DEVICE_OP;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return CpeDeviceOp.DB_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     *
     * Return null to always allow new device ops to be created even with duplicated data.
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
     * Allow Bulk Deleting device operations.
     */
    @Override
    public boolean bAllowBulkDelete() {
        return true;
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
     * @throws vertx.VertxException
     */
    public boolean validate(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
        if (crudType == AcsApiCrudTypeEnum.Create) {
            /**
             * Try to convert to a DeviceOp POJO
             */
            DeviceOp deviceOp = null;
            try {
                deviceOp = new DeviceOp(
                        vertx,
                        nbiRequest.body,
                        nbiRequest,
                        internalDeviceOpSn++,
                        internalCallbackUrl);
            } catch (VertxException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new VertxException("JSON Parsing Error!");
            }

            /**
             * Save the DeviceOp POJO as the service specific data of this NBI request
             */
            nbiRequest.serviceData = deviceOp;

            /**
             * Register this device op request in the hash map
             */
            mapDeviceOpToNbiRequest.put(deviceOp.internalSn, nbiRequest);
        }

        return true;
    }

    /**
     * Override the Default Create Handler
     */
    @Override
    public void handleCreate(final AcsNbiRequest nbiRequest) {
        final DeviceOp deviceOp = (DeviceOp) nbiRequest.serviceData;

        /**
         * Is the CPE DB Object provided by the client? (i.e. internal request)
         */
        if (deviceOp.cpeDbObject == null) {
            /**
             * Query the CPE Device Record
             */
            try {
                VertxMongoUtils.findOne(
                        vertx.eventBus(),
                        Cpe.CPE_COLLECTION_NAME,
                        // Matcher which contains the CPE Key as the id
                        deviceOp.matcher,
                        // Async FindOne Result Handler
                        new VertxMongoUtils.FindOneHandler(new Handler<JsonObject>() {
                            @Override
                            public void handle(JsonObject cpeJsonObject) {
                                // Check for MongoDB timeouts
                                if (VertxMongoUtils.FIND_ONE_TIMED_OUT.equals(cpeJsonObject)) {
                                    deviceOp.sendResponse(
                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                            DeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                                            MONGODB_TIMED_OUT_STRING,
                                            null);
                                } else {
                                    cpeDeviceQueryHandler(cpeJsonObject, deviceOp, nbiRequest);
                                }
                            }
                        }),
                        // Keys
                        deviceOp.queryKeys
                );
            } catch (Exception ex) {
                deviceOp.sendResponse(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                        DeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                        "DB Query Failed due to exception " + ex.getMessage(),
                        null);
            }
        } else {
            /**
             * Conn-req Info is already provided. so no need to query the CPE DB.
             */
            cpeDeviceQueryHandler(deviceOp.cpeDbObject, deviceOp, nbiRequest);
        }
    }

    /**
     * Async CPE Device Query Result Handler Method
     */
    public void cpeDeviceQueryHandler (
            final JsonObject cpeJsonObject,
            final DeviceOp deviceOp,
            final AcsNbiRequest nbiRequest) {
        if (cpeJsonObject == null) {
            deviceOp.matcher.removeField(AcsConstants.FIELD_NAME_ORG_ID);
            deviceOp.sendResponse(
                    HttpResponseStatus.BAD_REQUEST,
                    DeviceOp.CPE_DEVICE_OP_STATE_INVALID_REQUEST,
                    "No CPE found with filter " + deviceOp.matcher.encode().replace("\"", "") + "!",
                    null);
        } else {
            /**
             * Found the CPE in the DB.
             */
            deviceOp.cpeIdString = cpeJsonObject.getString("_id");
            deviceOp.cpeDbObject = cpeJsonObject;

            /**
             * For "GetXxx" Operations only:
             *
             * Did the client ask for cached or live data or both?
             */
            if (deviceOp.getOptionsEnum != DeviceOp.GetOptionsEnum.LiveDataOnly) {
                // get the cached data from query result
                if (deviceOp.operationType == CpeDeviceOpTypeEnum.GetParameterValues) {
                    deviceOp.cachedData = cpeJsonObject.getObject(Cpe.DB_FIELD_NAME_PARAM_VALUES);
                } else {
                    deviceOp.cachedData = cpeJsonObject.getObject(Cpe.DB_FIELD_NAME_PARAM_ATTRIBUTES);
                }

                if (deviceOp.getOptionsEnum == DeviceOp.GetOptionsEnum.CachedDataOnly) {
                    // response with cached data right now
                    deviceOp.sendResponse(
                            HttpResponseStatus.OK,
                            DeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                            null,
                            deviceOp.cachedData);
                    return;
                }
            }

            /**
             * Send Conn-Req Request
             */
            sendConnReq(cpeJsonObject, deviceOp);

            // Start internal callback timer
            if (deviceOp.deviceOpJsonObject.containsField(CpeDeviceOp.FIELD_NAME_INTERNAL_SN)) {
                // Start timer
                deviceOp.setCallbackTimer(
                        vertx.setTimer(
                                // add additional timeout to allow inter process communication time
                                (deviceOp.timeout + 3) * 1000,
                                new Handler<Long>() {
                                    @Override
                                    public void handle(Long timerId) {
                                        handleFailure(
                                                deviceOp,
                                                HttpResponseStatus.REQUEST_TIMEOUT,
                                                "Device Operation Timed out!"
                                        );
                                    }
                                }
                        )
                );
            }
        }
    }

    /**
     * Internal Callback Handler.
     *
     * The callbacks are made by the CPE servers to send the device op results up to the ACS server.
     */
    Handler<JsonObjectMessage> internalCallbackHandler = new Handler<JsonObjectMessage>() {
        @Override
        public void handle(JsonObjectMessage callbackMessage) {
            JsonObject callback = callbackMessage.body();
            if (callback == null) {
                log.error("The callback message has no body!!");
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Received callback from CPE Server:\n" + callback.encodePrettily());
            }

            // Send empty reply
            callbackMessage.reply();

            // extract internal SN
            Long internalSn = callbackMessage.body().getLong(CpeDeviceOp.FIELD_NAME_INTERNAL_SN);
            if (internalSn == null) {
                log.error("No internal SN found!\n" + callback.encodePrettily());
                return;
            }

            // Look up the NBI request by internal SN
            AcsNbiRequest nbiRequest = mapDeviceOpToNbiRequest.get(internalSn);
            if (nbiRequest == null) {
                log.error("No NBI Request found for internal SN " + internalSn + "!\n"
                        + callback.encodePrettily());
                return;
            }

            // Extract Device Op POJO
            DeviceOp deviceOp = (DeviceOp) nbiRequest.serviceData;

            // Determine HTTP Status Code by device op state
            String state = callback.getString(CpeDeviceOp.FIELD_NAME_STATE, DeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED);
            HttpResponseStatus responseStatus;
            if (DeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED.equals(state)) {
                responseStatus = HttpResponseStatus.OK;
            } else if (state.equals(DeviceOp.CPE_DEVICE_OP_STATE_INTERNAL_ERROR)) {
                responseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            } else {
                responseStatus = HttpResponseStatus.BAD_REQUEST;
            }

            // Send Response
            try {
                deviceOp.sendResponse(
                        responseStatus,
                        state,
                        null,
                        callback.getObject(CpeDeviceOp.FIELD_NAME_RESULT));

                // Update hash map
                mapDeviceOpToNbiRequest.remove(internalSn);
            } catch (Exception ex) {
                log.error("Caught exception " + ex.getMessage() + " when sending response (upon callback)!");
                ex.printStackTrace();

                // remove from hash map
                mapDeviceOpToNbiRequest.remove(internalSn);
            }
        }
    };

    /**
     * Send connection request to CPE
     *
     * @param cpeJsonObject
     * @param deviceOp
     */
    public void sendConnReq(final JsonObject cpeJsonObject, final DeviceOp deviceOp) {
        // Build a request message
        String username = cpeJsonObject.getString(Cpe.DB_FIELD_NAME_CONNREQ_USERNAME);
        String password = cpeJsonObject.getString(Cpe.DB_FIELD_NAME_CONNREQ_PASSWORD);
        String connReqUrl = cpeJsonObject.getString(Cpe.DB_FIELD_NAME_CONNREQ_URL);
        JsonObject connReqRequest = new JsonObject()
                .putString(ConnectionRequestConstants.CPE_ID, deviceOp.cpeIdString)
                .putString(CpeDeviceOp.FIELD_NAME_CALLBACK_URL, internalCallbackAddress)
                .putString(ConnectionRequestConstants.URL, connReqUrl)
                .putString(ConnectionRequestConstants.USERNAME, username)
                .putString(ConnectionRequestConstants.PASSWORD, password);

        String orgId = cpeJsonObject.getString(AcsConstants.FIELD_NAME_ORG_ID);
        if (orgId != null) {
            Organization org = organizationCache.getOrgById(orgId);
            if (org != null && org.internalProxy != null && org.internalProxy.length() > 1) {
                connReqRequest.putString(ConnectionRequestConstants.PROXY, org.internalProxy);
            }
        }

        // Send the request message to connection-request worker vertice
        log.info(deviceOp.cpeIdString + ": Sending new conn-req request:\n" + connReqRequest.encode());
        vertx.eventBus().sendWithTimeout(
                AcsConstants.VERTX_ADDRESS_ACS_CONNECTION_REQUEST,
                connReqRequest,
                ConnectionRequestConstants.DEFAULT_CONN_REQ_TIMEOUT,
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        HttpResponseStatus httpResponseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                        String state = ConnectionRequestFsm.STATE_FAILED;
                        String error = null;
                        JsonObject result = null;

                        if (asyncResult.succeeded()) {
                            result = asyncResult.result().body();
                            if (result != null) {
                                log.info(deviceOp.cpeIdString + ": conn-req info: " + result.encode());
                                error = result.getString(ConnectionRequestConstants.ERROR);
                                state = result.getString(ConnectionRequestConstants.STATE,
                                        ConnectionRequestFsm.STATE_FAILED);
                            } else {
                                // this should never happen
                                error = "Invalid response from Conn-Req Worker!";
                            }
                        } else {
                            // Failed to send conn-req to worker
                            if (asyncResult.cause().getMessage() == null) {
                                // probably due to CPE server offline
                                error = "Internal Server ERROR (CPE server offline)";
                            } else {
                                error = "Internal Server ERROR " + asyncResult.cause().getMessage();
                            }
                        }

                        switch (state) {
                            case ConnectionRequestFsm.STATE_FAILED:
                                // Failed to send conn-req
                                handleFailure(
                                        deviceOp,
                                        HttpResponseStatus.REQUEST_TIMEOUT,
                                        "Failed to send connection request to the target device due to " + error
                                );
                                break;

                            case ConnectionRequestFsm.STATE_SESSION:
                                // Already has a session. Send this device-op request to the CPE server directly
                                deviceOp.sendToCpeServerViaEventBus(
                                        result.getString(ConnectionRequestConstants.CWMP_SESSION_MANAGER),
                                        new Handler<AsyncResult<Message<JsonObject>>>() {
                                            @Override
                                            public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                                                if (asyncResult.succeeded()) {
                                                    JsonObject result = asyncResult.result().body();
                                                    String status = result.getString(AcsConstants.FIELD_NAME_STATUS_CODE);

                                                    if (HttpResponseStatus.OK.toString().equals(status)) {
                                                        // The request has been accepted by the CPE Server.
                                                        // The actual device op result will arrive later via callback
                                                    } else {
                                                        String error = result.getString(AcsConstants.FIELD_NAME_ERROR);
                                                        log.error("Failed to send device operation request "
                                                                + "to CPE Server due to " + error);

                                                        if (HttpResponseStatus.NOT_FOUND.toString().equals(status)) {
                                                            /**
                                                             * This is a race condition. The session just got terminated.
                                                             *
                                                             * We need to re-start the whole thing.
                                                             */
                                                            log.error("Re-sending conn-req for " + deviceOp.cpeIdString
                                                                    + " in 5 seconds.");
                                                            vertx.setTimer(
                                                                    5000,
                                                                    new Handler<Long>() {
                                                                        @Override
                                                                        public void handle(Long event) {
                                                                            sendConnReq(deviceOp.cpeDbObject, deviceOp);
                                                                        }
                                                                    }
                                                            );
                                                        } else {
                                                            handleFailure(
                                                                    deviceOp,
                                                                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                                    INTERNAL_SERVER_ERROR_CONTACT_CALIX
                                                                            + "(error detail: Failed to send device "
                                                                            + "operation request to CPE Server due to "
                                                                            + error + ")"
                                                            );
                                                        }
                                                    }
                                                } else {
                                                    handleFailure(
                                                            deviceOp,
                                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                            asyncResult.cause().toString()
                                                    );
                                                }
                                            }
                                        }
                                );
                                break;

                            case ConnectionRequestFsm.STATE_SENDING:
                            case ConnectionRequestFsm.STATE_SENT:
                                // Still sending conn-req or waiting for CPE to establish a new session.
                                // Store the device op request into a Redis Queue
                                deviceOp.storeIntoRedisQueue(redisClient);
                                break;

                            case ConnectionRequestFsm.STATE_LOCKED:
                                String currOp = "long";
                                try {
                                    currOp = result.getObject("deviceOp").getString("operation");
                                } catch (Exception ex) {
                                    log.info("Unable to tell the type of the current op.");
                                }
                                handleFailure(
                                        deviceOp,
                                        HttpResponseStatus.LOCKED,
                                        "The target device is currently performing a " + currOp
                                                + " operation and cannot perform other operations at the same time."
                                );
                                break;
                        }
                    }
                }
        );
    }

    /**
     * Handle Failures.
     *
     * @param deviceOp
     * @param status
     * @param errorString
     */
    public void handleFailure(final DeviceOp deviceOp, HttpResponseStatus status, String errorString) {
        // Remove this device op from hash map
        mapDeviceOpToNbiRequest.remove(deviceOp.internalSn);

        // Remove this device op from Redis if applicable
        if (deviceOp.redisString != null) {
            deviceOp.removeFromRedisQueue(redisClient);
        }

        // Cleanup incomplete file record if needed
        if (deviceOp.operationType.equals(CpeDeviceOpTypeEnum.Upload)) {
            AcsFile.cleanupIncompleteUploads(
                    vertx.eventBus(),
                    Cpe.getSnByCpeKey(deviceOp.cpeIdString)
            );
        }

        // Add more details to the error string if timed out
        if (HttpResponseStatus.REQUEST_TIMEOUT.equals(status)) {
            if (deviceOp.cpeDbObject != null &&
                    deviceOp.cpeDbObject.getBoolean(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_ENABLE, false)) {
                int interval = deviceOp.cpeDbObject.getInteger(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_INTERVAL, 0);
                if (interval > 0) {
                    JsonObject lastInformTimeJsonObj = deviceOp.cpeDbObject.getObject(Cpe.DB_FIELD_NAME_LAST_INFORM_TIME);
                    if (lastInformTimeJsonObj != null) {
                        try {
                            Date lastInformTime = VertxMongoUtils.getDateFromMongoDateObject(
                                    deviceOp.cpeDbObject.getObject(Cpe.DB_FIELD_NAME_LAST_INFORM_TIME)
                            );
                            long timeSinceLastInform = (System.currentTimeMillis() - lastInformTime.getTime()) / 1000;
                            if (timeSinceLastInform > interval) {
                                errorString += " This device failed to send the periodical inform(s) for the last "
                                        + timeSinceLastInform/interval + " scheduled interval(s).";
                            }
                        } catch (VertxException ex) {
                            log.error("Caught exception " + ex.getMessage() + " while processing "
                                    + "lastInformTime for CPE " + deviceOp.cpeIdString + ": "
                                    + lastInformTimeJsonObj);
                        }
                    }
                }
            }
        }

        // Send response to caller
        deviceOp.sendResponse(
                status,
                DeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                errorString,
                null);
    }


}
