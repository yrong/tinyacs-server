package vertx2.cpeserver.deviceop;

import vertx2.VertxConstants;
import vertx2.VertxHttpClientUtils;
import vertx2.VertxRedisUtils;
import vertx2.VertxUtils;
import vertx2.connreq.ConnectionRequestConstants;
import vertx2.connreq.ConnectionRequestFsm;
import vertx2.connreq.ConnectionRequestUtils;
import vertx2.cpeserver.CpeServerConstants;
import vertx2.cpeserver.session.*;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import vertx2.cwmp.CwmpUtils;
import vertx2.model.CpeDeviceOp;
import vertx2.model.CpeDeviceOpTypeEnum;
import vertx2.util.AcsConstants;
import dslforumOrgCwmp12.FaultDocument;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA CC
 *
 * @author: ronyang
 */
public class DeviceOpUtils {
    private static final Logger log = LoggerFactory.getLogger(DeviceOpUtils.class.getName());

    /**
     * Default Callback Timeout (in ms)
     */
    private static final long DEFAULT_CALLBACK_TIMEOUT = CpeServerConstants.CWMP_NBI_CALLBACK_TIMEOUT;

    /**
     * Static Common Error JSON Objects
     */
    private static final JsonObject DEVICE_OP_TIME_OUT =
            new JsonObject().putString(CpeDeviceOp.FIELD_NAME_ERROR,"Device Timed Out!");
    private static final JsonObject DEVICE_OP_DRAINED =
            new JsonObject().putString(CpeDeviceOp.FIELD_NAME_ERROR, "TR-069/CWMP Session terminated by CPE unexpectedly!");
    private static final JsonObject INVALID_DEVICE_OP =
            new JsonObject().putString(CpeDeviceOp.FIELD_NAME_ERROR, "Invalid Device Operation!");

    /**
     * Enqueue a Device Op to a CWMP Session.
     *
     * @param deviceOp
     * @param session
     * @throws Exception
     */
    public static void enqueueDeviceOp(JsonObject deviceOp, CwmpSession session)  {
        String state = deviceOp.getString(CpeDeviceOp.FIELD_NAME_STATE);
        if (CpeDeviceOp.CPE_DEVICE_OP_STATE_IN_PROGRESS.equals(state)) {
            // Do not enqueue it if it is in progress.
            return;
        }

        try {
            CpeDeviceOpTypeEnum deviceOpOperationType = CpeDeviceOp.getOperationType(deviceOp);

            /**
             * Enqueue the deviceOp
             */
            switch (deviceOpOperationType) {
                case SetParameterValues:
                    SetParameterValuesNbi.startDeviceOp(deviceOp, session);
                    break;

                case GetParameterValues:
                    GetParameterValuesNbi.start(deviceOp, session);
                    break;

                case AddObject:
                    AddObject.start(deviceOp, session);
                    break;

                case DeleteObject:
                    DeleteObject.start(deviceOp, session);
                    break;

                case SetParameterAttributes:
                    SetParameterAttributesNbi.start(deviceOp, session);
                    break;

                case GetParameterAttributes:
                    GetParameterAttributesNbi.start(deviceOp, session);
                    break;

                case Reboot:
                    Reboot.start(deviceOp,session);
                    break;

                case Download:
                    Download.start(deviceOp,session);
                    break;

                case Upload:
                    Upload.start(deviceOp, session);
                    break;

                case Diagnostics:
                    Diagnostics.start(deviceOp, session);
                    break;

                case FactoryReset:
                    FactoryReset.start(deviceOp,session);
                    break;
            }
        } catch (Exception ex) {
            log.error("Invalid deviceOp!\n" + deviceOp.encodePrettily());
            ex.printStackTrace();
            return;
        }
    }

    /**
     * Push a partially completed device op back to Redis.
     *
     * @param session
     * @param deviceOp
     */
    public static void saveInProgressDeviceOp(
            final CwmpSession session,
            JsonObject deviceOp,
            CpeDeviceOpTypeEnum opType) {
        /**
         * Push back to Redis
         */
        final String redisKey = ConnectionRequestUtils.getConnReqRedisKeyByCpeId(session.cpe.getCpeKey());
        final String payload = new JsonObject()
                .putString(ConnectionRequestConstants.STATE, ConnectionRequestFsm.STATE_LOCKED)
                .putObject(CpeDeviceOp.FIELD_NAME_DEVICE_OP, deviceOp)
                .encode();
        log.info(session.cpeKey + ": Storing in-progress device op...");
        VertxRedisUtils.set(
                session.redisClient,
                redisKey,
                payload,
                CpeDeviceOp.getTimeout(deviceOp) * 1000,
                false,
                false,
                new Handler<String>() {
                    @Override
                    public void handle(String s) {
                        log.info(session.cpeKey + ": Successfully locked CPE.");
                    }
                }
        );

        /**
         * The Session must be terminated (and the CPE will terminate the session anyway)
         */
        session.bTriggeredByConnReq = false;
        session.bSessionInfoStoredInRedis = false;  // to prevent the session info being wiped out at the end of session
        session.multiSessionOpType = opType;
        session.terminate();
    }

    /**
     * Send an error string as result of an async operation to a callback URL due to invalid request..
     *
     * @param session
     * @param deviceOp
     * @param error
     * @return
     */
    public static void callbackInvalidReq(
            final CwmpSession session,
            JsonObject deviceOp,
            String error) {
        log.error(session.cpe.getCpeKey() + ": Device Op failed with error "  + error + "!\n"
                + deviceOp.encodePrettily());
        callback(
                session,
                deviceOp,
                CpeDeviceOp.CPE_DEVICE_OP_STATE_INVALID_REQUEST,
                new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, error)
        );
    }

    /**
     * Send an error string as result of an async operation to a callback URL due to an internal error.
     *
     * @param session
     * @param deviceOp
     * @param error
     * @return
     */
    public static void callbackInternalError(
            final CwmpSession session,
            JsonObject deviceOp,
            JsonObject error) {
        log.error(session.cpe.getCpeKey() + ": Device Op failed with internal error "  + error + "!\n"
                + deviceOp.encodePrettily());
        callback(
                session,
                deviceOp,
                CpeDeviceOp.CPE_DEVICE_OP_STATE_INTERNAL_ERROR,
                error
        );
    }

    /**
     * Send an error string as result of an async operation to a callback URL due to session being terminated.
     *
     * @param session
     * @param deviceOp
     * @return
     */
    public static void callbackDueToSessionTermination(
            final CwmpSession session,
            JsonObject deviceOp) {
        callback(
                session,
                deviceOp,
                CpeDeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                DEVICE_OP_DRAINED);
    }

    /**
     * When completing a multi-session device-op, the session info in Redis also needs to be updated (change state
     * from "locked" to "session").
     *
     * @param session
     * @param deviceOp
     * @param state
     * @param result
     */
    public static void multiSessionOpCallback(
            final CwmpSession session,
            final JsonObject deviceOp,
            final String state,
            final JsonObject result) {
        session.multiSessionOpType = CpeDeviceOpTypeEnum.Invalid;
        session.bSessionInfoStoredInRedis = false;
        // Mark session as triggered by conn-req so we will keep the session up for a little while
        session.bTriggeredByConnReq = true;
        session.storeSessionInfo(
                new Handler<String>() {
                    @Override
                    public void handle(String event) {
                        // Do callback here (after session info has been updated)
                        log.info(session.cpeKey + ": Completing "
                                + deviceOp.getString(CpeDeviceOp.FIELD_NAME_OPERATION) + " Callback...");
                        callback(
                                session,
                                deviceOp,
                                state,
                                result
                        );
                    }
                }
        );
    }

    /**
     * Send the result of an async operation to a callback URL.
     *
     * If the response contains the next operation,which may be the next operation, enqueue the next operation
     *
     * @param session
     * @param deviceOp
     * @param state
     * @param result
     * @return
     */
    public static void callback(
            final CwmpSession session,
            final JsonObject deviceOp,
            final String state,
            final JsonObject result) {
        /**
         * Validate Callback URL
         */
        String callbackUrl = deviceOp.getString(CpeDeviceOp.FIELD_NAME_CALLBACK_URL);
        if (callbackUrl == null) {
            log.error(session.cpeKey + ": Callback URL is null! deviceOp internalSn: "
                    + deviceOp.getString(CpeDeviceOp.FIELD_NAME_INTERNAL_SN));
            return;
        }

        /**
         * Suspend the session until a reply has been received towards the callback
         */
        session.state = CwmpSessionFsmStateEnum.PendingNbiCallback;

        /**
         * Is it multi-session op?
         */
        if (!session.multiSessionOpType.equals(CpeDeviceOpTypeEnum.Invalid)) {
            log.debug(session.cpeKey + ": Multi Session Op Callback " + session.multiSessionOpType.name());
            multiSessionOpCallback(
                    session,
                    deviceOp,
                    state,
                    result
            );
            return;
        }

        /**
         * Build a new Object that contains the deviceOp and the state/result
         */
        JsonObject payload = deviceOp.putString(CpeDeviceOp.FIELD_NAME_STATE, state);
        if (result != null) {
            payload.putObject(CpeDeviceOp.FIELD_NAME_RESULT, result);
        }
        if (log.isDebugEnabled()) {
            log.debug("callback payload: " + payload.encodePrettily());
        }

        /**
         * Check the transport protocol used by the callback URL
         */
        if (callbackUrl.startsWith(VertxConstants.URL_PROTOCOL_VERTX_EVENT_BUS)) {
            /**
             * The callback URL is Vert.x event bus based.
             */
            String address = VertxUtils.getVertxEventBusAddressFromUrl(callbackUrl);

            /**
             * The requester may reply to the callback with the next device op
             */
            Handler<AsyncResult<Message<JsonObject>>> replyHandler =
                    new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> event) {
                            if (event.failed()) {
                                // timed out (no reply received within 30 seconds)
                                if (session != null && session.cpe != null) {
                                    log.error(session.cpeKey + ": failed to send callback! Reason: "
                                            + event.cause().toString());
                                }
                            } else {
                                JsonObject nextOp = null;

                                if (event.result() != null && event.result().body() != null) {
                                    nextOp = event.result().body();

                                    /**
                                     * Enqueue the next op if any
                                     */
                                    if (nextOp.size() > 0) {
                                        if (CpeDeviceOp.isValid(nextOp)) {
                                            log.info(session.cpeKey + ": Received next op:" + nextOp.toString());
                                            enqueueDeviceOp(nextOp, session);
                                        } else {
                                            log.error(session.cpeKey + ": Received an invalid next op:"
                                                    + nextOp.toString());
                                        }
                                    }
                                }
                            }

                            //Save the timestamp as the last NBI request completion time
                            session.lastNbiRequestCompletionTime = System.currentTimeMillis();

                            /**
                             * Resume the session either way
                             */
                            CwmpSessionFsm.transit(session, CwmpSessionFsmEventEnum.NbiCallbackComplete);
                        }
                    };

            //log.info("Sending result to Vert.x callback address " + address + "...");
            session.vertx.eventBus().sendWithTimeout(address, payload, DEFAULT_CALLBACK_TIMEOUT, replyHandler);

            /**
             * Is the device op a true async op?
             */
            if (address.endsWith(AcsConstants.VERTX_ADDRESS_ACS_API_CALLBACK)) {
                // not a a true async op since the callback is to an ACS API server
            } else {
                // a true async op since the callback is to an internal north bound client
                // persist the device op
                CpeDeviceOp.persistDeviceOp(session.vertx.eventBus(), payload);
            }
        } else if (callbackUrl.startsWith("http")) {
            // Persist this device op
            CpeDeviceOp.persistDeviceOp(session.vertx.eventBus(), payload);

            /**
             * Post the result and check for the response
             */
            final Handler<Throwable> exceptionHandler = new Handler<Throwable>() {
                @Override
                public void handle(Throwable exception) {
                    if (exception != null) {
                        log.info("Caught exception " + exception.getMessage() + "!");
                    }

                    // Resume the CWMP Session
                    CwmpSessionFsm.transit(session, CwmpSessionFsmEventEnum.NbiCallbackComplete);
                }
            };
            Handler<HttpClientResponse> responseHandler = new Handler<HttpClientResponse>() {
                @Override
                public void handle(HttpClientResponse response) {
                    response.bodyHandler(new Handler<Buffer>() {
                        @Override
                        public void handle(Buffer body) {
                            /**
                             * Extract next op (if any) from response body
                             */
                            JsonObject nextOp = null;
                            if (body != null && body.length() > 0) {
                                try {
                                    nextOp = new JsonObject(body.toString());
                                    /**
                                     * Enqueue the next op if any
                                     */
                                    if (CpeDeviceOp.isValid(nextOp)) {
                                        log.info("Received next op for CPE " + session.cpe.key + ": \n" + nextOp.toString());
                                        enqueueDeviceOp(nextOp, session);
                                    }
                                } catch (Exception ex) {
                                    exceptionHandler.handle(ex);
                                }
                            }

                            /**
                             * Resume the session either way
                             */
                            CwmpSessionFsm.transit(session, CwmpSessionFsmEventEnum.NbiCallbackComplete);
                        }
                    });
                }
            };
            VertxHttpClientUtils.sendHttpRequest(
                    // URL String
                    callbackUrl,
                    // HTTP Method
                    HttpMethod.POST,
                    // Username
                    null,
                    // Password
                    null,
                    // Payload String
                    payload.toString(),
                    // Client Response Handler
                    responseHandler,
                    // Exception Handler
                    exceptionHandler,
                    // Timeout
                    DEFAULT_CALLBACK_TIMEOUT
            );
        }
    }

    /**
     * Get the TR Object Instance # by the Object Partial Path.
     *
     * @param objectPartialPath
     * @return
     */
    public static long getObjectIndex(String objectPartialPath) {
        // Remove the trailing "."
        String pathWithoutTrailingDot = objectPartialPath.substring(0, objectPartialPath.length() - 1);
        return Long.valueOf(
                pathWithoutTrailingDot.substring(
                        pathWithoutTrailingDot.lastIndexOf('.') + 1, pathWithoutTrailingDot.length()
                )
        );
    }

    /**
     * Get the TR Object Partial Path without the instance #
     * @param objectPartialPath
     * @return
     */
    public static String getObjectPathWithoutIndex(String objectPartialPath) {
        // Remove the trailing "."
        String pathWithoutTrailingDot = objectPartialPath.substring(0, objectPartialPath.length() - 1);
        return pathWithoutTrailingDot.substring(0, pathWithoutTrailingDot.lastIndexOf('.') + 1);
    }

    /**
     * Base/Common Response handler Class for NBI Device Op Requests
     */
    public static class NbiDeviceOpResponseHandler implements CwmpRequest.Handlers {
        JsonObject deviceOp;

        /**
         * Default Empty Constructor
         */
        public NbiDeviceOpResponseHandler() {
        }

        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
        }

        /**
         * Abstract Fault Response Handler Class that must be extended by actual requests
         *
         * @param session
         * @param request
         * @param cwmpFault
         */
        @Override
        public void faultHandler(CwmpSession session, CwmpRequest request, FaultDocument.Fault cwmpFault)
                throws CwmpException {
            log.info("Received fault response for " + request.methodName + " request!");

            // Send fault to callback URL if any
            if (deviceOp != null) {
                DeviceOpUtils.callback(
                        session,
                        deviceOp,
                        CpeDeviceOp.CPE_DEVICE_OP_STATE_INVALID_REQUEST,
                        CwmpUtils.cwmpFaultToJsonObject(cwmpFault)
                );
            }
        }

        /**
         * Abstract Timeout Handler Class that must be extended by actual requests.
         *
         * If the request has already been sent to the CPE but no response has been received when the session is being
         * timed out, this handler shall be called.
         *
         * @param session
         * @param request
         */
        @Override
        public void timeoutHandler(CwmpSession session, CwmpRequest request) throws CwmpException {
            log.info(session.cpe.getCpeKey() + ": " + request.methodName + " timed out!");

            // Send failure to callback URL if any
            if (deviceOp != null) {
                DeviceOpUtils.callback(
                        session,
                        deviceOp,
                        CpeDeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                        DEVICE_OP_TIME_OUT
                );
            }
        }

        /**
         * Abstract Drain Handler Class that must be extended by actual requests
         *
         * When a session is being timed out, all the requests that are still in the queue (i.e. have not been sent to
         * the CPE) will be drained out of the queue and this handler will be called for those requests being drained.
         *
         * @param session
         * @param request
         */
        @Override
        public void drainHandler(CwmpSession session, CwmpRequest request) throws CwmpException {
            /**
             * Do nothing for now
             */
            log.info(session.cpe.getCpeKey() + ": " + request.methodName + " is being drained!");

            // Send fault to callback URL if any
            if (deviceOp != null) {
                DeviceOpUtils.callback(
                        session,
                        deviceOp,
                        CpeDeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                        DEVICE_OP_DRAINED
                );
            }
        }
    }
}
