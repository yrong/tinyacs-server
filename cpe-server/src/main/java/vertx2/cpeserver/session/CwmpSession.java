package vertx2.cpeserver.session;

import com.calix.sxa.*;
import vertx2.connreq.ConnectionRequestUtils;
import vertx2.cpeserver.CpeServerConstants;
import vertx2.cpeserver.CpeServerTR069SessionVertice;
import vertx2.cpeserver.deviceop.*;
import vertx2.cwmp.*;
import vertx2.model.*;
import vertx2.util.*;
import vertx2.util.sxajboss.SxaStagerApiUtils;
import dslforumOrgCwmp12.*;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.impl.JsonObjectMessage;
import org.vertx.java.core.json.JsonObject;
import redis.clients.jedis.Jedis;

import java.util.LinkedList;

/**
 * Project:  SXA-CC CPE Server
 *
 * This class defines the CWMP Session Object and Session FSM (Finite-State-Machine) methods.
 *
 * @author: ronyang
 */
public class CwmpSession {
    private static final Logger log = LoggerFactory.getLogger(CwmpSession.class.getName());

    // FSM State
    public CwmpSessionFsmStateEnum state;

    // Vert.x
    public Vertx vertx;

    // Session vertice Instance
    public CpeServerTR069SessionVertice sessionVertice;

    // Vert.x Event Bus
    public EventBus eventBus;

    // Redis Client
    public RedisClient redisClient;

    // Org Id
    public String orgId;

    // CPE Key (initially built from the DeviceId struct within Inform)
    public String cpeKey = null;

    // The CPE Object
    public Cpe cpe = null;

    // Zero Touch Activation?
    public boolean bZeroTouchActivation = false;
    public String acsUsername = null;
    public String acsPassword = null;

    // Subscriber Service Plan
    public JsonObject subscriberServicePlan = null;
    public JsonObject serviceParameterValues = null;

    // Timers (for tracking CPE activities)
    public Long inactiveTimerId = null;
    public Long redisTimerId = null;

    // Copy of the original "Inform" request which started this session
    public InformDocument.Inform informRequest;
    public String[] informEventCodes = null;

    public CwmpMessage receivedCpeMessage;

    // Current HTTP Server Request Message (received from the HTTP Load Balancer Vertice)
    public JsonObject httpRequest;
    public JsonObjectMessage httpRequestMessage;

    // CWMP Version
    public CwmpVersionEnum cwmpVersion;

    // Internal Request Queue
    public LinkedList<CwmpRequest> cwmpRequestQueue;

    // Current out-standing north bound Request
    public CwmpRequest outstandingRequest;

    // Session Message ID (a sequential int starting from 1)
    public int messageId = 1;

    // Boolean indicator that indicates whether the session was triggered by Connection-Request.
    public boolean bTriggeredByConnReq = false;

    // Boolean indicator that indicates whether the session info was stored into Redis
    public boolean bSessionInfoStoredInRedis = false;

    // Boolean indicator that there is a multi-session device op in progress
    public CpeDeviceOpTypeEnum multiSessionOpType = CpeDeviceOpTypeEnum.Invalid;

    // To-be-saved boot event type (if any)
    public EventTypeEnum bootEventType = null;

    // In-progress (Multi-Session) Device Op
    public JsonObject inProgressDeviceOp = null;

    // File Transfer Result
    public JsonObject fileTransferResult = null;

    // Has this CPE been upgraded since last Inform?
    public boolean cpeUpgraded = false;
    // Has this CPE got a new IP address since last Inform?
    public boolean cpeWanIpInfoChanged = false;
    // Has this CPE got a new RegId since last Inform?
    public boolean cpeRegIdChanged = false;
    // Has this CPE reported a new value for its change counter?
    public boolean cpeChangeCounterChanged = false;

    /**
     * Default constructor that requires an "Inform" request.
     *
     * @param sessionVertice
     * @param vertx
     * @param redisClient
     * @param httpRequestMessage
     * @param httpRequest
     * @param orgId
     * @param cpeKey
     * @param cwmpMessage
     * @throws CwmpException
     * @throws SxaVertxException
     */
    public CwmpSession(
            CpeServerTR069SessionVertice sessionVertice,
            Vertx vertx,
            RedisClient redisClient,
            final JsonObjectMessage httpRequestMessage,
            final JsonObject httpRequest,
            final String orgId,
            final String cpeKey,
            final CwmpMessage cwmpMessage)
            throws CwmpException, SxaVertxException {
        // Save the Session Vertice and Vert.x instance
        this.sessionVertice = sessionVertice;
        this.vertx = vertx;

        // Save the Vert.X Event Bus
        this.eventBus = vertx.eventBus();

        // Save the Redis Client
        this.redisClient = redisClient;

        // Save orgId
        this.orgId = orgId;

        // Save the HTTP Request
        this.httpRequest = httpRequest;
        this.httpRequestMessage = httpRequestMessage;

        // Check for Zero Touch Activation
        this.bZeroTouchActivation = httpRequest.getBoolean(CpeServerConstants.FIELD_NAME_ZERO_TOUCH, false);
        if (bZeroTouchActivation) {
            acsUsername = httpRequest.getString(CpeServerConstants.FIELD_NAME_ACS_USERNAME);
            acsPassword = httpRequest.getString(CpeServerConstants.FIELD_NAME_ACS_PASSWORD);
        }

        // Save the "Inform" request
        this.receivedCpeMessage = cwmpMessage;
        this.informRequest = cwmpMessage.soapEnv.getBody().getInform();

        // Save the CWMP Version
        this.cwmpVersion = cwmpMessage.cwmpVersion;

        // Create a new request queue
        cwmpRequestQueue = new LinkedList<>();

        // Save CPE Key
        this.cpeKey = cpeKey;

        // Initialize FSM State
        state = CwmpSessionFsmStateEnum.Start;

        // Save Session into HashMap
        sessionVertice.addSession(this);

        log.info("New CWMP Session Created: " + this.toString());

        // Transit State Machine
        CwmpSessionFsm.transit(this, CwmpSessionFsmEventEnum.CpeMessageReceived);
    }

    /**
     * Inner Handler Class that handles the MongoDB CPE record query result.
     */
    public static class DbQueryHandler implements Handler<JsonObject> {
        CwmpSession session;
        String orgId;
        dslforumOrgCwmp12.DeviceIdStruct deviceIdStruct;

        /**
         * Constructor.
         *
         * @param session
         * @param orgId
         * @param deviceIdStruct
         */
        public DbQueryHandler(
                CwmpSession session,
                String orgId,
                dslforumOrgCwmp12.DeviceIdStruct deviceIdStruct) {
            this.session = session;
            this.orgId = orgId;
            this.deviceIdStruct = deviceIdStruct;
        }

        @Override
        public void handle(JsonObject cpeJsonObject) {
            /**
             * Check for MongoDB Timeouts
             */
            if (VertxMongoUtils.FIND_ONE_TIMED_OUT.equals(cpeJsonObject)) {
                CwmpSessionFsm.transit(session, CwmpSessionFsmEventEnum.InternalDbFailure);
                return;
            }

            /**
             * Create CPE POJO by the result
             */
            if (cpeJsonObject == null) {
                // Received Inform from a new CPE
                session.cpe = new Cpe(orgId, deviceIdStruct);
                session.cpe.bNeedDiscovery = true;
                log.info(session.cpeKey + ": Received Inform from a new CPE");
            } else {
                // Received Inform from an existing (created via API or auto-discovered) CPE
                session.cpe = new Cpe(cpeJsonObject);

                if (!cpeJsonObject.containsField(Cpe.DB_FIELD_NAME_CONNREQ_URL)) {
                    log.info(session.cpeKey + ": Received Inform from a pre-provisioned CPE");
                    session.cpe.bNeedDiscovery = true;
                } else {
                    log.info(session.cpeKey + ": Received Inform from a known CPE");

                    session.cpe.bNeedDiscovery = false;

                    JsonObject initialProvisioning = cpeJsonObject.getObject(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING);
                    if (initialProvisioning != null &&
                            initialProvisioning.getBoolean(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING_FORCE_APPLY, false)
                                    == true){
                        // Re-push the initial provisioning
                        try {
                            // Enqueue the request
                            SetParameterValuesNbi.startNbiProvisioning(session, initialProvisioning);
                        } catch (CwmpException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            // Save the timestamp for last Inform time
            session.cpe.addTimeStamp(Cpe.DB_FIELD_NAME_LAST_INFORM_TIME);

            // Transit State Machine
            CwmpSessionFsm.transit(session, CwmpSessionFsmEventEnum.DbQueryResultReceived);
        }
    }

    /**
     * Post a new request to this session's request queue
     */
    public void addNewRequest(CwmpRequest request) {
        cwmpRequestQueue.addLast(request);
        CwmpSessionFsm.transit(this, CwmpSessionFsmEventEnum.NewNbiRequest);
    }

    /**
     * Process a raw Device Op JSON Object String
     * @param deviceOpRawString
     */
    public void processRawDeviceOpString(String deviceOpRawString) {
        if (deviceOpRawString == null) {
            return;
        }

        JsonObject aDeviceOp = null;
        try {
            aDeviceOp = new JsonObject(deviceOpRawString);
        } catch (Exception ex) {
            log.error("Received malformed device op from Redis!" + deviceOpRawString);
            return;
        }

        processRawDeviceOp(aDeviceOp);
    }

    /**
     * Process a raw Device Op JSON Object
     * @param aDeviceOp
     */
    public void processRawDeviceOp(JsonObject aDeviceOp) {
        /**
         * Process a single deviceOp
         */
        if (isTerminated()) {
            // Only proceed if session is still up
            DeviceOpUtils.callbackDueToSessionTermination(this, aDeviceOp);
            return;
        }

        //log.info("Processing new deviceOp: \n" + aDeviceOp.toString());

        // Enqueue ops that have not been started
        try {
            DeviceOpUtils.enqueueDeviceOp(aDeviceOp, this);
            CwmpSessionFsm.transit(this, CwmpSessionFsmEventEnum.NewNbiRequest);
        } catch (Exception e) {
            log.error("Failed to enqueue deviceOp due to " + e.getMessage());
        }
    }

    /**
     * Terminate the session with a custom handler which is to be called after removing session info from Redis
     */
    public void terminate(VertxRedisUtils.ResultHandler handler) {
        if (state.equals(CwmpSessionFsmStateEnum.Terminated)) {
            return;
        }

        // Send empty response
        if (httpRequestMessage != null) {
            CwmpHttpRequestHandler.reply(httpRequestMessage, HttpResponseStatus.NO_CONTENT, null, null);
        }

        // Set proper state
        state = CwmpSessionFsmStateEnum.Terminated;

        /**
         * Remove Session Key from Redis
         */
        removeSessionInfoFromRedis(handler);

        /**
         * Persist CPE Updates if any
         */
        if (cpe != null) {
            /**
             * Update SXA Search Engine if this is a new CPE or the CPE's WAN info or SW version has changed
             */
            if (cpe.bNeedDiscovery || cpeWanIpInfoChanged || cpeUpgraded || cpeRegIdChanged) {
                log.debug("cpe.bNeedDiscovery:" + cpe.bNeedDiscovery
                        + ", cpeWanIpInfoChanged:" + cpeWanIpInfoChanged
                        + ", cpeUpgraded:" + cpeUpgraded
                        + ", cpeRegIdChanged: " + cpeRegIdChanged);
                /**
                 * Call SXA JBoss/Stager API to update Elastic Search Index
                 */
                SxaStagerApiUtils.deviceDiscoveryAndUpdate(vertx.eventBus(), cpe);
            }

            if (cpe.bDecommissioned) {
                if (cpe.bNeedReDiscovery) {
                    if (!cpeRegIdChanged) {
                        /**
                         * Received "0 BOOTSTRAP" from a decommissioned device with the same RegId
                         *
                         * Skip the rest of the discovery process to avoid infinite loop
                         */
                        log.info(cpeKey + ": Received \"0 BOOTSTRAP\" but device is decommissioned.");
                        cpe.bNeedReDiscovery = false;
                        cpe.bNeedDiscovery = false;
                    }
                }
            }

            if (cpe.bNeedDiscovery || cpe.bNeedReDiscovery) {
                /**
                 * Save Discovery Event
                 */
                Event.saveEvent(
                        eventBus,
                        cpe.orgId,
                        cpe.deviceId.sn,
                        EventTypeEnum.Discovery,
                        EventSourceEnum.System,
                        null
                );

                /**
                 * Update Last-Discover Time (for discovery-workflow exec log query)
                 */
                cpe.addTimeStamp(Cpe.DB_FIELD_NAME_LAST_DISCOVER_TIME);
            }

            if (cpeChangeCounterChanged) {
                /**
                 * Enqueue an auto backup task for this device
                 */
                AutoBackupUtils.addToQueue(redisClient, cpe.toBasicJsonObjectForConnReq());
            }

            /**
             * Persist CPE Data now
             */
            cpe.updateDb(
                    eventBus,
                    (cpe.bNeedDiscovery && cpe.bDiscoveryDone) ?
                            // Add this CPE to the cpe-discovery queue after saving it to DB
                            new Handler<Long>() {
                                @Override
                                public void handle(Long result) {
                                    if (result != null) {
                                        CpeDiscoveryUtils.addToQueue(
                                                redisClient,
                                                cpe.deviceId.toJsonObject()
                                                        .putString(AcsConstants.FIELD_NAME_ORG_ID, cpe.getOrgId())
                                                        .putBoolean("newDiscovery", !cpe.bNeedReDiscovery)
                                                        .putString(AcsConstants.FIELD_NAME_ID, cpe.key)
                                        );
                                    }
                                }
                            }
                            :
                            null
            );
        }

        // Delete session from HashMap
        sessionVertice.delSession(this);

        // Cancel timer(s) if any
        cancelTimers();
    }

    /**
     * Terminate the session
     */
    public void terminate() {
        terminate(null);
    }

    /**
     * Return the ACS Hostname being used by the CPE
     */
    public String getAcsHostname() {
        if (httpRequest != null) {
            return httpRequest.getString(
                    CpeServerConstants.FIELD_NAME_ACS_HOST,
                    // Default to CPE_SERVER_LB_HOST
                    AcsConfigProperties.CPE_SERVER_LB_HOST
            );
        } else {
            return AcsConfigProperties.CPE_SERVER_LB_HOST;
        }
    }

    /**
     * CPE Timeout Handler
     */
    public Handler<Long> sessionInactiveTimerHandler = null;

    /**
     * Start Inactive Timer
     *
     * @param timeout
     */
    public void startInactiveTimer(long timeout) {
        if (inactiveTimerId != null) {
            vertx.cancelTimer(inactiveTimerId);
        }

        if (sessionInactiveTimerHandler == null) {
            sessionInactiveTimerHandler = new SessionInactiveTimeoutHandler(this);
        }
        inactiveTimerId = vertx.setTimer(timeout, sessionInactiveTimerHandler);
    }

    /**
     * Start Redis Timer
     *
     * @param timeout
     */
    public void startRedisTimer(long timeout) {
        if (redisTimerId != null) {
            vertx.cancelTimer(redisTimerId);
        }

        if (redisTimerHandler == null) {
            redisTimerHandler = new Handler<Long>() {
                @Override
                public void handle(Long timerId) {
                    if (!isTerminated()) {
                        popDeviceOpFromRedis();
                    }
                }
            };
        }
        redisTimerId = vertx.setTimer(timeout, redisTimerHandler);
    }

    /**
     * Cancel Inactive Timer and Redis Timer
     */
    public void cancelTimers() {
        if (inactiveTimerId != null) {
            vertx.cancelTimer(inactiveTimerId);
            inactiveTimerId = null;
        }
        if (redisTimerId != null) {
            vertx.cancelTimer(redisTimerId);
            redisTimerId = null;
        }
    }

    /**
     * Inner Timeout Handler Class
     */
    public static class SessionInactiveTimeoutHandler implements Handler<Long> {
        CwmpSession session;

        /**
         * Constructor.
         *
         * @param session
         */
        public SessionInactiveTimeoutHandler(CwmpSession session) {
            this.session = session;
        }

        @Override
        public void handle(Long timerID) {
            log.debug(session.cpeKey + ": timed out! (timerId: " + timerID + ")");

            // Drain all requests in the queue
            CwmpRequest pendingRequest = null;
            do {
                pendingRequest = session.cwmpRequestQueue.poll();
                if (pendingRequest != null) {
                    try {
                        pendingRequest.handlers.timeoutHandler(session, pendingRequest);
                    } catch (CwmpException e) {
                        e.printStackTrace();
                    }
                }
            } while (pendingRequest != null);

            // Perform additional actions by state
            CwmpSessionFsm.transit(session, CwmpSessionFsmEventEnum.Timeout);
        }
    }

    /**
     * Timer Handler for reading from Redis Periodically
     */
    public Handler<Long> redisTimerHandler = null;
    /**
     * Time Length (in # of seconds) that NBI remains inactive
     */
    public long lastNbiRequestCompletionTime = 0;

    /**
     * Handler that handles the Redis lpop result which may contain a new device op request string or null
     */
    public static class PopDeviceOpFromRedisQueueHandler implements Handler<String> {
        CwmpSession session;

        public PopDeviceOpFromRedisQueueHandler(CwmpSession session) {
            this.session = session;
        }

        @Override
        public void handle(String deviceOpRawString) {
            // Do we have a valid device-op?
            if (deviceOpRawString != null) {
                // Got a valid device-op, process it
                session.processRawDeviceOpString(deviceOpRawString);

                // Continue reading till the queue is empty
                session.popDeviceOpFromRedis();
            } else if (session.cwmpRequestQueue.size() == 0 && session.outstandingRequest == null) {
                // no valid device-op from queue
                //log.debug(cpeKey + ": No device op request received from Redis.");
                long currTime = System.currentTimeMillis();
                if (session.lastNbiRequestCompletionTime == 0) {
                    session.lastNbiRequestCompletionTime = currTime;
                } else {
                    long nbiInactiveTime = currTime - session.lastNbiRequestCompletionTime;
                    if (nbiInactiveTime > CpeServerConstants.CWMP_SESSION_NBI_INACTIVE_TIMEOUT) {
                        log.debug(session.cpeKey + ": No NBI request received for " + nbiInactiveTime + " milliseconds.");
                        CwmpSessionFsm.transit(session, CwmpSessionFsmEventEnum.Timeout);
                    }
                }
            }

            // Try to read again in 1 second
            if (!session.isTerminated()) {
                session.startRedisTimer(1000);
            }
        }
    }
    public Handler<String> popDeviceOpFromRedisQueueHandler = new PopDeviceOpFromRedisQueueHandler(this);

    /**
     * Get the next device op from Redis via blocking POP
     */
    public void popDeviceOpFromRedis() {
        //log.info(cpe.getCpeKey() + ": checking next device op request if any...");

        // Hold the session up for a few more seconds in case the FE client has more requests
        VertxRedisUtils.lpop(
                redisClient,
                CpeDeviceOp.getDeviceOpsRedisListKeyByCpeId(cpe.getCpeKey()),
                popDeviceOpFromRedisQueueHandler
        );
    }

    /**
     * Static Inner Class to handle redis read result which contains the current in-progress device op for this session
     */
    public static class ReadInProgressDeviceOpFromRedisHandler implements Handler<String> {
        CwmpSession session;

        // Constructor
        public ReadInProgressDeviceOpFromRedisHandler(CwmpSession session) {
            this.session = session;
        }

        @Override
        public void handle(String s) {
            session.bSessionInfoStoredInRedis = true;
            CpeDeviceOpTypeEnum opType = CpeDeviceOpTypeEnum.Invalid;

            if (s != null) {
                try {
                    session.inProgressDeviceOp = new JsonObject(s).getObject(CpeDeviceOp.FIELD_NAME_DEVICE_OP);
                    if (session.inProgressDeviceOp != null &&
                            session.inProgressDeviceOp.getString(CpeDeviceOp.FIELD_NAME_OPERATION) != null) {
                        opType = CpeDeviceOpTypeEnum.getDeviceOpTypeEnumByString(
                                session.inProgressDeviceOp.getString(CpeDeviceOp.FIELD_NAME_OPERATION));
                    }
                } catch (Exception ex) {
                    log.error("Caught exception while reading in-progress device op info from Redis! "
                            + ex.getMessage() + "\n" + s);
                    session.multiSessionOpType = CpeDeviceOpTypeEnum.Invalid;
                    session.inProgressDeviceOp = null;
                }
            } else {
                session.multiSessionOpType = CpeDeviceOpTypeEnum.Invalid;
                session.inProgressDeviceOp = null;

                if (session.bootEventType != null) {
                    if (session.cpeUpgraded) {
                        // Save autonomous boot event
                        Event.saveEvent(
                                session.eventBus,
                                session.cpe.orgId,
                                session.cpe.deviceId.sn,
                                EventTypeEnum.SwUpgrade,
                                EventSourceEnum.Autonomous,
                                new JsonObject().putString("upgraded to", session.cpe.deviceId.swVersion)
                        );
                    } else {
                        // Save autonomous boot event
                        Event.saveEvent(
                                session.eventBus,
                                session.cpe.orgId,
                                session.cpe.deviceId.sn,
                                session.bootEventType.equals(EventTypeEnum.Reboot)?
                                    EventTypeEnum.Reboot
                                    :
                                    EventTypeEnum.FactoryReset,
                                EventSourceEnum.Autonomous,
                                null
                        );
                    }
                }
            }

            // Sometime the in-progress op type cannot be determined by event codes from Inform
            if (session.multiSessionOpType.equals(CpeDeviceOpTypeEnum.Unknown)) {
                session.multiSessionOpType = opType;
            }

            if (!session.multiSessionOpType.equals(opType)) {
                session.multiSessionOpType = CpeDeviceOpTypeEnum.Invalid;
            } else if (!opType.equals(CpeDeviceOpTypeEnum.Invalid)) {
                log.info(session.cpeKey + ": Continue processing device op " + opType.name());
                switch (opType) {
                    case Diagnostics:
                        session.collectDiagResult();
                        break;

                    case Download:
                    case Upload:
                        session.transferCompleteCallback();
                        break;

                    case FactoryReset:
                        // callback
                        DeviceOpUtils.callback(
                                session,
                                session.inProgressDeviceOp,
                                CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                                null
                        );
                        break;

                    case Reboot:
                        DeviceOpUtils.callback(
                                session,
                                session.inProgressDeviceOp,
                                CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                                null
                        );
                        break;
                }
            }

            if (!CwmpSessionFsmStateEnum.Terminated.equals(session.state) &&
                    !CwmpSessionFsmStateEnum.PendingNbiCallback.equals(session.state)) {
                CwmpSessionFsm.transit(session, CwmpSessionFsmEventEnum.ReceivedInProgressDeviceOp);
            }
        }
    };

    /**
     * Collect Diagnostics Results
     */
    public void collectDiagResult() {
        Diagnostics.collectResult(inProgressDeviceOp, this);
    }

    /**
     * Read the session info back in order to get the current in-progress (diag) device op
     */
    public void readCurrentInProgressDeviceOp() {
        ConnectionRequestUtils.getConnReqStateByCpeId(
                redisClient,
                cpe.getCpeKey(),
                new ReadInProgressDeviceOpFromRedisHandler(this)
        );
    }

    /**
     * Store the session info for a CPE in Redis.
     */
    public void storeSessionInfo() {
        if (bSessionInfoStoredInRedis || !multiSessionOpType.equals(CpeDeviceOpTypeEnum.Invalid)) {
            return;
        }

        /**
         * Simply write the session info into Redis with no timeout
         */
        VertxRedisUtils.set(
                redisClient,
                ConnectionRequestUtils.getConnReqRedisKeyByCpeId(cpe.getCpeKey()),
                sessionVertice.CONN_REQ_FSM_INFO_STRING,
                CpeServerConstants.CWMP_SESSION_TIMEOUT,
                false,
                false,
                new Handler<String>() {
                    @Override
                    public void handle(String s) {
                        log.info(cpe.getCpeKey() + ": Successfully stored/refreshed session info in Redis.");
                    }
                }
        );
        bSessionInfoStoredInRedis = true;
    }

    /**
     * Store the session info for a CPE in Redis with a custom handler.
     */
    public void storeSessionInfo(Handler<String> customHandler) {
        log.debug(cpeKey + ": Saving session info as " + sessionVertice.CONN_REQ_FSM_INFO_STRING);
        /**
         * Simply write the session info into Redis with no timeout
         */
        VertxRedisUtils.set(
                redisClient,
                ConnectionRequestUtils.getConnReqRedisKeyByCpeId(cpe.getCpeKey()),
                sessionVertice.CONN_REQ_FSM_INFO_STRING,
                CpeServerConstants.CWMP_SESSION_TIMEOUT,
                false,
                false,
                customHandler
        );
        bSessionInfoStoredInRedis = true;
    }

    /**
     * Refresh Session Info in redis (to avoid Redis Expiration)
     */
    public void refreshSessionInfoInRedis() {
        if (bTriggeredByConnReq && bSessionInfoStoredInRedis) {
            bSessionInfoStoredInRedis = false;
            storeSessionInfo();
        }
    }

    /**
     * Remove Session Info from Redis using blocking call (Jedis).
     *
     * Called when shutting down CPE server.
     *
     * @param jedisClient
     */
    public void removeSessionInfoFromRedis(Jedis jedisClient) {
        if (bSessionInfoStoredInRedis) {
            bSessionInfoStoredInRedis = false;
            jedisClient.del(ConnectionRequestUtils.getConnReqRedisKeyByCpeId(cpe.getCpeKey()));
        }
    }

    /**
     * Remove Session Info from Redis using non-blocking call (mod-redis).
     *
     * Called when terminating session.
     */
    public void removeSessionInfoFromRedis() {
        if (bSessionInfoStoredInRedis) {
            bSessionInfoStoredInRedis = false;
            VertxRedisUtils.del(redisClient, ConnectionRequestUtils.getConnReqRedisKeyByCpeId(cpe.getCpeKey()));
        }
    }

    /**
     * Remove Session Info from Redis using non-blocking call (mod-redis) with a custom handler.
     *
     * Called when terminating session.
     */
    public void removeSessionInfoFromRedis(VertxRedisUtils.ResultHandler handler) {
        if (bSessionInfoStoredInRedis) {
            bSessionInfoStoredInRedis = false;
            VertxRedisUtils.del(
                    redisClient,
                    ConnectionRequestUtils.getConnReqRedisKeyByCpeId(cpe.getCpeKey()),
                    handler
            );
        }
    }

    /**
     * Check if this session has already been terminated
     * @return
     */
    public boolean isTerminated() {
        switch (state) {
            case Terminated:
            case AuthFailure:
                return true;

            default:
                return false;
        }
    }

    /**
     * Customized toString() method.
     */
    @Override
    public String toString() {
        return cpeKey;
    }

    /**
     * Longer version of toString() method.
     */
    public String toStringLong() {
        String result = cpeKey;

        if (cwmpVersion != CwmpVersionEnum.CWMP_VERSION_ENUM_UNKNOWN) {
            result += ", CWMP Version: " + cwmpVersion.name();
        }
        if (outstandingRequest != null)
            result += ", outstanding req: " + outstandingRequest.toString();

        return result;
        /**
         * TODO: add event info from the inform request.
         */
    }

    /**
     * Get CPE Key by orgId and an Inform message.
     *
     * @param orgId
     * @param cwmpMessage
     * @return
     */
    public static String getCpeKeyFromInform(String orgId, CwmpMessage cwmpMessage) {
        // Get DeviceId Struct
        final dslforumOrgCwmp12.DeviceIdStruct deviceId = cwmpMessage.soapEnv.getBody().getInform().getDeviceId();
        if (deviceId != null) {
            String oui = deviceId.getOUI();
            if (!AcsMiscUtils.isOuiString(oui)) {
                return null;
            }

            String sn = deviceId.getSerialNumber();
            if (!AcsMiscUtils.isFSANString(sn)) {
                return null;
            }

            return Cpe.getCpeKey(orgId, oui, sn);
        } else {
            return null;
        }
    }
    /**
     * Process a newly received CWMP Message.
     *
     *
     * @param vertx
     * @param redisClient
     * @param request
     * @param cwmpMessage
     * @return
     */
    public static CwmpSession processCpeMessage(
            Vertx vertx,
            CpeServerTR069SessionVertice sessionVertice,
            RedisClient redisClient,
            JsonObjectMessage requestMessage,
            JsonObject request,
            CwmpMessage cwmpMessage)
            throws CwmpException {
        /**
         * OrgId is carried by the Per-Org Authenticator
         */
        String orgId = request.getString(AcsConstants.FIELD_NAME_ORG_ID);

        boolean bIsNewSession = false;

        CwmpSession session = null;

        /**
         * Retrieve HTTP session cookie from request header
         */
        String cookie = request.getString(CpeServerConstants.FIELD_NAME_COOKIE);
        if (cookie == null) {
            /**
             * No cookie found.
             *
             * The only message type allowed in this case is the Inform.
             */
            if (cwmpMessage != null && cwmpMessage.rpcMessageName.equals("Inform")) {
                String cpeKey = getCpeKeyFromInform(orgId, cwmpMessage);
                if (cpeKey == null) {
                    log.error(VertxUtils.highlightWithHashes("Received an invalid Inform!")
                            + cwmpMessage.toPrettyXmlText());
                    throw new CwmpException("Invalid Inform Received!", CwmpFaultCodes.ACS_REQUEST_DENIED);
                }
                session = sessionVertice.getSessionByCpeKey(cpeKey);
                boolean bNeedNewSession = false;
                if (session == null) {
                    bNeedNewSession = true;
                } else {
                    log.info("Received an Inform, but Found existing CWMP Session: " + session.toString()
                            + " (state: " + session.state + ")");

                    /**
                     * Not expecting an Inform.
                     *
                     * Probably because the CPE re-connects before we terminate the previous session.
                     */
                    // Let us clean up the previous session and restart from scratch.
                    //session.httpRequestMessage = null;
                    session.terminate();
                    bNeedNewSession = true;
                }

                // Need a new Session?
                if (bNeedNewSession) {
                    /**
                     * Try to create a new CWMP session based on this inform request
                     */
                    log.info("Starting a new session upon Inform...");
                    try {
                        session = new CwmpSession(
                                sessionVertice,
                                vertx,
                                redisClient,
                                requestMessage,
                                request,
                                orgId,
                                cpeKey,
                                cwmpMessage
                        );
                        bIsNewSession = true;
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Unable to create CWMP Session due to " + e.getMessage());
                        throw new CwmpException(
                                "Malformed Inform Request",
                                CwmpFaultCodes.ACS_REQUEST_DENIED,
                                cwmpMessage.cwmpVersion);
                    }
                }
            } else {
                // No session, not an Inform
                log.error("Please start CWMP session with a valid Inform!");
                throw new CwmpException(
                        "Please start CWMP session with a valid Inform!",
                        CwmpFaultCodes.ACS_REQUEST_DENIED);
            }
        } else {
            session = sessionVertice.getSessionByCookie(cookie);
            if (session == null) {
                String error = "Unable to find existing session by cookie " + cookie;
                log.error(error);
                throw new CwmpException(error, CwmpFaultCodes.ACS_REQUEST_DENIED);
            } else {
                log.info("Found existing CWMP Session: " + session.toString()
                        + ", vertice " + sessionVertice.verticeIndex);
            }
        }

        if (!bIsNewSession) {
            /**
             * Process the received message for existing sessions.
             *
             * For new sessions, the received Inform has already been processed.
             */
            session.receivedCpeMessage = cwmpMessage;
            session.httpRequestMessage = requestMessage;
            session.httpRequest = request;

            // Persist this CWMP Message
            if (cwmpMessage != null && session.cpe != null) {
                cwmpMessage.persist(session.eventBus, session.cpe, null);
            }

            CwmpSessionFsm.transit(session, CwmpSessionFsmEventEnum.CpeMessageReceived);
        } else {
            session.sessionVertice = sessionVertice;
        }
        return session;
    }

    /**
     * Create an InformResponse message based on the Inform message
     */
    CwmpMessage handleInform() {
        /**
         * Parse the event list.
         */
        EventList eventList = informRequest.getEvent();
        if (eventList != null && eventList.getEventStructArray(0) != null) {
            informEventCodes = new String[eventList.sizeOfEventStructArray()];

            for (int i = 0; i < eventList.sizeOfEventStructArray(); i ++) {
                informEventCodes[i] = eventList.getEventStructArray(i).getEventCode();
                log.info("eventCode: " + informEventCodes[i]);
                switch (informEventCodes[i]) {
                    case CwmpInformEventCodes.CONNECTION_REQUEST:
                        /**
                         * Mark Session as triggered by Connection Request
                         */
                        bTriggeredByConnReq = true;

                        /**
                         * Store Session Info in Redis
                         */
                        storeSessionInfo();
                        break;

                    case CwmpInformEventCodes.DIAGNOSTICS_COMPLETE:
                        multiSessionOpType = CpeDeviceOpTypeEnum.Diagnostics;
                        break;

                    case CwmpInformEventCodes.TRANSFER_COMPLETE:
                        multiSessionOpType = CpeDeviceOpTypeEnum.Unknown;
                        break;

                    case CwmpInformEventCodes.M_DOWNLOAD:
                        multiSessionOpType = CpeDeviceOpTypeEnum.Download;
                        break;

                    case CwmpInformEventCodes.M_UPLOAD:
                        multiSessionOpType = CpeDeviceOpTypeEnum.Upload;
                        break;

                    case CwmpInformEventCodes.M_REBOOT:
                        multiSessionOpType = CpeDeviceOpTypeEnum.Reboot;
                        break;

                    case CwmpInformEventCodes.BOOT:
                    case CwmpInformEventCodes.BOOT_STRAP:
                        if (!cpe.bNeedDiscovery) {
                            /**
                             * Check for "FactoryReset" only if the CPE is not newly discovered.
                             *
                             * Both "0 BOOTSTRAP" and "1 BOOT" are set if the reboot was caused by a "FactoryReset"
                             */
                            int bootEventCodeCount = 0;
                            for (int j = 0; j < eventList.sizeOfEventStructArray(); j++) {
                                informEventCodes[j] = eventList.getEventStructArray(j).getEventCode();
                                if (informEventCodes[j].equals(CwmpInformEventCodes.BOOT) ||
                                        informEventCodes[j].equals(CwmpInformEventCodes.BOOT_STRAP)) {
                                    bootEventCodeCount++;
                                }
                            }
                            if (bootEventCodeCount >= 2) {
                                multiSessionOpType = CpeDeviceOpTypeEnum.FactoryReset;
                                bootEventType = EventTypeEnum.FactoryReset;
                            } else {
                                /**
                                 * We may have to check in-progress "Download" device op
                                 */
                                multiSessionOpType = CpeDeviceOpTypeEnum.Unknown;
                                if (informEventCodes[i].equals(CwmpInformEventCodes.BOOT)) {
                                    bootEventType = EventTypeEnum.Reboot;
                                }
                            }

                            /**
                             * Always perform re-discovery on bootstrap for known CPE
                             * (except those decommissioned devices)
                             */
                            if (informEventCodes[i].equals(CwmpInformEventCodes.BOOT_STRAP)) {
                                cpe.bNeedReDiscovery = true;
                            }
                        }
                        break;

                    case CwmpInformEventCodes.VALUE_CHANGE:
                        /**
                         * "4 VALUE CHANGE" Event
                         */

                        /**
                         * TODO: Publish event onto Vert.x event bus
                         */
                        break;
                }
            }
        } else {
            log.error("Malformed Inform! No event list found!");
            return CwmpUtils.getFaultMessage(
                    cwmpVersion,
                    CwmpFaultCodes.ACS_INVALID_ARGS,
                    "Malformed Inform! No event list found!");
        }

        // Persist the Inform message
        receivedCpeMessage.persist(eventBus, cpe, informEventCodes);

        /**
         * Process the parameter list if present
         */
        ParameterValueList parameterList = informRequest.getParameterList();
        if (parameterList != null) {
            /**
             * We are only checking parameters that are needed to identify device type for now.
             */
            for (ParameterValueStruct paramValueStruct : parameterList.getParameterValueStructArray()) {
                cpe.addParamValueStructToSets(paramValueStruct, CwmpMessageTypeEnum.INFORM);
            }

            // Check for SW Upgrades and WAN IP Changes
            if (cpe.sets != null) {
                if (cpe.sets.containsField(Cpe.DeviceId.FIELD_NAME_SW_VER)) {
                    cpeUpgraded = true;
                }
                if (cpe.sets.containsField(Cpe.DB_FIELD_NAME_IP_ADDRESS)) {
                    cpeWanIpInfoChanged = true;
                }
                if (cpe.sets.containsField(Cpe.DB_FIELD_NAME_REGISTRATION_ID)) {
                    cpeRegIdChanged = true;
                }
                if (cpe.sets.containsField(Cpe.DB_FIELD_NAME_CHANGE_COUNTER)) {
                    cpeChangeCounterChanged = true;
                }
            }
        }

        /**
         * TODO: Publish "Inform" event
         */

        /**
         * Is it a new CPE?
         */
        if (cpe.bNeedDiscovery) {
            /**
             * Start Auto-Discovery Process
             */
            DiscoverNewCpe.start(this);
        } else {
            if (cpeUpgraded || cpeWanIpInfoChanged || cpeChangeCounterChanged || cpeRegIdChanged) {
                /**
                 * update the DeviceId
                 */
                cpe.addDeviceIdToSets();
            }

            if (cpe.bNeedReDiscovery) {
                /**
                 * Start Re-Discovery Process
                 */
                ReDiscoverCpe.start(this);
            }

            /**
             * Refresh WAN Connection Info on changes
             */
            if (cpeWanIpInfoChanged) {
                GetWanConnectionParameters.start(this);
            }

            if (cpeUpgraded) {
                /**
                 * Save the device type if new
                 */
                CpeDeviceType.addIfNew(vertx.eventBus(), cpe.deviceId.toDeviceTypeObject());

                /**
                 * Try to Turn On Change Counter Notification if the previous SW version didn't support it
                 */
                if (cpe.deviceId.changeCounter == null && cpe.bNeedReDiscovery == false) {
                    EnableNotifOnChangeCounter.start(this);
                }
            }
        }

        if (bZeroTouchActivation) {
            /**
             * Push down the actual ACS credentials if zero touch
             */
            ZeroTouchActivation.start(this, acsUsername, acsPassword);
        }

        // Create a new InformResponse Message
        CwmpMessage responseMessage = new CwmpMessage(this.cwmpVersion, 1);
        responseMessage.soapEnv.getBody().addNewInformResponse().setMaxEnvelopes(1);

        return responseMessage;
    }

    /**
     * "TransferComplete" RPC Method Handler.
     * @param message
     * @return TransferCompleteResponse or Fault
     */
    CwmpMessage handleTransferComplete(CwmpMessage message) {
        /**
         * Parse the message
         */
        TransferCompleteDocument.TransferComplete transferComplete =
                message.soapEnv.getBody().getTransferComplete();

        // Find in-progress device op by command key
        String commandKey = transferComplete.getCommandKey();
        if (commandKey == null) {
            log.error("Invalid TransferComplete from CPE!");
        } else {
            TransferCompleteFaultStruct fault = transferComplete.getFaultStruct();
            fileTransferResult = new JsonObject();
            if (transferComplete.getStartTime() != null) {
                fileTransferResult.putString("fileTransferStartTime", transferComplete.getStartTime().toString());
            }
            if (transferComplete.getCompleteTime() != null) {
                fileTransferResult.putString("fileTransferCompleteTime", transferComplete.getCompleteTime().toString());
            }
            if (fault != null) {
                if (fault.getFaultCode() == 0) {
                    fileTransferResult.putString(
                            CpeDeviceOp.FIELD_NAME_STATE,
                            CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED
                    );
                } else {
                    fileTransferResult.putString(CpeDeviceOp.FIELD_NAME_STATE, CpeDeviceOp.CPE_DEVICE_OP_STATE_FAILED);
                    fileTransferResult.putString(CpeDeviceOp.FIELD_NAME_ERROR,
                            "File Transfer Failed! TR-069/CWMP Fault Details: " + fault.getFaultCode()
                                    + ": " + fault.getFaultString());

                    // Cleanup incomplete file record
                    AcsFile.cleanupIncompleteUploads(eventBus, cpe.deviceId.sn);

                    /**
                     * TODO: Handle Download Failure
                     */
                }
            } else {
                fileTransferResult.putString(CpeDeviceOp.FIELD_NAME_STATE, CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED);
            }
            transferCompleteCallback();
        }

        // Build Response
        CwmpMessage responseMessage = new CwmpMessage(this.cwmpVersion, 1);
        responseMessage.soapEnv.getBody().addNewTransferCompleteResponse();

        return responseMessage;
    }

    /**
     * Do Download Callback
     */
    public void transferCompleteCallback() {
        if (inProgressDeviceOp == null) {
            // still waiting for more info
            return;
        }

        String deviceOpState = CpeDeviceOp.CPE_DEVICE_OP_STATE_IN_PROGRESS;

        // Try to determine file type
        AcsFileType fileType = AcsFileType.getAcsFileTypeByDeviceOpFileType(
                inProgressDeviceOp.getString(CpeDeviceOp.FIELD_NAME_FILE_TYPE)
        );
        if (AcsFileType.Unknown.equals(fileType)) {
            JsonObject fileStruct = inProgressDeviceOp.getObject(CpeDeviceOp.FIELD_NAME_FILE_STRUCT);
            if (fileStruct != null) {
                fileType = AcsFileType.getAcsFileType(fileStruct.getString(AcsFile.FIELD_NAME_TYPE));
            }
        }

        if (CpeDeviceOpTypeEnum.Download.equals(multiSessionOpType)) {
            /**
             * Download is in progress
             */
            if (fileTransferResult == null) {
                fileTransferResult = inProgressDeviceOp.getObject(CpeDeviceOp.FIELD_NAME_RESULT);
                if (fileTransferResult == null) {
                    // Keep waiting
                    return;
                } else {
                    // CPE has already completed the reboot after file transfer
                    fileTransferResult.putString("rebootCompleteTime", VertxJsonUtils.getIso8601DateString());
                }
            } else {
                /**
                 * The file transfer is now completed (succeeded or failed)
                 */
                if (CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED.equals(
                        fileTransferResult.getString(CpeDeviceOp.FIELD_NAME_STATE))) {
                    /**
                     * Image or Config File Downloads require reboot, while SIP config file downloads do not.
                     *
                     **/
                    if (AcsFileType.SipConfigFile.equals(fileType)) {
                        // Downloading a SIP Config file, no reboot
                    } else {
                        /*
                         * Downloading an Image or IGD Config File.
                         *
                         * Does the Inform also have "1 BOOT" in the event code list?
                         */
                        boolean bHasBootEventCode = false;
                        for (String eventCode : informEventCodes) {
                            if (eventCode.equals(CwmpInformEventCodes.BOOT)) {
                                bHasBootEventCode = true;
                                break;
                            }
                        }
                        if (bHasBootEventCode == false) {
                            /**
                             * No "1 BOOT" in the event code list. We will have to wait for the next "Inform".
                             * Store the file transfer info into Redis
                             */
                            log.info(cpeKey + ": Received transferComplete result, waiting for the next reboot.");
                            inProgressDeviceOp.putObject(CpeDeviceOp.FIELD_NAME_RESULT, fileTransferResult);
                            DeviceOpUtils.saveInProgressDeviceOp(this, inProgressDeviceOp, CpeDeviceOpTypeEnum.Invalid);
                            return;
                        }
                    }
                } else {
                    /**
                     * File Transfer Failed.
                     */
                }
            }
        } else {
            /**
             * Upload
             */
            if (fileTransferResult == null) {
                // Keep waiting
                return;
            }
        }

        // extract state
        deviceOpState = fileTransferResult.getString(CpeDeviceOp.FIELD_NAME_STATE);
        if (multiSessionOpType.equals(CpeDeviceOpTypeEnum.Upload)) {
            // add the internal file id into the upload result
            fileTransferResult.putString(
                    CpeDeviceOp.FIELD_NAME_INTERNAL_FILE_ID,
                    inProgressDeviceOp.getString(CpeDeviceOp.FIELD_NAME_INTERNAL_FILE_ID)
            );
            // add credentials if any
            if (inProgressDeviceOp.containsField(AcsFile.FIELD_NAME_USERNAME)) {
                fileTransferResult.putString(
                        AcsFile.FIELD_NAME_USERNAME,
                        inProgressDeviceOp.getString(AcsFile.FIELD_NAME_USERNAME)
                );
                fileTransferResult.putString(
                        AcsFile.FIELD_NAME_PASSWORD,
                        inProgressDeviceOp.getString(AcsFile.FIELD_NAME_PASSWORD)
                );
            }
        } else if (CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED.equals(state)) {
            if (AcsFileType.Image.equals(fileType)) {
                /**
                 * Image Downloads require reboot.
                 *
                 * Does this Inform has "1 BOOT" in the event code list?
                 */
                boolean bHasBootEventCode = false;
                for (String eventCode : informEventCodes) {
                    if (eventCode.equals(CwmpInformEventCodes.BOOT)) {
                        bHasBootEventCode = true;
                        break;
                    }
                }
                if (bHasBootEventCode == false) {
                    /**
                     * No "1 BOOT" in the event code list. We will have to wait for the next "Inform".
                     * Store the file transfer info into Redis
                     */
                    log.info(cpeKey + ": Received transferComplete result, waiting for the next reboot.");
                    inProgressDeviceOp.putObject(CpeDeviceOp.FIELD_NAME_RESULT, fileTransferResult);
                    DeviceOpUtils.saveInProgressDeviceOp(this, inProgressDeviceOp, CpeDeviceOpTypeEnum.Invalid);
                    return;
                }

                /**
                 * Check if the device is actually running the expected SW version
                 */
                JsonObject fileStruct = inProgressDeviceOp.getObject(CpeDeviceOp.FIELD_NAME_FILE_STRUCT);
                String expectedVersion = null;
                if (fileStruct != null) {
                    expectedVersion = fileStruct.getString(AcsFile.FIELD_NAME_VERSION);
                }
                if (expectedVersion != null) {
                    if (!expectedVersion.equals(cpe.deviceId.swVersion)) {
                        log.error(cpeKey + ": Expect to run version " + expectedVersion + " after download, but found"
                                + cpe.deviceId.swVersion + "!");

                        // Fail the device op with details
                        deviceOpState = CpeDeviceOp.CPE_DEVICE_OP_STATE_FAILED;
                        fileTransferResult.putString(
                                AcsConstants.FIELD_NAME_ERROR,
                                "Device runs version " + cpe.deviceId.swVersion + " after downloading version "
                                        + expectedVersion
                        );
                    } else {
                        log.info(cpeKey + ": Successfully upgraded to version " + expectedVersion);
                    }
                } else {
                    log.info(cpeKey + ": Download op does not have version info:" + inProgressDeviceOp);
                }
            //} else {
                //if (inProgressDeviceOp.getBoolean(CpeDeviceOp.FIELD_NAME_GOLDEN_CONFIG_FILE, false)) {
                    /**
                     * After downloading a golden config file, we need to:
                     *
                     * 1. Cleanup all the parameter values/attributes
                     * 2. Re-do the re-discovery process
                     */
                    //cpe.deleteAllParamValuesAndAttributes(eventBus);
                    //ReDiscoverCpe.start(this);
                //}
            }
        }
        fileTransferResult.removeField(CpeDeviceOp.FIELD_NAME_STATE);

        // Perform Callback
        DeviceOpUtils.callback(
                this,
                inProgressDeviceOp,
                deviceOpState,
                fileTransferResult
        );

        /*
        // Terminate the session and do device-op callback after session info is removed from Redis
        final String finalDeviceOpState = deviceOpState;
        final CwmpSession session = this;
        terminate(
                new VertxRedisUtils.ResultHandler(
                        null,
                        null,
                        new Handler<Long>() {
                            @Override
                            public void handle(Long result) {
                                // Do callback and complete the device op
                                vertx.setTimer(
                                        10000,
                                        new Handler<Long>() {
                                            @Override
                                            public void handle(Long event) {
                                                log.info(session.cpeKey + ": Completing Download Callback...");
                                                DeviceOpUtils.callback(
                                                        session,
                                                        inProgressDeviceOp,
                                                        finalDeviceOpState,
                                                        fileTransferResult
                                                );
                                            }
                                        }
                                );
                            }
                        },
                        null
                )
        );
        */
    }

    /**
     * "GetRPCMethods" RPC Method Handler.
     *
     * @param message
     * @return GetRPCMethodsResponse or Fault
     */
    private static final String[] rpcMethodList = {
            "Inform",
            "GetRPCMethods",
            "TransferComplete"
    };
    CwmpMessage handleGetRPCMethods(CwmpMessage message) {
        // Re-use the original Message and SOAP Envelope
        message.soapEnv.getBody().unsetGetRPCMethods();

        // Build Response
        GetRPCMethodsResponseDocument.GetRPCMethodsResponse response =
                message.soapEnv.getBody().addNewGetRPCMethodsResponse();
        response.addNewMethodList().setStringArray(rpcMethodList);
        response.getMethodList().setArrayType("xsd:string[" + rpcMethodList.length + "]");

        return message;
    }

    /**
     * Send a CWMP Message to CPE with cookie.
     *
     * @param cwmpMessage
     */
    public void sendCwmpMessageToCpe(CwmpMessage cwmpMessage, String cookie) {
        if (log.isDebugEnabled()) {
            log.debug("session vertice " + sessionVertice.verticeIndex + ": " + cpeKey
                    + ": Sending CWMP Message to CPE:\n" + cwmpMessage.toPrettyXmlText());
        }

        if (cookie != null) {
            CwmpHttpRequestHandler.replyWithCookie(
                    httpRequestMessage,
                    HttpResponseStatus.OK,
                    cwmpMessage.toXmlText(),
                    cookie
            );
        } else {
            CwmpHttpRequestHandler.reply(
                    httpRequestMessage,
                    HttpResponseStatus.OK,
                    cwmpMessage.toXmlText(),
                    null
            );
        }

        // Persist this CWMP Message
        if (cpe != null) {
            cwmpMessage.persist(eventBus, cpe, informEventCodes);
        }
    }

    /**
     * Send a CWMP Message to CPE.
     *
     * @param cwmpMessage
     */
    public void sendCwmpMessageToCpe(CwmpMessage cwmpMessage) {
        sendCwmpMessageToCpe(cwmpMessage, null);
    }
}
