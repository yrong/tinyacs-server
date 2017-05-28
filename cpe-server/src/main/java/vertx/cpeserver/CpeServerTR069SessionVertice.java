package vertx.cpeserver;

import vertx.VertxConfigProperties;
import vertx.VertxHttpClientUtils;
import vertx.VertxUtils;
import vertx.cache.DialPlanCache;
import vertx.cache.OrganizationCache;
import vertx.connreq.ConnectionRequestConstants;
import vertx.connreq.ConnectionRequestFsm;
import vertx.cpeserver.session.CwmpHttpRequestHandler;
import vertx.cpeserver.session.CwmpSession;
import vertx.cpeserver.session.CwmpSessionCookieUtils;
import vertx.model.CpeDeviceOp;
import vertx.model.Organization;
import vertx.util.AcsConstants;
import vertx.util.CpeDataModelMgmt;
import vertx.util.sxajboss.SxaJBossApiUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.platform.Verticle;
import redis.clients.jedis.Jedis;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Project:  SXA CC CPE Server
 *
 * This is the TR-CWMP Session Vertice (behind the 069 HTTP Server Vertice).
 *
 * @author: ronyang
 */
public class CpeServerTR069SessionVertice extends Verticle {
    private final Logger log = LoggerFactory.getLogger(CpeServerTR069SessionVertice.class.getName());

    /**
     * Session Vertice Index
     */
    public int verticeIndex;

    /**
     * The session HashMap <CPE ID String --> Session POJO>
     */
    public ConcurrentHashMap<String, CwmpSession> sessions = new ConcurrentHashMap<String, CwmpSession>();;

    /**
     * Static Session Info String
     */
    public String CONN_REQ_FSM_INFO_STRING = null;

    /**
     * Organization Cache
     */
    public OrganizationCache organizationCache;

    /**
     * DialPlan Cache Cache
     */
    public DialPlanCache dialPlanCache;

    /**
     * Frequently Used JSON Object
     */
    public static final JsonObject OK =
            new JsonObject().putString(AcsConstants.FIELD_NAME_STATUS_CODE, HttpResponseStatus.OK.toString());
    public static final JsonObject NULL_POINTER =
            new JsonObject()
                    .putString(AcsConstants.FIELD_NAME_STATUS_CODE, HttpResponseStatus.BAD_REQUEST.toString())
                    .putString(AcsConstants.FIELD_NAME_ERROR, "Null Pointer(s)!");
    public static final JsonObject NO_SUCH_SESSION =
            new JsonObject()
                    .putString(AcsConstants.FIELD_NAME_STATUS_CODE, HttpResponseStatus.NOT_FOUND.toString())
                    .putString(AcsConstants.FIELD_NAME_ERROR, "No session found for the given CPE id!");

    /**
     * Start the Vertice
     */
    public void start() {
        /**
         * Read Vertice Index from config
         */
        verticeIndex = container.config().getInteger(CpeServerConstants.FIELD_NAME_VERTICE_INDEX);
        log.info("Starting Session Vertice " + verticeIndex + "...");

        /**
         * Initialize Static Session Info String
         */
        CONN_REQ_FSM_INFO_STRING = new JsonObject()
                .putString(ConnectionRequestConstants.STATE, ConnectionRequestFsm.STATE_SESSION)
                .putString(ConnectionRequestConstants.CWMP_SESSION_MANAGER,
                        VertxUtils.getHostnameAndPid() + "~" + verticeIndex)
                .encode();

        /**
         * Register a handler to receive direct device-op requests via Event Bus
         */
        vertx.eventBus().registerHandler(
                AcsConstants.VERTX_ADDRESS_ACS_DEVICE_OP_REQUEST_PREFIX
                        + VertxUtils.getHostnameAndPid() + "~" + verticeIndex,
                DIRECT_DEVICE_OP_REQ_HANDLER
        );

        /**
         * Create another local cache of all organizations for other purposes
         */
        organizationCache = new OrganizationCache(
                vertx,
                AcsConstants.VERTX_ADDRESS_ACS_ORGANIZATION_CRUD,
                Organization.DB_COLLECTION_NAME,
                Organization.class.getSimpleName()
        );

        /**
         * Create DialPlan Cache
         */
        dialPlanCache = new DialPlanCache(vertx);

        /**
         * Initialize the Vertx HTTP Client Utils
         */
        VertxHttpClientUtils.init(vertx);

        /**
         * Initialize the HTTP Client for SXA JBoss API Utils
         */
        SxaJBossApiUtils.initHttpClient(vertx);

        /**
         * Initialize CPE Device Data Models
         */
        CpeDataModelMgmt.init(vertx, "tr_data_models");

        /**
         * Register Handler for HTTP Request (forwarded from the HTTP Load Balancer Vertices)
         */
        vertx.eventBus().registerLocalHandler(
                CpeServerConstants.CWMP_SESSION_VERTICE_ADDRESS_PREFIX + String.valueOf(verticeIndex),
                new CwmpHttpRequestHandler(vertx, this)
        );
    }

    /**
     * Stop Method for cleanup.
     */
    @Override
    public void stop() {
        /**
         * Clean up all the sessions stored in Redis
         */
        if (sessions.size() > 0) {
            log.info("Cleaning up all Redis Session Keys...  (# of sessions = " + sessions.size() + ")");
            Jedis jedisClient = new Jedis(VertxConfigProperties.redisHost, VertxConfigProperties.redisPort);

            for (CwmpSession session : sessions.values()) {
                session.removeSessionInfoFromRedis(jedisClient);
            }
        }
    }

    /**
     * Direct Device-Op Request Handler
     */
    public final Handler<Message<JsonObject>> DIRECT_DEVICE_OP_REQ_HANDLER = new Handler<Message<JsonObject>>() {
        @Override
        public void handle(final Message<JsonObject> reqMessage) {
            JsonObject reqBody = reqMessage.body();
            String cpeId = reqBody.getString(AcsConstants.FIELD_NAME_ID);
            JsonObject deviceOp = reqBody.getObject(CpeDeviceOp.FIELD_NAME_DEVICE_OP);

            // Validate pointers
            if (cpeId == null || deviceOp == null) {
                reqMessage.reply(NULL_POINTER);
            }

            // Lookup session
            CwmpSession session = sessions.get(cpeId);
            if (session == null) {
                log.info("Unable to find a session for " + cpeId
                        + ". hash map has " + sessions.size() + " session(s) in it.");
                reqMessage.reply(NO_SUCH_SESSION);
            } else {
                // Enqueue this request to the session's queue
                session.processRawDeviceOp(deviceOp);

                reqMessage.reply(OK);
            }
        }
    };

    /**
     * Get CWMP Session by Cookie
     * @param cookie
     * @return
     */
    public CwmpSession getSessionByCookie(String cookie) {
        return sessions.get(CwmpSessionCookieUtils.getCpeKeyFromCookie(cookie));
    }

    /**
     * Get CWMP Session by CPE Key
     * @param cpeKey
     * @return
     */
    public CwmpSession getSessionByCpeKey(String cpeKey) {
        return sessions.get(cpeKey);
    }

    /**
     * Add a new session.
     * @param session
     */
    public void addSession(CwmpSession session) {
        log.info("Adding new CWMP Session " + session.cpeKey);
        sessions.put(session.cpeKey, session);
    }

    /**
     * Delete an existing session from the HashMap
     * @param session
     */
    public void delSession(CwmpSession session) {
        if (session != null) {
            log.info(session.cpeKey + ": Deleting session.");
            sessions.remove(session.cpeKey);
        } else {
            log.error("Null pointer!");
        }
    }
}
