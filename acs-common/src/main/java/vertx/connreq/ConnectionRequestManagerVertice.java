package vertx.connreq;

import io.vertx.core.AbstractVerticle;
import vertx.VertxConfigProperties;
import vertx.VertxConstants;
import vertx.VertxHttpClientUtils;
import vertx.VertxRedisUtils;
import vertx.util.AcsConstants;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import redis.clients.jedis.Jedis;

import java.net.MalformedURLException;
import java.util.HashMap;

/**
 * Project:  cwmp
 *
 * Worker Vertice that sends connection requests
 *
 * @author: ronyang
 */
public class ConnectionRequestManagerVertice extends AbstractVerticle {
    private Logger log = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * Async Vertx Redis Client Instance
     */
    private RedisClient redisClient;

    /**
     * Frequently used reply messages
     */
    public JsonObject MALFORMED_URL = new JsonObject()
            .put(ConnectionRequestConstants.STATE, ConnectionRequestFsm.STATE_FAILED)
            .put(ConnectionRequestConstants.ERROR, "Malformed URL!");
    public JsonObject INTERNAL_ERROR = new JsonObject()
            .put(ConnectionRequestConstants.STATE, ConnectionRequestFsm.STATE_FAILED)
            .put(ConnectionRequestConstants.ERROR, "Server Internal Error!");

    /**
     * A Collection of all in-progress Conn-Req FSM POJOs
     */
    public static HashMap<String, ConnectionRequestFsm> connReqFsmHashMap;

    /**
     * New Request Handler
     */
    final Handler<Message<JsonObject>> newReqHandler = new Handler<Message<JsonObject>>() {
        @Override
        public void handle(final Message<JsonObject> reqMessage) {
            handleRequest(reqMessage);
        }
    };

    /**
     * The actual method that handles a conn-req request.
     *
     * @param reqMessage
     */
    public void handleRequest(final Message<JsonObject> reqMessage) {
        JsonObject reqBody = reqMessage.body();
        final String cpeId = reqBody.getString(ConnectionRequestConstants.CPE_ID);
        final String url = reqBody.getString(ConnectionRequestConstants.URL);
        final String username = reqBody.getString(ConnectionRequestConstants.USERNAME);
        final String password = reqBody.getString(ConnectionRequestConstants.PASSWORD);
        final HttpClient httpClient;
        log.info("Received a Connection-Request request for CPE " + cpeId);

        // Proxy
        final String proxy = reqBody.getString(ConnectionRequestConstants.PROXY);

        // Build HTTP Client by URL String (in order to validate the URL)
        try {
            if (proxy == null) {
                httpClient = VertxHttpClientUtils.createHttpClient(vertx, url);
            } else {
                httpClient = vertx.createHttpClient()
                        .setHost(proxy)
                        .setPort(ConnectionRequestConstants.INTERNAL_PROXY_PORT);
            }
        } catch (MalformedURLException e) {
            reqMessage.reply(MALFORMED_URL);
            return;
        }

        /**
         * Store the new conn-req into Redis only if it does not exist now.
         */
        VertxRedisUtils.set(
                redisClient,
                ConnectionRequestUtils.getConnReqRedisKeyByCpeId(cpeId),
                ConnectionRequestFsm.NEW_CONN_REQ_FSM_STATE_STRING,
                ConnectionRequestConstants.DEFAULT_CONN_REQ_TIMEOUT,
                true,       // not exist only
                false,      // exist only
                new Handler<String> () {    // result handler
                    @Override
                    public void handle(String result) {
                        if (VertxRedisUtils.OK.equals(result)) {
                            // kick off the FSM
                            ConnectionRequestFsm fsm = new ConnectionRequestFsm(
                                    vertx,
                                    redisClient,
                                    cpeId,
                                    proxy,
                                    httpClient,
                                    url,
                                    username,
                                    password
                            );
                            fsm.send(null);

                            // Simply reply to sender that this new request has been successfully en-queued.
                            reqMessage.reply(fsm.fsmInfo);
                        } else {
                            // Conn-Req state machine has already been started for this CPE
                            log.info("Conn-Req state machine has already been started for CPE " + cpeId
                                    + "(" + result + ")");

                            // Read the current Conn-Req State (FSM) Info and forward to ACS API server
                            ConnectionRequestUtils.getConnReqStateByCpeId(
                                    redisClient,
                                    cpeId,
                                    new Handler<String>() {
                                        @Override
                                        public void handle(String fsmInfoString) {
                                            if (fsmInfoString == null) {
                                                /**
                                                 * Race Condition!!!!
                                                 *
                                                 * The session just got terminated by the CPE server
                                                 *
                                                 * Re-try in 5 seconds.
                                                 */
                                                vertx.setTimer(5000, new Handler<Long>() {
                                                    @Override
                                                    public void handle(Long event) {
                                                        handleRequest(reqMessage);
                                                    }
                                                });
                                            } else {
                                                /**
                                                 * Retrieved the current conn-req/session info
                                                 */
                                                try {
                                                    reqMessage.reply(new JsonObject(fsmInfoString));
                                                } catch (Exception ex) {
                                                    log.error("Received invalid conn-req/session info from Redis "
                                                            + "for CPE " + cpeId + "! (" + fsmInfoString + ")");
                                                    reqMessage.reply(INTERNAL_ERROR);
                                                }
                                            }
                                        }
                                    }
                            );
                        }
                    }
                }
        );
    }

    /**
     * Start the Vertice
     */
    @Override
    public void start() {
        log.info("ConnectionRequest Worker Vertice is starting up..\n");

        /**
         * Create Redis Client
         */
        redisClient = new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);

        /**
         * Register Connection-Request Request Handler
         */
        vertx.eventBus().registerHandler(
                AcsConstants.VERTX_ADDRESS_ACS_CONNECTION_REQUEST,
                newReqHandler
        );

        /**
         * Initialize FSM Hash Map
         */
        connReqFsmHashMap = new HashMap<>();
    }

    @Override
    public void stop() {
        log.info("ConnectionRequest Worker Vertice is shutting down...\n");

        /**
         * Un-Register Connection-Request Request Handler
         */
        vertx.eventBus().unregisterHandler(
                AcsConstants.VERTX_ADDRESS_ACS_CONNECTION_REQUEST,
                newReqHandler
        );

        /**
         * Clean up all redis keys via Jedis (as Vertx mod redis is no longer available to us)
         */
        if (connReqFsmHashMap.size() > 0) {
            Jedis jedisClient = new Jedis(VertxConfigProperties.redisHost, VertxConfigProperties.redisPort);

            for (ConnectionRequestFsm fsm : connReqFsmHashMap.values()) {
                fsm.destroy(jedisClient);
            }
        }
    }
}

