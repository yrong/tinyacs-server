package vertx2.connreq;

import vertx2.VertxException;
import vertx2.VertxMongoUtils;
import vertx2.VertxRedisUtils;
import vertx2.VertxUtils;
import vertx2.cwmp.CwmpMessage;
import vertx2.model.Cpe;
import vertx2.model.CpeIdentifier;
import vertx2.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.java.redis.RedisClient;
import org.apache.http.Header;
import org.apache.http.auth.*;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.auth.DigestScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonObject;
import redis.clients.jedis.Jedis;

/**
 * Project:  cwmp
 *
 * Connection Request Finite State Machine.
 *
 * Run curl command "curl --digest -v -u "admin:admin" http://10.201.8.10:30005/" to see how Digest Auth works.
 *
 * TODO: Add support for XMPP.
 *
 * @author: ronyang
 */
public class ConnectionRequestFsm {
    private static Logger log = LoggerFactory.getLogger(ConnectionRequestFsm.class);

    /**
     * States
     */
    // Sending connection-request
    public static final String STATE_SENDING = "sending";
    // Sent connection-request to CPE, waiting for session
    public static final String STATE_SENT = "sent";
    // Sent connection-request to CPE, waiting for session
    public static final String STATE_FAILED = "failed";
    // An active CWMP session is now up
    public static final String STATE_SESSION = "session";
    // Locked due to Diag/Download/etc In Progress
    public static final String STATE_LOCKED = "locked";

    /**
     * Result Objects (used when saving to communication logs
     */
    public static final JsonObject RESULT_SUCCEEDED = new JsonObject().putString("status", "Succeeded");
    public static final JsonObject RESULT_AUTH_FAILURE = new JsonObject().putString("status", "Authentication Failure");

    /**
     * New State Machine String
     */
    public static final JsonObject NEW_CONN_REQ_FSM_STATE = new JsonObject()
            .putString(ConnectionRequestConstants.STATE, STATE_SENDING)
            .putString(ConnectionRequestConstants.CONN_REQ_MANAGER,VertxUtils.getHostnameAndPid());
    public static final String NEW_CONN_REQ_FSM_STATE_STRING = NEW_CONN_REQ_FSM_STATE.encode();

    /**
     * TODO: add soaking period after failures
     */

    // Vertx
    Vertx vertx;
    // Redis Client
    RedisClient redisClient;

    // Original Request JSON Object
    String cpeId;
    JsonObject fsmInfo;

    String redisKey;

    // URL String and Credential
    String url;
    String username;
    String password;

    // HTTP Get
    HttpGet httpGet;
    // HTTP Client
    HttpClient httpClient;
    // Proxy
    String proxy;

    /**
     * Two steps:
     * 1. Send an empty HTTP, and wait for challenge
     * 2. Upon the challenge, send the actual authentication header
     */
    boolean bWaitingForChallenge = true;

    // Done Indicator
    boolean bDone = false;

    // Result
    JsonObject result = RESULT_SUCCEEDED;


    /**
     * Constructor by the original request sent from ACS API server.
     *
     * @param vertx
     * @param redisClient
     * @param cpeId
     * @param url
     * @param username
     * @param password
     */
    public ConnectionRequestFsm(
            Vertx vertx,
            RedisClient redisClient,
            String cpeId,
            String proxy,
            HttpClient httpClient,
            String url,
            String username,
            String password) {
        this.cpeId = cpeId;
        this.vertx = vertx;
        this.redisClient = redisClient;
        this.httpClient = httpClient;
        this.proxy = proxy;
        this.url = url;
        this.username = username;
        this.password = password;
        redisKey = ConnectionRequestUtils.getConnReqRedisKeyByCpeId(cpeId);
        fsmInfo = NEW_CONN_REQ_FSM_STATE.copy();
        httpGet = new HttpGet(url);

        /**
         * Add to Hash Map
         */
        ConnectionRequestManagerVertice.connReqFsmHashMap.put(cpeId, this);
    }

    // HTTP Response Handler
    final Handler<HttpClientResponse> httpResponseHandler = new Handler<HttpClientResponse>() {
        @Override
        public void handle(HttpClientResponse response) {
            if (bDone == true) {
                log.error("Unexpected response received after the FSM is done.");
                return;
            }

            String resultString = null;
            boolean bSent = false;

            if (bWaitingForChallenge) {
                if (response.statusCode() == HttpResponseStatus.OK.code()) {
                    // The CPE is probably a simulator which does not require any authentication
                    bSent = true;
                } else if (response.statusCode() == HttpResponseStatus.UNAUTHORIZED.code()) {
                    try {
                        DigestScheme digestScheme = new DigestScheme();
                        String challenge = response.headers().get(AUTH.WWW_AUTH);
                        log.info("Received auth challenge " + challenge);

                        Header authChanHeader = new BasicHeader(AUTH.WWW_AUTH, challenge);
                        digestScheme.processChallenge(authChanHeader);

                        // Produce auth header
                        AuthScope authScope = new AuthScope(httpGet.getURI().getHost(), httpGet.getURI().getPort());
                        if (password == null || password.equals("")) {
                            password = username;
                        }
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(
                                authScope,
                                new UsernamePasswordCredentials(username, password)
                        );
                        Credentials credentials = credentialsProvider.getCredentials(authScope);
                        Header authHeader;
                        authHeader = digestScheme.authenticate(
                                credentials,
                                httpGet,
                                null
                        );

                        // Send the auth string to CPE
                        bWaitingForChallenge = false;
                        send(authHeader.getValue());
                    } catch (AuthenticationException e) {
                        resultString = "Authentication Failure " + e.getMessage();
                        result = RESULT_AUTH_FAILURE;
                        bSent = true;
                    } catch (MalformedChallengeException e) {
                        doCallback("Received Malformed Auth Challenge from CPE!");
                        resultString = "Authentication Failure (received Malformed HTTP Digest Auth Challenge from target device)";
                        result = RESULT_AUTH_FAILURE;
                        bSent = true;
                    }

                } else {
                    // Unexpected response status code
                    resultString = response.statusMessage();
                    result = new JsonObject().putString("status", "Unexpected HTTP response code " + resultString);
                    bSent = true;
                }
            } else {
                bSent = true;
                if (response.statusCode() != HttpResponseStatus.OK.code()) {
                    resultString = response.statusMessage();
                    result = new JsonObject().putString("status", "Unexpected HTTP response code " + resultString);
                }
            }

            if (bSent == true) {
                /**
                 * Connection-Request has been sent either successfully or failed
                 */
                // Close the HTTP Client either way
                httpClient.close();

                // Update Redis
                if (resultString == null) {
                    // sent successfully
                    log.info(cpeId + ": Successfully sent conn-req.");
                    //updateRedis(STATE_SENT, null, ConnectionRequestConstants.DEFAULT_CONN_REQ_TIMEOUT * 2);
                } else {
                    // Failed
                    log.error(cpeId + ": Failed to send conn-req due to " + resultString + "!");

                    /**
                     * Store this failure in Redis for up to 10 seconds which will make sure no more conn-req will
                     * sent to the same CPE in the next 10 seconds
                     */
                    updateRedis(
                            STATE_FAILED,
                            resultString,
                            ConnectionRequestConstants.DEFAULT_FAILURE_SOAKING_TIMEOUT
                    );
                }

                /**
                 * Remove from Hash Map
                 */
                ConnectionRequestManagerVertice.connReqFsmHashMap.remove(cpeId);

                // Do callbacks to notify the ACS API servers
                doCallback(resultString);

                // Save communication logs
                saveCommunicationLog(result);

                bDone = true;
            }
        }
    };

    // HTTP Exception Handler
    Handler<Throwable> httpExceptionHandler = new Handler<Throwable>() {
        @Override
        public void handle(Throwable ex) {
            if (bDone == true) {
                log.error("Unexpected exception caught after the FSM is done. (" + ex.getMessage() + ")");
                return;
            }
            log.error("Caught exception while sending connection request to CPE due to\" " + ex.getMessage()
                    + "\", target url: " + url);

            /**
             * Remove from Hash Map
             */
            ConnectionRequestManagerVertice.connReqFsmHashMap.remove(cpeId);

            result = new JsonObject().putString("status", ex.getMessage());
            // Save communication logs
            saveCommunicationLog(result);

            updateRedis(
                    STATE_FAILED,
                    ex.getMessage(),
                    ConnectionRequestConstants.DEFAULT_FAILURE_SOAKING_TIMEOUT
            );
            doCallback(ex.getMessage());
            httpClient.close();
            bDone = true;
        }
    };

    /**
     * Update the FSM Info in Redis.
     *
     * @param newState
     * @param error
     * @param timeout
     */
    public void updateRedis(
            String newState,
            String error,
            long timeout) {
        fsmInfo.putString(ConnectionRequestConstants.STATE, newState);

        if(error != null) {
            fsmInfo.putString(ConnectionRequestConstants.ERROR, error);
        } else {
            fsmInfo.removeField(ConnectionRequestConstants.ERROR);
        }

        if (timeout > 0) {
            /**
             * Store this failure in Redis for up to 10 seconds which will make sure no more conn-req will
             * sent to the same CPE in the next 10 seconds
             */
            VertxRedisUtils.set(
                    redisClient,
                    redisKey,
                    fsmInfo.encode(),
                    timeout,
                    false,
                    false,
                    null
            );
        } else {
            VertxRedisUtils.set(
                    redisClient,
                    redisKey,
                    fsmInfo.encode()
            );
        }

    }

    /**
     * Send a request with an optional header string
     * @param authHeader
     */
    public void send(String authHeader) {
        // HTTP Client Request
        String destinationUrlPath;
        if (proxy == null) {
            destinationUrlPath = httpGet.getURI().getPath();
        } else {
            /**
             * When using proxy, the destination URL path is the full URL
             */
            destinationUrlPath = url;
        }
        HttpClientRequest clientRequest = httpClient.request(
                "GET",
                destinationUrlPath,
                httpResponseHandler
        );
        clientRequest.exceptionHandler(httpExceptionHandler);
        clientRequest.headers().set("User-Agent", "Calix cwmp");
        clientRequest.headers().set("Accept", "*/*");
        // Set timeout (in ms)
        clientRequest.setTimeout(ConnectionRequestConstants.DEFAULT_CONN_REQ_TIMEOUT);

        // Auth Header
        if (authHeader != null) {
            log.info("Sending auth string " + authHeader);
            clientRequest.headers().set(AUTH.WWW_AUTH_RESP, authHeader);
        }

        clientRequest.end();
    }

    /**
     * TODO: this is not immediately needed initially, but good to keep track of it.
     *
     * After a connection request is sent to the CPE successfully or failed to do so, all callbacks that were
     * previously registered shall be called thus the API servers will know about the result of this conn-req request.
     *
     * @param error An error string on failures, or null if succeeded.
     */
    public void doCallback(String error) {
        /**
         * Get all the callback URLs from Redis
         */

        /**
         * Send result to all Callback URLs
         */
    }

    /**
     * Save the connection-request into the communication logs (i.e. the CWMP message table)
     * @param result
     */
    public void saveCommunicationLog(JsonObject result) {
        JsonObject dbObject = new JsonObject()
                .putString(AcsConstants.FIELD_NAME_ORG_ID, Cpe.getOrgIdByCpeKey(cpeId))
                .putObject(
                        AcsConstants.FIELD_NAME_CPE_ID,
                        new JsonObject().putString(CpeIdentifier.FIELD_NAME_SN, Cpe.getSnByCpeKey(cpeId))
                )
                .putObject(CwmpMessage.DB_FIELD_NAME_TIMESTAMP, VertxMongoUtils.getDateObject())
                .putString(CwmpMessage.DB_FIELD_NAME_TYPE, "Connection Request")
                .putObject(CwmpMessage.DB_FIELD_NAME_SUMMARY, result)
                .putNumber(CwmpMessage.DB_FIELD_NAME_SN, 0)
                // Expire in one week
                .putObject(
                        CwmpMessage.DB_FIELD_NAME_EXPIRE_AT,
                        VertxMongoUtils.getDateObject(System.currentTimeMillis() + CwmpMessage.DEFAULT_TTL)
                );

        // Persist it
        try {
            VertxMongoUtils.save(
                    vertx.eventBus(),
                    CwmpMessage.DB_COLLECTION_NAME,
                    dbObject,
                    null
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Remove this state machine from Redis via blocking call (Jedis)
     * @param jedisClient
     */
    public void destroy(Jedis jedisClient) {
        /**
         * Remove Conn-Req Info from Redis
         */
        log.info("Removing Conn-Req Info for CPE " + cpeId + " from Redis...");
        jedisClient.del(redisKey);
    }
}
