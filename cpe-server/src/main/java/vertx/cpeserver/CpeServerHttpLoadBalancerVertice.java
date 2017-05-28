package vertx.cpeserver;

import vertx.VertxUtils;
import vertx.cpeserver.httpauth.PerOrgAuthenticator;
import vertx.cpeserver.session.CwmpSessionCookieUtils;
import vertx.cwmp.CwmpFaultCodes;
import vertx.cwmp.CwmpMessage;
import vertx.cwmp.CwmpUtils;
import vertx.fileserver.FileServerRequestHandler;
import vertx.util.AcsConfigProperties;
import vertx.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.auth.AUTH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.platform.Verticle;

/**
 * Project:  SXA CC CPE Server
 *
 * This is the Raw HTTP Server Vertice which functions as sticky-session HTTP load balancer that always distributes
 * HTTP requests from the same session to the same Session Vertice.
 *
 * HTTP (Digest) Authentication is also handled within the HTTP Load Balancer Vertice.
 *
 * @author: ronyang
 */
public class CpeServerHttpLoadBalancerVertice extends Verticle {
    private final Logger log = LoggerFactory.getLogger(CpeServerHttpLoadBalancerVertice.class.getName());

    /**
     * The index of the next CWMP Session Vertice Index
     */
    int nextSessionVerticeIndex = 0;

    /**
     * Default Timeout is 5 minutes
     */
    final long DEFAULT_TIMEOUT = 300000;

    /**
     * Per-Org Authenticator Cache
     */
    public PerOrgAuthenticatorCache authenticatorCache;

    /**
     * File Server Http Request Handler
     */
    public FileServerRequestHandler fileServerRequestHandler;

    /**
     * Frequently Used Fault CWMP Message Strings
     */
    public static final String INTERNAL_TIMEOUT =
            CwmpUtils.getFaultMessage(
                    CwmpMessage.DEFAULT_CWMP_VERSION,
                    CwmpFaultCodes.ACS_INTERNAL_ERROR,
                    "Internal Timeout!"
            ).toXmlText();
    public static final String INVALID_COOKIE =
            CwmpUtils.getFaultMessage(
                    CwmpMessage.DEFAULT_CWMP_VERSION,
                    CwmpFaultCodes.ACS_REQUEST_DENIED,
                    "Invalid Cookie!"
            ).toXmlText();
    public static final String INVALID_URL =
            CwmpUtils.getFaultMessage(
                    CwmpMessage.DEFAULT_CWMP_VERSION,
                    CwmpFaultCodes.ACS_REQUEST_DENIED,
                    "Invalid ACS URL!"
            ).toXmlText();

    /**
     * Start the Vertice
     */
    public void start() {
        /**
         * Initialize Authenticator Cache
         */
        authenticatorCache = new PerOrgAuthenticatorCache(vertx);

        /**
         * Initialize File Server Request Handler
         */
        fileServerRequestHandler = new FileServerRequestHandler(vertx);

        /**
         * Start the HTTP server
         */
        HttpServer server = vertx.createHttpServer();
        server.setAcceptBacklog(10000);
        server.requestHandler(requestHandler);
        server.listen(AcsConfigProperties.CPE_SERVER_LB_PORT);
        log.info(VertxUtils.highlightWithHashes("CPE Server Base URL: " + AcsConfigProperties.CPE_SERVER_BASE_URL));
    }

    /**
     * Raw HTTP Request Handler
     */
    Handler<HttpServerRequest> requestHandler = new Handler<HttpServerRequest>() {
        @Override
        public void handle(final HttpServerRequest request) {
            /**
             * Is it for File Server?
             */
            if (FileServerRequestHandler.isFileServerRequest(request)) {
                fileServerRequestHandler.handle(request);
                return;
            }

            /**
             * The process starts after the entire body has been received
             */
            request.bodyHandler(new Handler<Buffer>() {
                public void handle(Buffer body) {
                    try {
                        final JsonObject message = new JsonObject();
                        Integer sessionVerticeIndex = null;

                        // Pass the ACS Hostname to Session Vertice
                        String rawHostString = request.headers().get("Host");
                        if (rawHostString != null) {
                            message.putString(
                                    CpeServerConstants.FIELD_NAME_ACS_HOST,
                                    // Extract the hostname
                                    rawHostString.substring(0, rawHostString.indexOf(":"))
                            );
                        }

                        /**
                         * Check Cookie
                         */
                        String cookie = request.headers().get("Cookie");
                        if (cookie == null) {
                            /**
                             * Rebuild the ACS URL that is used by this CPE
                             */
                            String acsUrl = CpeServerConstants.ACS_URL_PROTOCOL_PREFIX
                                    + request.headers().get("HOST") + request.path();
                            if (request.path() == null || request.path().equals("")) {
                                // Add a trailing "/
                                acsUrl += "/";
                            }
                            log.debug("Extracted ACS URL from HTTP Header: " + acsUrl);

                            /**
                             * Do Authentication
                             */
                            // Get per-Org Authenticator Instance by ACS URL
                            PerOrgAuthenticator authenticator = authenticatorCache.getAuthenticatorByAcsUrl(acsUrl);
                            if (authenticator == null) {
                                // Try again with just the URL path for compatibility
                                authenticator = authenticatorCache.getAuthenticatorByAcsUrl(request.path());
                            }
                            if (authenticator == null) {
                                // Send the HTTP Digest Challenge which may force the CPE to include the "HOST" header
                                log.error("Received request on unexpected URL path " + acsUrl + "!");
                                sendResponse(request, HttpResponseStatus.FORBIDDEN, INVALID_URL);
                                return;
                            }

                            // Extract Auth Response Header
                            String authHeader = request.headers().get(AUTH.WWW_AUTH_RESP);
                            if (authHeader != null) {
                                log.debug("Received " + AUTH.WWW_AUTH_RESP + ": " + authHeader);
                                message.putString(CpeServerConstants.FIELD_NAME_AUTH_HEADER, authHeader);

                                // Verify Auth Response Header
                                if (authenticator.verifyAuthHeader(authHeader)) {
                                    // Request passed authentication
                                } else if (authenticator.hasZeroTouchCredentials(authHeader)) {
                                    log.info("Detected a Zero-Touch Activation Request.");
                                    // Request does not have the real credentials but has zero-touch credentials
                                    message.putBoolean(CpeServerConstants.FIELD_NAME_ZERO_TOUCH, true);
                                    message.putString(
                                            CpeServerConstants.FIELD_NAME_ACS_USERNAME,
                                            authenticator.acsUsername
                                    );
                                    message.putString(
                                            CpeServerConstants.FIELD_NAME_ACS_PASSWORD,
                                            authenticator.acsPassword
                                    );
                                } else {
                                    sendResponse(request, HttpResponseStatus.UNAUTHORIZED, null);
                                    return;
                                }

                                // Add Org Id
                                message.putString(AcsConstants.FIELD_NAME_ORG_ID, authenticator.id);

                                // Pick the next session vertice in a round-robin fashion
                                sessionVerticeIndex = nextSessionVerticeIndex;
                                nextSessionVerticeIndex = (nextSessionVerticeIndex + 1) %
                                        CpeServerConstants.NUMBER_OF_SESSION_VERTICES;
                            } else {
                                // Send Auth Challenge
                                request.response().putHeader(AUTH.WWW_AUTH, authenticator.getChallengeString());
                                sendResponse(request, HttpResponseStatus.UNAUTHORIZED, null);
                                return;
                            }
                        } else {
                            // Found a cookie in request header
                            sessionVerticeIndex = CwmpSessionCookieUtils.getSessionVerticeIndexFromCookie(cookie);
                            if (sessionVerticeIndex == null) {
                                sendResponse(request, HttpResponseStatus.BAD_REQUEST, INVALID_COOKIE);
                                return;
                            }

                            // Pass the cookie string to session vertice
                            message.putString(CpeServerConstants.FIELD_NAME_COOKIE, cookie);
                        }

                        // Build the destination event bus address by session vertice index
                        final String ebAddress = CpeServerConstants.CWMP_SESSION_VERTICE_ADDRESS_PREFIX
                                + sessionVerticeIndex;
                        log.debug("Forwarding request to " + ebAddress);

                        /**
                         * Payload
                         */
                        if (body.length() > 0) {
                            message.putString(CpeServerConstants.FIELD_NAME_BODY, body.toString());
                        }

                        /**
                         * Send the request to Session vertice via event bus
                         */
                        vertx.eventBus().sendWithTimeout(
                                ebAddress,
                                message,
                                DEFAULT_TIMEOUT,
                                new AsyncResultHandler<Message<JsonObject>>() {
                                    @Override
                                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                                        if (asyncResult.failed()) {
                                            log.error("Failed to receive reply from " + ebAddress
                                                    + "due to " + asyncResult.cause() + "!");
                                            log.error("Original Request Message:\n" + message.encodePrettily());
                                            sendResponse(request, HttpResponseStatus.OK, INTERNAL_TIMEOUT);
                                            return;
                                        }

                                        JsonObject response = asyncResult.result().body();
                                        //log.debug("Received reply:\n" + response.encodePrettily());

                                        /**
                                         * Check Cookie
                                         */
                                        String cookie = response.getString(CpeServerConstants.FIELD_NAME_COOKIE);
                                        if (cookie != null) {
                                            request.response().putHeader("Set-Cookie", cookie);
                                        }

                                        /**
                                         * Check Status Code
                                         */
                                        int statusCode = response.getInteger(
                                                CpeServerConstants.FIELD_NAME_STATUS_CODE,
                                                HttpResponseStatus.OK.code()
                                        );
                                        HttpResponseStatus status = HttpResponseStatus.valueOf(statusCode);
                                        if (!status.equals(HttpResponseStatus.OK)) {
                                            VertxUtils.setResponseStatus(request, HttpResponseStatus.valueOf(statusCode));

                                            if (statusCode == HttpResponseStatus.UNAUTHORIZED.code()) {
                                                // Auth Challenge
                                                if (response.containsField(CpeServerConstants.FIELD_NAME_AUTH_CHALLENGE)) {
                                                    request.response().putHeader(
                                                            AUTH.WWW_AUTH,
                                                            response.getString(CpeServerConstants.FIELD_NAME_AUTH_CHALLENGE)
                                                    );
                                                }
                                            } else if (statusCode == HttpResponseStatus.NO_CONTENT.code()) {
                                                request.response().putHeader("Content-Type", "text/xml; charset=\"utf-8\"");
                                                request.response().putHeader("SOAPAction", "\"\"");
                                            }
                                        }

                                        /**
                                         * Body
                                         */
                                        String body = response.getString(CpeServerConstants.FIELD_NAME_BODY);
                                        sendResponse(request, status, body);
                                    }
                                }
                        );
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
        }
    };

    /**
     * Send response.
     *
     * @param request
     * @param httpResponseStatus
     * @param payload
     */
    public void sendResponse(
            HttpServerRequest request,
            HttpResponseStatus httpResponseStatus,
            String payload) {
        VertxUtils.setResponseStatus(request, httpResponseStatus);
        if (payload != null) {
            request.response().putHeader("Content-Type", "text/xml; charset=\"utf-8\"");
            request.response().putHeader("Content-Length", String.valueOf(payload.length()));
            request.response().end(payload, "UTF-8");
        } else {
            if (httpResponseStatus.equals(HttpResponseStatus.NO_CONTENT)) {
                // Empty Response
                log.debug("Terminating a session...");
            }
            request.response().end();
        }
    }
}
