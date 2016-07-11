package vertx2.cpeserver.session;

import vertx2.VertxConstants;
import vertx2.cpeserver.CpeServerConstants;
import vertx2.cpeserver.CpeServerTR069SessionVertice;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpFaultCodes;
import vertx2.cwmp.CwmpMessage;
import vertx2.cwmp.CwmpUtils;
import vertx2.util.AcsConfigProperties;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.impl.JsonObjectMessage;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  cwmp CPE Server
 *
 * Handle the "HTTP" Requests (wrapped as JsonObject Messages) forwarded from the HTTP Load Balancer Vertices.
 *
 * @author: ronyang
 */
public class CwmpHttpRequestHandler implements Handler<JsonObjectMessage>{
    private static final Logger log = LoggerFactory.getLogger(CwmpHttpRequestHandler.class.getName());

    /**
     * CPE ACS URL Protocol Prefix ("http" vs. "https")
     */
    public static final String ACS_URL_PROTOCOL_PREFIX =
            AcsConfigProperties.CPE_SERVER_LB_HTTPS_ENABLED? "https://" : "http://";

    /**
     * Vert.X
     */
    Vertx vertx;

    /**
     * CWMP Session Vertice Instance
     */
    CpeServerTR069SessionVertice sessionVertice;

    /**
     * Redis Client
     */
    RedisClient redisClient;

    /**
     * Constructor.
     *
     * @param vertx
     * @param sessionVertice
     */
    public CwmpHttpRequestHandler(Vertx vertx, CpeServerTR069SessionVertice sessionVertice) {
        /**
         * Save Vert.x Instance adn Session Vertice Index
         */
        this.vertx = vertx;
        this.sessionVertice = sessionVertice;

        /**
         * Initialize Redis Client
         */
        redisClient = new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);
    }

    /**
     * Util Method to send reply to HTTP Load Balancer Vertice
     *
     * @param requestMessage
     * @param status
     * @param payload
     * @param authChallenge
     */
    public static void reply(
            JsonObjectMessage requestMessage,
            HttpResponseStatus status,
            String payload,
            String authChallenge) {
        JsonObject reply = new JsonObject();
        if (status != null) {
            reply.putNumber(CpeServerConstants.FIELD_NAME_STATUS_CODE, status.code());
            if (HttpResponseStatus.UNAUTHORIZED.equals(status) && authChallenge != null) {
                reply.putString(CpeServerConstants.FIELD_NAME_AUTH_CHALLENGE, authChallenge);
            }
        }
        if (payload != null) {
            reply.putString(CpeServerConstants.FIELD_NAME_BODY, payload);
        }

        //log.debug("Reply message:\n" + reply.encodePrettily());
        requestMessage.reply(reply);
    }

    /**
     * Util Method to send reply to HTTP Load Balancer Vertice
     *
     * @param requestMessage
     * @param status
     * @param payload
     * @param cookie
     */
    public static void replyWithCookie(
            JsonObjectMessage requestMessage,
            HttpResponseStatus status,
            String payload,
            String cookie) {
        JsonObject reply = new JsonObject();
        if (status != null) {
            reply.putNumber(CpeServerConstants.FIELD_NAME_STATUS_CODE, status.code());
        }
        if (cookie != null) {
            reply.putString(CpeServerConstants.FIELD_NAME_COOKIE, cookie);
        }
        if (payload != null) {
            reply.putString(CpeServerConstants.FIELD_NAME_BODY, payload);
        }
        //log.debug("Reply message:\n" + reply.encodePrettily());
        requestMessage.reply(reply);
    }

    /**
     * Handler Body
     *
     * @param requestMessage
     */
    @Override
    public void handle(JsonObjectMessage requestMessage) {
        JsonObject request = requestMessage.body();

        /**
         * Declare a null response message first
         */
        CwmpMessage responseMessage = null;

        /**
         * Convert Payload into a CWMP Message POJO
         */
        CwmpMessage message = null;
        String requestBody = request.getString(CpeServerConstants.FIELD_NAME_BODY);
        if (requestBody != null) {
            try {
                message = new CwmpMessage(requestBody);
            } catch (CwmpException e) {
                log.error("Failed to convert HTTP Payload into CWMP Message!\n" + requestBody);
                responseMessage = CwmpUtils.getFaultMessage(
                        CwmpMessage.DEFAULT_CWMP_VERSION,
                        CwmpFaultCodes.ACS_INVALID_ARGS,
                        "Malformed CWMP Message!"
                );
                reply(requestMessage, HttpResponseStatus.OK, responseMessage.toXmlText(), null);
                return;
            }
        }

        /**
         * Process the message
         */
        try {
            CwmpSession.processCpeMessage(
                    vertx,
                    sessionVertice,
                    redisClient,
                    requestMessage,
                    request,
                    message
            );
        } catch (CwmpException e) {
            responseMessage = CwmpUtils.getFaultMessage(
                    e.getCwmpVersion(),
                    e.getFaultCode(),
                    e.getMessage()
            );
            reply(requestMessage, HttpResponseStatus.OK, responseMessage.toXmlText(), null);
            return;
        }
    }
}
