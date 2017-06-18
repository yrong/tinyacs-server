package vertx.cpeserver;

import vertx.VertxUtils;
import vertx.util.AcsConfigProperties;

/**
 * Project:  cwmp-parent
 *
 * @author: ronyang
 */
public class CpeServerConstants {

    /**
     * CPE ACS URL Protocol Prefix ("http" vs. "https")
     */
    public static final String ACS_URL_PROTOCOL_PREFIX =
            AcsConfigProperties.CPE_SERVER_LB_HTTPS_ENABLED? "https://" : "http://";

    /**
     * Vert.x Event Bus Address Prefix for HTTP Request Messages
     */
    public static final String CWMP_SESSION_VERTICE_ADDRESS_PREFIX = "cwmp-session-vertice~";
    public static final int NUMBER_OF_SESSION_VERTICES = 1;//VertxUtils.getNumberOfCpuCores();

    /**
     * Message Field Definition for messages between the HTTP vertices and the Session Vertices
     */
    public static final String FIELD_NAME_VERTICE_INDEX = "index";
    public static final String FIELD_NAME_ACS_HOST = "acsHostname";
    public static final String FIELD_NAME_AUTH_HEADER = "authHeader";
    public static final String FIELD_NAME_BODY = "body";
    public static final String FIELD_NAME_ZERO_TOUCH = "zeroTouch";
    public static final String FIELD_NAME_ACS_USERNAME = "acsUsername";
    public static final String FIELD_NAME_ACS_PASSWORD = "acsPassword";
    public static final String FIELD_NAME_COOKIE = "cookie";
    public static final String FIELD_NAME_STATUS_CODE = "statusCode";
    public static final String FIELD_NAME_AUTH_CHALLENGE = "authChallenge";

    /**
     * CWMP Session Timeout is 2 minutes (in ms)
     */
    public static final long CWMP_SESSION_TIMEOUT = 120000;

    /**
     * For sessions triggered by Connection-Request, we will the sessions for up to 10 seconds after sending the
     * last RPC message.
     */
    public static final int CWMP_SESSION_NBI_INACTIVE_TIMEOUT = 10000;

    /**
     * NBI Callback Timeout
     */
    public static final int CWMP_NBI_CALLBACK_TIMEOUT = 30000;
}
