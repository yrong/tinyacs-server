package vertx2.connreq;

/**
 * Project:  SXA-CC
 *
 * Constant Field Names for making a connection-request request.
 *
 * @author: ronyang
 */
public class ConnectionRequestConstants {
    // Field Names in the Connection-Request State (stored in Redis)
    public static final String STATE = "state";
    public static final String CONN_REQ_MANAGER = "conn-req-manager";
    public static final String CWMP_SESSION_MANAGER = "cwmp-session-manager";

    // Field Names in the request message
    public static final String CPE_ID = "cpeId";
    public static final String URL = "url";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String PROXY = "proxy";
    public static final String TIMEOUT = "timeout";
    public static final String MAX_RETRIES = "maxRetries";

    // Field Names/Values in the reply message
    public static final String STATUS = "status";
    public static final String STATUS_VALUE_OK = "ok";
    public static final String STATUS_VALUE_FAILED = "failed";
    public static final String ERROR = "error";

    // Default Connection Request Timeout (30 seconds)
    public static final long DEFAULT_CONN_REQ_TIMEOUT = 30000;

    /**
     * Default Failure Soaking Period
     *
     * (Do not send any conn-req to the same CPE within 10 seconds after the previous failure)
     */
    public static final long DEFAULT_FAILURE_SOAKING_TIMEOUT = 10000;

    /**
     * Proxy Port
     */
    public static final int INTERNAL_PROXY_PORT = 30005;
}
