package vertx2.cpe.sim;

import com.calix.sxa.VertxUtils;

/**
 * Project:  sxa-cc-parent
 *
 * @author: ronyang
 */
public class CpeSimConstants {
    /**
     * The full VertX Module Name including [package]~[artifact]~[version]
     */
    public static final String ARTIFACT_ID = "cpe-sim";

    /**
     * HTTP Request Service Port
     */
    public static final int HTTP_SERVICE_REQ_PORT = 30005;

    /**
     * MongoDB Collection Name
     */
    public static final String MONGO_CPE_SIM__COLLECTION = "sxacc-cpe-sim-devices";

    /**
     * Default ACS URL
     */
    public static final String DEFAULT_ACS_HOST = VertxUtils.getLocalHostname();
    public static final int DEFAULT_ACS_PORT = 8080;
    public static final String DEFAULT_ACS_URL_SUFFIX = "/";

    /**
     * Default ACS Username
     */
    public static final String DEFAULT_ACS_USERNAME = "tr069";

    /**
     * Default ACS Password
     */
    public static final String DEFAULT_ACS_PASSWORD = "tr069";

    /**
     * System Environment Variable Names for ACS URL/Username/Password
     */
    public static final String ACS_HOST_SYS_ENV_VAR = "SXA_CC_ACS_HOST";
    public static final String ACS_PORT_SYS_ENV_VAR = "SXA_CC_ACS_PORT";
    public static final String ACS_URL_SUFFIX_SYS_ENV_VAR = "SXA_CC_ACS_URL_SUFFIX";
    public static final String ACS_USERNAME_SYS_ENV_VAR = "SXA_CC_ACS_USERNAME";
    public static final String ACS_PASSWORD_SYS_ENV_VAR = "SXA_CC_ACS_PASSWORD";

    /**
     * Actual ACS URL/Username/Password
     */
    public static final String ACS_HOST = VertxUtils.initStringProp(ACS_HOST_SYS_ENV_VAR, DEFAULT_ACS_HOST);
    public static final int ACS_PORT = VertxUtils.initIntegerProp(ACS_PORT_SYS_ENV_VAR, DEFAULT_ACS_PORT);
    public static final String ACS_URL_SUFFIX = VertxUtils.initStringProp(ACS_URL_SUFFIX_SYS_ENV_VAR, DEFAULT_ACS_URL_SUFFIX);
    public static final String ACS_URL = "http://" + ACS_HOST + ":" + ACS_PORT + ACS_URL_SUFFIX;
    public static final String ACS_USERNAME = VertxUtils.initStringProp(ACS_USERNAME_SYS_ENV_VAR, DEFAULT_ACS_USERNAME);
    public static final String ACS_PASSWORD = VertxUtils.initStringProp(ACS_PASSWORD_SYS_ENV_VAR, DEFAULT_ACS_PASSWORD);

    /**
     * Vert.X Event Bus Address for Starting a new Session
     */
    public static final String VERTX_ADDRESS_NEW_SESSION = "cpe.sim.new.session";

    /**
     * Vert.X Event Bus Address for new diag request
     */
    public static final String VERTX_ADDRESS_DIAG_REQUEST = "cpe.sim.diag.request";

    /**
     * Shared Session Set
     */
    public static final String SHARED_SESSION_SET = "sessions";
}
