package vertx2.util;

import com.calix.sxa.VertxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  sxa-cc-parent
 *
 * SXA-CC ACS Configuration Property Utils.
 *
 * For now, the Configuration Properties are read from System environment.
 *
 * Some defaults are used if failed to read from System environment.
 *
 * @author: ronyang
 */
public class AcsConfigProperties {
    private static final Logger log = LoggerFactory.getLogger(AcsConfigProperties.class.getName());

    /**
     * Default ACS NBI API Server Hostname/Port/ContextRoot
     */
    public static final String DEFAULT_ACS_NBI_API_HOST = VertxUtils.getLocalIpAddress();
    public static final int DEFAULT_ACS_INTERNAL_API_PORT = 8081;
    public static final String DEFAULT_ACS_INTERNAL_API_CONTEXT_ROOT = "cc";
    public static final String DEFAULT_ACS_EXTERNAL_API_CONTEXT_ROOT = "api";
    public static final int DEFAULT_ACS_EXTERNAL_API_PORT = 8082;

    /**
     * Default ACS CPE Server Load Balancer Hostname/Port/HTTPs
     */
    public static final String DEFAULT_CPE_SERVER_LB_HOST = VertxUtils.getLocalIpAddress();
    public static final int DEFAULT_CPE_SERVER_LB_PORT = 8080;
    public static final boolean DEFAULT_ACS_CPE_SERVER_LB_HTTPS_ENABLED = false;

    /**
     * Default Max # of concurrent discovery sessions.
     *
     * Deep Discovery is disabled by default.
     */
    public static final boolean DEFAULT_PERFORM_DEEP_DISCOVERY = false;
    public static final int DEFAULT_MAX_DISCOVERY_SESSIONS = 100;

    /**
     * Default Max # of concurrent auto-backup tasks
     */
    public static final int DEFAULT_MAX_CONCURRENT_AUTO_BACKUP_TASKS = 32;
    /**
     * Default auto-backup soaking time (30 minutes)
     */
    public static final int DEFAULT_AUTO_BACKUP_SOAK_TIME = 30 * 60; // in # of seconds

    /**
     * Default CWMP Message TTL (in # of days)
     */
    public static final int DEFAULT_CWMP_MESSAGE_TTL = 7;

    /**
     * Default # of passive workflow worker vertice(s) equals the # of CPU cores
     */
    public static final int DEFAULT_NBR_OF_PASSIVE_WORKFLOW_WORKER_VERTICES = VertxUtils.getNumberOfCpuCores();

    /**
     * Default SXA JBOSS API Port #
     */
    public static final String DEFAULT_SXA_JBOSS_API_HOST = null;
    public static final int DEFAULT_SXA_JBOSS_API_PORT = 8080;

    /**
     * System Environment Variable Names for ACS NBI API Server Hostname/Port/ContextRoot
     */
    public static final String ACS_INTERNAL_API_HOST_SYS_ENV_VAR = "SXA_CC_ACS_INTERNAL_API_HOST";
    public static final String ACS_EXTERNAL_API_PORT_SYS_ENV_VAR = "SXA_CC_ACS_API_EXTERNAL_PORT";
    public static final String ACS_INTERNAL_API_PORT_SYS_ENV_VAR = "SXA_CC_ACS_INTERNAL_API_PORT";
    public static final String ACS_INTERNAL_API_CONTEXT_ROOT_SYS_ENV_VAR = "SXA_CC_ACS_INTERNAL_API_CONTEXT_ROOT";
    public static final String ACS_EXTERNAL_API_CONTEXT_ROOT_SYS_ENV_VAR = "SXA_CC_ACS_EXTERNAL_API_CONTEXT_ROOT";

    /**
     * System Environment Variable Names for ACS File Server Host/Ports/FileStorePath
     */
    public static final String ACS_FILE_STORE_PATH_ENV_VAR = "SXA_CC_ACS_FILE_STORE_PATH";

    /**
     * System Environment Variable Names for CPE Server Load Balancer Hostname/Port/Timeout/DB_Index
     */
    public static final String CPE_SERVER_LB_HOST_SYS_ENV_VAR = "SXA_CPE_SERVER_LB_HOST";
    public static final String CPE_SERVER_LB_PORT_SYS_ENV_VAR = "SXA_CPE_SERVER_LB_PORT";
    public static final String CPE_SERVER_LB_HTTPS_ENABLED_ENV_VAR = "SXA_CPE_SERVER_LB_HTTPS_ENABLED";

    /**
     * System Environment Variable Name for Max # of concurrent discovery sessions
     */
    public static final String PERFORM_DEEP_DISCOVERY_SYS_ENV_VAR = "SXA_CC_PERFORM_DEEP_DISCOVERY";
    public static final String MAX_DISCOVERY_SESSIONS_SYS_ENV_VAR = "SXA_CC_MAX_DISCOVERY_SESSIONS";

    /**
     * System Environment Variable Name for # of passive workflow worker vertice(s)
     */
    public static final String NBR_OF_PASSIVE_WORKFLOW_WORKER_VERTICES_SYS_ENV_VAR =
            "SXA_CC_NBR_OF_PASSIVE_WORKFLOW_WORKER_VERTICES";

    /**
     * System Environment Variable Name for Max # of concurrent auto backup tasks
     */
    public static final String MAX_AUTO_BACKUP_TASKS_SYS_ENV_VAR = "SXA_CC_MAX_AUTO_BACKUP_TASKS";
    /**
     * System Environment Variable Name for auto backup soak time
     */
    public static final String AUTO_BACKUP_SOAK_TIME_SYS_ENV_VAR = "SXA_CC_AUTO_BACKUP_SOAK_TIME";

    /**
     * System Environment Variable Name for CWMP Message TTL
     */
    public static final String CWMP_MESSAGE_TTL_SYS_ENV_VAR = "SXA_CC_CWMP_MESSAGE_TTL";

    /**
     * System Environment Variable Names for ACS NBI API Server Hostname/Port/ContextRoot
     */
    public static final String SXA_JBOSS_API_HOST_SYS_ENV_VAR = "SXA_JBOSS_API_HOST";
    public static final String SXA_JBOSS_API_PORT_SYS_ENV_VAR = "SXA_JBOSS_API_PORT";

    /**
     * Actual ACS_NBI_API Server Hostname/Port/ContextRoot/FileStorePath
     */
    public static final String ACS_NBI_API_HOST =
            VertxUtils.initStringProp(ACS_INTERNAL_API_HOST_SYS_ENV_VAR, DEFAULT_ACS_NBI_API_HOST);
    public static final int ACS_INTERNAL_API_PORT =
            VertxUtils.initIntegerProp(ACS_INTERNAL_API_PORT_SYS_ENV_VAR, DEFAULT_ACS_INTERNAL_API_PORT);
    public static final int ACS_EXTERNAL_API_PORT =
            VertxUtils.initIntegerProp(ACS_EXTERNAL_API_PORT_SYS_ENV_VAR, DEFAULT_ACS_EXTERNAL_API_PORT);
    public static final String ACS_INTERNAL_API_CONTEXT_ROOT =
            VertxUtils.initStringProp(ACS_INTERNAL_API_CONTEXT_ROOT_SYS_ENV_VAR, DEFAULT_ACS_INTERNAL_API_CONTEXT_ROOT);
    public static final String ACS_EXTERNAL_API_CONTEXT_ROOT =
            VertxUtils.initStringProp(ACS_EXTERNAL_API_CONTEXT_ROOT_SYS_ENV_VAR, DEFAULT_ACS_EXTERNAL_API_CONTEXT_ROOT);

    /**
     * Actual CPE Server Load Balancer Hostname/Port/HTTPs
     */
    public static final String CPE_SERVER_LB_HOST =
            VertxUtils.initStringProp(CPE_SERVER_LB_HOST_SYS_ENV_VAR, DEFAULT_CPE_SERVER_LB_HOST);
    public static final int CPE_SERVER_LB_PORT =
            VertxUtils.initIntegerProp(CPE_SERVER_LB_PORT_SYS_ENV_VAR, DEFAULT_CPE_SERVER_LB_PORT);
    public static final boolean CPE_SERVER_LB_HTTPS_ENABLED =
            VertxUtils.initBooleanProp(CPE_SERVER_LB_HTTPS_ENABLED_ENV_VAR, DEFAULT_ACS_CPE_SERVER_LB_HTTPS_ENABLED);
    public static final String CPE_SERVER_BASE_URL =
            "http" + (CPE_SERVER_LB_HTTPS_ENABLED?"s":"") + "://" + CPE_SERVER_LB_HOST + ":" + CPE_SERVER_LB_PORT;

    /**
     * Default File Server Local File Store Path (to be obsolete)
     */
    public static final String DEFAULT_ACS_FILE_STORE_PATH =
            VertxUtils.initStringProp("HOME", "/usr/tmp") + "/acs-file-store";

    /**
     * ACS File Server share the same host/port with the CPE Server FileStorePath
     */
    public static final String FILE_SERVER_HOST = CPE_SERVER_LB_HOST;
    public static final int FILE_SERVER_PORT = CPE_SERVER_LB_PORT;
    public static final String FILE_SERVER_URL_ROOT = "/files/";
    public static final String BASE_FILE_SERVER_URL = CPE_SERVER_BASE_URL + FILE_SERVER_URL_ROOT;

    /**
     * Actual File Server File Store Path
     */
    public static final String ACS_FILE_STORE_PATH =
            VertxUtils.initStringProp(ACS_FILE_STORE_PATH_ENV_VAR, DEFAULT_ACS_FILE_STORE_PATH);

    /**
     * Actual Max # of concurrent discovery sessions
     */
    public static final boolean PERFORM_DEEP_DISCOVERY =
            VertxUtils.initBooleanProp(PERFORM_DEEP_DISCOVERY_SYS_ENV_VAR, DEFAULT_PERFORM_DEEP_DISCOVERY);
    public static final int MAX_DISCOVERY_SESSIONS_PER_VERTICE =
            VertxUtils.initIntegerProp(MAX_DISCOVERY_SESSIONS_SYS_ENV_VAR, DEFAULT_MAX_DISCOVERY_SESSIONS);

    /**
     * Actual # of passive workflow worker vertice(s)
     */
    public static final int NBR_OF_PASSIVE_WORKFLOW_WORKER_VERTICES =
            // Do not exceed the # CPU cores
            Math.min(
                    VertxUtils.initIntegerProp(NBR_OF_PASSIVE_WORKFLOW_WORKER_VERTICES_SYS_ENV_VAR,
                            DEFAULT_NBR_OF_PASSIVE_WORKFLOW_WORKER_VERTICES),
                    VertxUtils.getNumberOfCpuCores()
            );

    /**
     * Actual Max # of concurrent auto backup tasks
     */
    public static final int MAX_CONCURRENT_AUTO_BACKUP_TASKS =
            VertxUtils.initIntegerProp(MAX_AUTO_BACKUP_TASKS_SYS_ENV_VAR, DEFAULT_MAX_CONCURRENT_AUTO_BACKUP_TASKS);
    /**
     * Actual auto backup soak time
     */
    public static final int AUTO_BACKUP_SOAK_TIME =
            VertxUtils.initIntegerProp(AUTO_BACKUP_SOAK_TIME_SYS_ENV_VAR, DEFAULT_AUTO_BACKUP_SOAK_TIME);

    /**
     * Actual CWMP Message TTL
     */
    public static final int CWMP_MESSAGE_TTL =
            VertxUtils.initIntegerProp(CWMP_MESSAGE_TTL_SYS_ENV_VAR, DEFAULT_CWMP_MESSAGE_TTL);

    /**
     * Actual SXA JBoss API Hostname/Port
     */
    public static final String SXA_JBOSS_API_HOST =
            VertxUtils.initStringProp(SXA_JBOSS_API_HOST_SYS_ENV_VAR, DEFAULT_SXA_JBOSS_API_HOST);
    public static final int SXA_JBOSS_API_PORT =
            VertxUtils.initIntegerProp(SXA_JBOSS_API_PORT_SYS_ENV_VAR, DEFAULT_SXA_JBOSS_API_PORT);
}
