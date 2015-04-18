package com.calix.sxa.taskmgmt;

import com.calix.sxa.VertxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  sxa-cc-parent
 *
 * SXA-CC Configuration Property Utils.
 *
 * For now, the Configuration Properties are read from System environment.
 *
 * Some defaults are used if failed to read from System environment.
 *
 * @author: jqin
 */
public class ConfigProperties {
    private static final Logger log = LoggerFactory.getLogger(ConfigProperties.class.getName());

    /**
     * Default Task Service Port #
     */
    public static final int defaultTaskServicePort = 8090;

    /**
     * System Environment Variable Names for Task Service Port
     */
    public static final String taskServicePortSysEnvVar = "SXA_TASK_SERVICE_PORT";

    /**
     * Actual Task Service Port
     */
    public static final int taskServicePort = com.calix.sxa.VertxUtils.initIntegerProp(taskServicePortSysEnvVar, defaultTaskServicePort);

    /**
     * Default Task Service Host
     */
    public static final String defaultTaskServiceHost = "localhost";

    /**
     * System Environment Variable Names for Task Service
     */
    public static final String taskServiceHostSysEnvVar = "SXA_TASK_SERVICE_HOST";

    /**
     * Actual Task Server Host
     */
    public static final String taskServiceHost = VertxUtils.initStringProp(taskServiceHostSysEnvVar, defaultTaskServiceHost);

    /**
     * Actual Task Server URL
     */
    public static final String taskServiceUrl = "http://" + taskServiceHost + ":" + taskServicePort;

}
