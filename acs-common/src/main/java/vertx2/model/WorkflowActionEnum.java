package vertx2.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  cwmp
 *
 * Workflow Action Enums.
 *
 * @author: ronyang
 */
public enum WorkflowActionEnum {
    /**
     * Values
     */
    DOWNLOAD_CONFIG_FILE(
            "Download Configuration File",
            EventTypeEnum.ConfigFileDownload,
            EventTypeEnum.ConfigFileDownloadFailure
    ),
    DOWNLOAD_FW_IMAGE(
            "Download SW/FW Image",
            EventTypeEnum.SwUpgrade,
            EventTypeEnum.SwUpgradeFailure
    ),
    APPLY_CONFIG_PROFILE(
            "Apply Configuration Profile",
            EventTypeEnum.ConfigurationProfile,
            EventTypeEnum.ConfigurationProfileFailure
    ),
    APPLY_SERVICE_PROFILE(
            "Apply Service Profile",
            null,
            null
    ),
    APPLY_NOTIFICATION_PROFILE(
            "Apply Notification Profile",
            null,
            null
    ),
    APPLY_PERFORMANCE_PROFILE(
            "Apply Performance Profile",
            null,
            null
    ),
    GET_PARAMETER_VALUES(
            "Get Parameter Values",

            null,
            null
    ),
    SET_PARAMETER_VALUES(
            "Set Parameter Values",
            null,
            null
    ),
    REBOOT(
            "Reboot",
            EventTypeEnum.Reboot,
            EventTypeEnum.RebootFailure
    ),
    FACTORY_RESET(
            "Factory Reset",
            EventTypeEnum.FactoryReset,
            EventTypeEnum.FactoryResetFailure
    ),
    DIAGNOSTIC(
            "Diagnostic",
            null,
            null
    ),
    DELAY(
            "Delay",
            null,
            null
    ),
    INVALID(
            null,
            null,
            null
    );

    // Private logger
    private static final Logger log = LoggerFactory.getLogger(WorkflowActionEnum.class.getName());

    // Each Enum Value shall has a String attribute
    public String typeString;

    // Type of event to be saved, or null
    public EventTypeEnum eventType;
    public EventTypeEnum failureEventType;

    /**
     * Constructor which requires a type string.
     * @param typeString
     */
    private WorkflowActionEnum (String typeString, EventTypeEnum eventType, EventTypeEnum failureEventType) {
        this.typeString = typeString;
        this.eventType = eventType;
        this.failureEventType = failureEventType;
    }

    /**
     * Get Action Type by string.
     *
     * @param typeString
     * @return
     * @throws vertx2.CcException
     */
    public static WorkflowActionEnum getWorkflowActionEnum(String typeString) {
        for (WorkflowActionEnum actionType : values()) {
            if (actionType.typeString != null && actionType.typeString.equals(typeString)) {
                return actionType;
            }
        }

        log.error("Invalid Workflow Action Type " + typeString + "!");
        return INVALID;
    }
}
