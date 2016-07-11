package vertx2.model;

/**
 * Project:  cwmp (aka CCFG)
 *
 * CCFG Event Type Enums
 *
 * @author: ronyang
 */
public enum EventTypeEnum {
    // Event Type                   Type String                                 Severity
    Reboot(                         "Reboot Success",                           EventSeverityEnum.Info),
    RebootFailure(                  "Reboot Failure",                           EventSeverityEnum.Critical),
    FactoryReset(                   "Factory Reset Success",                    EventSeverityEnum.Info),
    FactoryResetFailure(            "Factory Reset Failure",                    EventSeverityEnum.Critical),
    SwUpgrade(                      "SW Upgrade Success",                       EventSeverityEnum.Info),
    SwUpgradeFailure(               "SW Upgrade Failure",                       EventSeverityEnum.Major),
    ConfigurationProfile(           "Apply Configuration Profile Success",      EventSeverityEnum.Info),
    ConfigurationProfileFailure(    "Apply Configuration Profile Failure",      EventSeverityEnum.Major),
    ConfigFileDownload(             "Golden Config File Download Success",      EventSeverityEnum.Info),
    ConfigFileDownloadFailure(      "Golden Config File Download Failure",      EventSeverityEnum.Major),
    SipConfigFileDownload(          "SIP Config File Download Success",         EventSeverityEnum.Info),
    SipConfigFileDownloadFailure(   "SIP Config File Download Failure",         EventSeverityEnum.Major),
    Restore(                        "Configuration Restore Success",            EventSeverityEnum.Info),
    RestoreFailure(                 "Configuration Restore Failure",            EventSeverityEnum.Major),
    ManualConfigFileBackup(         "Manual Configuration Backup Success",      EventSeverityEnum.Info),
    ManualConfigFileBackupFailure(  "Manual Configuration Backup Failure",      EventSeverityEnum.Major),
    AutoConfigFileBackup(           "Auto Configuration Backup Success",        EventSeverityEnum.Info),
    AutoConfigFileBackupFailure(    "Auto Configuration Backup Failure",        EventSeverityEnum.Major),
    Discovery(                      "Discovery Success",                        EventSeverityEnum.Info),
    ApplyServicePlan(               "Service Provisioning",                     EventSeverityEnum.Info),
    ApplyServicePlanFailure(        "Service Provisioning Failure",             EventSeverityEnum.Critical),
    Association(                    "Associated with Subscriber",               EventSeverityEnum.Info),
    Disassociation(                 "Disassociated with Subscriber",            EventSeverityEnum.Info),
    Decommissioned(                 "Decommissioned",                           EventSeverityEnum.Warning),
    Replacement(                    "Replacement Success",                      EventSeverityEnum.Info),
    ReplacementFailure(             "Replacement Failure",                      EventSeverityEnum.Critical);
    //ZeroTouchActivation(            "Zero Touch Activation Success",            EventSeverityEnum.Info);

    public String typeString;
    public EventSeverityEnum severity;

    /**
     * Constructor.
     *
     * @param typeString
     * @param severity
     */
    private EventTypeEnum(String typeString, EventSeverityEnum severity) {
        this.typeString = typeString;
        this.severity = severity;
    }
}
