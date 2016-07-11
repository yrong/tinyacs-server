package vertx2.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  cwmp
 * 
 * CC Event Type Enums
 *
 * @author: ronyang
 */
public enum CcEventTypeEnum {

    /**
     * Values
     */
    INFORM_BOOTSTRAP("CWMP INFORM EVENT - 0 BOOTSTRAP"),
    INFORM_BOOT("CWMP INFORM EVENT - 1 BOOT"),
    INFORM_PERIODIC("CWMP INFORM EVENT - 2 PERIODIC"),
    INFORM_SCHEDULED("CWMP INFORM EVENT - 3 SCHEDULED"),
    INFORM_VALUE_CHANGE("CWMP INFORM EVENT - 4 VALUE CHANGE"),
    INFORM_KICKED("CWMP INFORM EVENT - 5 KICKED"),
    INFORM_CONNECTION_REQUEST("CWMP INFORM EVENT - 6 CONNECTION REQUEST"),
    INFORM_TRANSFER_COMPLETE("CWMP INFORM EVENT - 7 TRANSFER COMPLETE"),
    INFORM_DIAGNOSTICS_COMPLETE("CWMP INFORM EVENT - 8 DIAGNOSTICS COMPLETE"),
    INFORM_REQUEST_DOWNLOAD("CWMP INFORM EVENT - 9 REQUEST DOWNLOAD"),
    INFORM_AUTONOMOUS_TRANSFER_COMPLETE("CWMP INFORM EVENT - 10 AUTONOMOUS TRANSFER COMPLETE"),
    INFORM_DU_STATE_CHANGE_COMPLETE("CWMP INFORM EVENT - 11 DU STATE CHANGE COMPLETE"),
    INFORM_AUTONOMOUS_DU_STATE_CHANGE_COMPLETE("CWMP INFORM EVENT - 12 AUTONOMOUS DU STATE CHANGE COMPLETE"),
    INFORM_M_REBOOT("CWMP INFORM EVENT - M Reboot"),
    INFORM_M_SCHEDULE_INFORM("CWMP INFORM EVENT - M ScheduleInform"),
    INFORM_M_DOWNLOAD("CWMP INFORM EVENT - M Download"),
    INFORM_M_SCHEDULE_DOWNLOAD("CWMP INFORM EVENT - M ScheduleDownload"),
    INFORM_M_UPLOAD("CWMP INFORM EVENT - M Upload"),
    INFORM_M_CHANGE_DU_STATE("CWMP INFORM EVENT - M ChangeDUState"),
    NEW_CPE_DISCOVERED("CC EVENT - New CPE Discovered"),
    ACS_SERVER_ARRIVAL("CC EVENT - ACS Server Arrival"),
    ACS_SERVER_DEPARTURE("CC EVENT - ACS Server Departure"),
    CPE_SERVER_ARRIVAL("CC EVENT - CPE Server Arrival"),
    CPE_SERVER_DEPARTURE("CC EVENT - CPE Server Departure"),
    RG_INITIAL_TURN_UP_STARTED("CC EVENT - RG Initial Turn Up Started"),
    RG_INITIAL_TURN_UP_COMPLETED("CC EVENT - RG Initial Turn Up Completed"),
    MAINTENANCE_WINDOW_OPENED("Maintenance Window Opened"),
    MAINTENANCE_WINDOW_CLOSED("Maintenance Window Closed"),
    INVALID(null);

    // Private logger
    private static final Logger log = LoggerFactory.getLogger(CcEventTypeEnum.class.getName());

    // Each Event Type Enum shall has a String attribute
    public String typeString;

    /**
     * Constructor which requires a type string.
     * @param typeString
     */
    private CcEventTypeEnum(String typeString) {
        this.typeString = typeString;
    }

    /**
     * Get Event Type by string.
     *
     * @param typeString
     * @return
     * @throws vertx2.CcException
     */
    public static CcEventTypeEnum getCcEventType(String typeString) {
        for (CcEventTypeEnum fileType : values()) {
            if (fileType.typeString.equals(typeString)) {
                return fileType;
            }
        }

        log.error("Invalid ACS Event Type " + typeString + "!");
        return INVALID;
    }

    /**
     * Get Event Type String by Event Type Name.
     *
     * @param typeName
     * @return
     * @throws vertx2.CcException
     */
    public static String getCcEventTypeString(String typeName) {
        for (CcEventTypeEnum eventTypeEnum : values()) {
            if (eventTypeEnum.name().equals(typeName)) {
                return eventTypeEnum.typeString;
            }
        }

        log.error("Invalid ACS Event Type " + typeName + "!");
        return INVALID.name();
    }
}
