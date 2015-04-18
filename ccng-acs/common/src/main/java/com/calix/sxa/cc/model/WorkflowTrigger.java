package com.calix.sxa.cc.model;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.cc.CcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA-CC
 *
 * Workflow Initial Trigger.
 *
 * A valid Workflow Initial Trigger JSON Object must contain a valid trigger type which must be one of the following:
 *
 *  - "CPE Event"
 *  - "Fixed Delay"
 *  - "Maintenance Window"
 *
 * @author: jqin
 */
public class WorkflowTrigger {
    // Private logger
    private static final Logger log = LoggerFactory.getLogger(WorkflowTrigger.class.getName());

    /**
     * Trigger Type Enum
     */
    public enum TriggerTypeEnum {
        /**
         * Values
         */
        CPE_EVENT("CPE Event"),
        FIXED_DELAY("Fixed Delay"),
        MAINTENANCE_WINDOW("Maintenance Window"),
        INVALID(null);

        // Each Enum Value shall has a String attribute
        public String typeString;

        /**
         * Constructor which requires a type string.
         * @param typeString
         */
        private TriggerTypeEnum (String typeString) {
            this.typeString = typeString;
        }
        /**
         * Get Trigger Type by string.
         *
         * @param typeString
         */
        public static TriggerTypeEnum getTriggerTypeEnum(String typeString) {
            for (TriggerTypeEnum actionType : values()) {
                if (actionType.typeString.equals(typeString)) {
                    return actionType;
                }
            }

            log.error("Invalid Workflow Trigger Type " + typeString + "!");
            return INVALID;
        }
    }

    /**
     * Field Names
     */
    public static final String FIELD_NAME_TRIGGER_TYPE = "type";
    public static final String FIELD_NAME_CPE_EVENT = "cpeEvent";

    /**
     * POJO Variables
     */
    public TriggerTypeEnum triggerType = TriggerTypeEnum.INVALID;
    public CcEventTypeEnum triggerCpeEvent = CcEventTypeEnum.INVALID;
    public Integer delay = null;

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator mandatoryFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_TRIGGER_TYPE, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator optionalFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_CPE_EVENT, VertxJsonUtils.JsonFieldType.String);

    /**
     * Static CcException(s)
     */
    public static final CcException INVALID_INITIAL_TRIGGER = new CcException("Invalid Initial Trigger");

    /**
     * Constructor by a JSON Object
     *
     * @param jsonObject
     * @throws SxaVertxException
     * @throws CcException
     */
    public WorkflowTrigger (JsonObject jsonObject) throws SxaVertxException, CcException {
        // Validate Field Types
        VertxJsonUtils.validateFields(jsonObject, mandatoryFields, optionalFields);

        // Extract/Validate Trigger Type
        triggerType = TriggerTypeEnum.getTriggerTypeEnum(jsonObject.getString(FIELD_NAME_TRIGGER_TYPE));
        switch (triggerType) {
            case CPE_EVENT:
                /**
                 * We only allow CPE related events
                 */
                triggerCpeEvent = CcEventTypeEnum.getCcEventType(jsonObject.getString(FIELD_NAME_CPE_EVENT));
                if (!triggerCpeEvent.name().startsWith("INFORM") && !triggerCpeEvent.name().startsWith("NEW_CPE")) {
                    throw INVALID_INITIAL_TRIGGER;
                }
                break;
        }
    }
}
