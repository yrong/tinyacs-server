package vertx.model;

import vertx.VertxJsonUtils;
import vertx.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  844e_mvp
 *
 * Voice Dial Plan.
 *
 * @author: ronyang
 */
public class DialPlan {
    private static final Logger log = LoggerFactory.getLogger(DialPlan.class.getName());

    /**
     * DB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "CWMP-dial-plans";

    /**
     * Field Name Constants
     */
    public static final String FIELD_NAME_RULES = "rules";
    public static final String FIELD_NAME_SHORT_TIMER = "shortTimer";
    public static final String FIELD_NAME_LONG_TIMER = "longTimer";

    /**
     * The "customId" and "servicePrefix" fields, if present, must be unique across the same organization.
     */
    public static final String[] INDEX_FIELDS = {
            AcsConstants.FIELD_NAME_ORG_ID,
            AcsConstants.FIELD_NAME_NAME
    };

    /**
     * Editable Fields
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{
        add(AcsConstants.FIELD_NAME_NAME);
        add(AcsConstants.FIELD_NAME_DESCRIPTION);
        add(FIELD_NAME_RULES);
        add(FIELD_NAME_SHORT_TIMER);
        add(FIELD_NAME_LONG_TIMER);
    }};

    /**
     * Static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator MANDATORY_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_RULES, VertxJsonUtils.JsonFieldType.JsonArray);

    public static final VertxJsonUtils.JsonFieldValidator OPTIONAL_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_SHORT_TIMER, VertxJsonUtils.JsonFieldType.Integer)
            .append(FIELD_NAME_LONG_TIMER, VertxJsonUtils.JsonFieldType.Integer);

    /**
     * System Default Dial Plan
     */
    public static final String SYSTEM_DEFAULT_DIAL_PLAN_ID = "system-default";
    public static final JsonObject SYSTEM_DEFAULT_DIAL_PLAN = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ID, SYSTEM_DEFAULT_DIAL_PLAN_ID)
            .put(AcsConstants.FIELD_NAME_NAME, SYSTEM_DEFAULT_DIAL_PLAN_ID)
            .put(AcsConstants.FIELD_NAME_DESCRIPTION, "System Default Dial Plan")
            .put(FIELD_NAME_SHORT_TIMER, 4)
            .put(FIELD_NAME_LONG_TIMER, 16)
            .put(
                    FIELD_NAME_RULES,
                    new JsonArray()
                            .add("^911n")
                            .add("^411")
                            .add("^[2-9][0-9]{6}")
                            .add("^1[2-9][0-9]{9}")
                            .add("^011[0-9]*T")
                            .add("^S[0-9]{2}")
            );

    /**
     * Convert an CWMP Dial Plan JSON Object to RG Data Model Object
     * @param dialPlan
     */
    public static JsonObject toRgDataModelObject(JsonObject dialPlan) {
        if (dialPlan == null) {
            return null;
        }

        /**
         * Build Digit Map String
         */
        JsonArray rules = dialPlan.getJsonArray(FIELD_NAME_RULES);
        String digitMap = "";
        for (int i = 0; i < rules.size(); i ++)  {
            String aRule = rules.getString(i);
            digitMap += aRule;
            if (i < (rules.size() - 1)) {
                digitMap += "|";
            }
        }
        JsonObject modelObject = new JsonObject().put("DigitMap", digitMap);

        if (dialPlan.containsKey(FIELD_NAME_LONG_TIMER)) {
            modelObject.put("X_000631_DigitLongTimer", dialPlan.getInteger(FIELD_NAME_LONG_TIMER));
        }
        if (dialPlan.containsKey(FIELD_NAME_SHORT_TIMER)) {
            modelObject.put("X_000631_DigitShortTimer", dialPlan.getInteger(FIELD_NAME_SHORT_TIMER));
        }

        return modelObject;
    }
}
