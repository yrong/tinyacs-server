package vertx2.model;

import vertx2.VertxException;
import vertx2.VertxJsonUtils;
import vertx2.VertxMongoUtils;
import vertx2.cache.DialPlanCache;
import vertx2.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  844e_mvp
 *
 * Subscriber Service Plan Data Model.
 *
 * See http://wiki.calix.local/display/Compass/cwmp+ACS+API#cwmpACSAPI-"ServicePlan"Struct for more details.
 *
 * @author: ronyang
 */
public class ServicePlan {
    private static final Logger log = LoggerFactory.getLogger(ServicePlan.class.getName());

    /**
     * DB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "CWMP-service-plans";

    /**
     * Field Name Constants
     */
    public static final String FIELD_NAME_DATA = "data";
    public static final String FIELD_NAME_VOICE = "voice";
    public static final String FIELD_NAME_VIDEO = "video";
    public static final String FIELD_NAME_SUBSCRIBER_ID = "subscriberId";
    public static final String FIELD_NAME_DEVICE_ID = "deviceId";

    /**
     * Sub Field Names
     */
    public static final String FIELD_NAME_ENABLE = "Enable";
    public static final String FIELD_NAME_DIAL_PLAN = "DialPlan";
    public static final String FIELD_NAME_LINES = "Line";
    public static final String FIELD_NAME_FAX_T38 = "FaxT38";
    public static final String FIELD_NAME_SIP = "SIP";
    public static final String FIELD_NAME_CALLING_FEATURES = "CallingFeatures";
    public static final String FIELD_NAME_VOICE_PROCESSING = "VoiceProcessing";

    /**
     * Index Field.
     *
     * Within each organization, each Device can only be associated to one service plan.
     */
    public static final String[] INDEX_FIELDS = {
            AcsConstants.FIELD_NAME_ORG_ID,
            FIELD_NAME_DEVICE_ID
    };

    /**
     * Editable Fields
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{
        add(FIELD_NAME_DATA);
        add(FIELD_NAME_VOICE);
        add(FIELD_NAME_VIDEO);
        add(FIELD_NAME_DEVICE_ID);
    }};

    /**
     * A Static Service Plan that disables everything
     */
    public static final JsonObject DISABLE_VOICE_LINE = new JsonObject()
            .putString(FIELD_NAME_ENABLE, "disabled");
    public static final JsonObject SERVICE_PLAN_ALL_DISABLED = new JsonObject()
            .putObject(FIELD_NAME_DATA, new JsonObject().putBoolean(FIELD_NAME_ENABLE, false))
            .putObject(FIELD_NAME_VIDEO, new JsonObject().putBoolean(FIELD_NAME_ENABLE, false))
            .putObject(
                    FIELD_NAME_VOICE,
                    new JsonObject().putObject(
                            FIELD_NAME_LINES,
                            new JsonObject()
                                    .putObject("1", DISABLE_VOICE_LINE)
                                    .putObject("2", DISABLE_VOICE_LINE)
                    )
            );

    /**
     * Static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator MANDATORY_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_SUBSCRIBER_ID, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_DEVICE_ID, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator OPTIONAL_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_DATA, VertxJsonUtils.JsonFieldType.JsonObject)
            .append(FIELD_NAME_VOICE, VertxJsonUtils.JsonFieldType.JsonObject)
            .append(FIELD_NAME_VIDEO, VertxJsonUtils.JsonFieldType.JsonObject);

    public static final VertxJsonUtils.JsonFieldValidator VIDEO_DATA_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_ENABLE, VertxJsonUtils.JsonFieldType.Boolean);
    public static final VertxJsonUtils.JsonFieldValidator VIDEO_DATA_OPTIONAL_FIELDS = null;

    public static final VertxJsonUtils.JsonFieldValidator VOICE_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_FAX_T38, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_DIAL_PLAN, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_LINES, VertxJsonUtils.JsonFieldType.JsonObject);
    public static final VertxJsonUtils.JsonFieldValidator VOICE_OPTIONAL_FIELDS = null;

    public static final VertxJsonUtils.JsonFieldValidator VOICE_FAX_T38_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_ENABLE, VertxJsonUtils.JsonFieldType.Boolean);
    public static final VertxJsonUtils.JsonFieldValidator VOICE_FAX_T38_OPTIONAL_FIELDS = null;

    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINES_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append("1", VertxJsonUtils.JsonFieldType.JsonObject)
                    .append("2", VertxJsonUtils.JsonFieldType.JsonObject);
    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINES_OPTIONAL_FIELDS = null;

    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINE_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_ENABLE, VertxJsonUtils.JsonFieldType.String);
    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINE_OPTIONAL_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_SIP, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_CALLING_FEATURES, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_VOICE_PROCESSING, VertxJsonUtils.JsonFieldType.JsonObject);
    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINE_MANDATORY_FIELDS_WHEN_ENABLED =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_ENABLE, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_SIP, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_CALLING_FEATURES, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_VOICE_PROCESSING, VertxJsonUtils.JsonFieldType.JsonObject);


    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINE_SIP_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append("AuthUserName", VertxJsonUtils.JsonFieldType.String)
                    .append("AuthPassword", VertxJsonUtils.JsonFieldType.String)
                    .append("URI", VertxJsonUtils.JsonFieldType.String);
    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINE_SIP_OPTIONAL_FIELDS = null;

    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINE_CALLING_FEATURES_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append("CallerIDEnable", VertxJsonUtils.JsonFieldType.Boolean)
                    .append("CallWaitingEnable", VertxJsonUtils.JsonFieldType.Boolean)
                    .append("X_000631_ThreewayCallingEnable", VertxJsonUtils.JsonFieldType.Boolean)
                    .append("MWIEnable", VertxJsonUtils.JsonFieldType.Boolean)
                    .append("X_000631_DirectConnectEnable", VertxJsonUtils.JsonFieldType.Boolean);
    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINE_CALLING_FEATURES_OPTIONAL_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append("X_000631_DirectConnectNumber", VertxJsonUtils.JsonFieldType.String)
                    .append("X_000631_DirectConnectTimer", VertxJsonUtils.JsonFieldType.Integer);

    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINE_VOICE_PROCESSING_MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator()
                    .append("ReceiveGain", VertxJsonUtils.JsonFieldType.Integer)
                    .append("TransmitGain", VertxJsonUtils.JsonFieldType.Integer);
    public static final VertxJsonUtils.JsonFieldValidator VOICE_LINE_VOICE_PROCESSING_OPTIONAL_FIELDS = null;

    /**
     * Validation Exceptions
     */
    public static VertxException INVALID_DIAL_PLAN_ID = new VertxException("Invalid Voice DialPlan Id!");
    public static VertxException VOICE_LINE_1_ENABLE_INVALID_VALUE = new VertxException(
            "Voice Line 1 \"Enable\" Field contains invalid value! (must be \"Enabled\" or \"Disabled\")"
    );
    public static VertxException VOICE_LINE_2_ENABLE_INVALID_VALUE = new VertxException(
            "Voice Line 1 \"Enable\" Field contains invalid value! (must be \"Enabled\" or \"Disabled\")"
    );

    /**
     * Build DeviceId Matcher
     */
    public static JsonObject buildDeviceIdMatcher(Cpe cpe) {
        JsonArray matcherArray = new JsonArray()
                .addString(cpe.deviceId.sn)
                .addString(cpe.key);
        if (cpe.deviceId.registrationId != null) {
            matcherArray.add(cpe.deviceId.registrationId);
        }
        JsonObject matcher = new JsonObject()
                .putObject(
                        FIELD_NAME_DEVICE_ID,
                        new JsonObject().putArray(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN, matcherArray)
                )
                .putString(AcsConstants.FIELD_NAME_ORG_ID, cpe.orgId);
        return matcher;
    }

    /**
     * Validation Method.
     *
     * @param aServicePlan
     * @throws VertxException
     */
    public static void validate(JsonObject aServicePlan, DialPlanCache dialPlanCache)
            throws VertxException{
        VertxJsonUtils.validateFields(aServicePlan, MANDATORY_FIELDS, OPTIONAL_FIELDS);

        for (String fieldName : aServicePlan.getFieldNames()) {
            if (fieldName.equals(FIELD_NAME_DATA) || fieldName.equals(FIELD_NAME_VIDEO)) {
                VertxJsonUtils.validateFields(
                        aServicePlan.getObject(fieldName),
                        VIDEO_DATA_MANDATORY_FIELDS,
                        VIDEO_DATA_OPTIONAL_FIELDS
                );
            } else if (fieldName.equals(FIELD_NAME_VOICE)) {
                JsonObject voiceService = aServicePlan.getObject(fieldName);
                try {
                    VertxJsonUtils.validateFields(
                            voiceService,
                            VOICE_MANDATORY_FIELDS,
                            VOICE_OPTIONAL_FIELDS
                    );
                } catch (VertxException ex) {
                    throw new VertxException("Voice Field: " + ex.getMessage());
                }

                for (String voiceFieldName : voiceService.getFieldNames()) {
                    log.debug("Processing voice field " + voiceFieldName);
                    if (voiceFieldName.equals(FIELD_NAME_FAX_T38)) {
                        try {
                            VertxJsonUtils.validateFields(
                                    voiceService.getObject(FIELD_NAME_FAX_T38),
                                    VOICE_FAX_T38_MANDATORY_FIELDS,
                                    VOICE_FAX_T38_OPTIONAL_FIELDS
                            );
                        } catch (VertxException ex) {
                            throw new VertxException("Voice->FaxT38 : " + ex.getMessage());
                        }
                    } else if (FIELD_NAME_DIAL_PLAN.equals(voiceFieldName)) {
                        String dialPlanId = voiceService.getString(FIELD_NAME_DIAL_PLAN);
                        if (dialPlanCache.getDialPlanById(dialPlanId) == null) {
                            throw INVALID_DIAL_PLAN_ID;
                        }
                    } else if (FIELD_NAME_LINES.equals(voiceFieldName)) {
                        JsonObject lines = voiceService.getObject(FIELD_NAME_LINES);
                        try {
                            VertxJsonUtils.validateFields(
                                    lines,
                                    VOICE_LINES_MANDATORY_FIELDS,
                                    VOICE_LINES_OPTIONAL_FIELDS
                            );
                        } catch (VertxException ex) {
                            String error = ex.getMessage()
                                    .replace("Missing Mandatory Field", "No Configuration Found for Line");
                            throw new VertxException("Voice->Line : " + error);
                        }

                        // Validate individual voice line attributes
                        for (String lineNumber : lines.getFieldNames()) {
                            JsonObject aLine = lines.getObject(lineNumber);
                            try {
                                VertxJsonUtils.validateFields(
                                        aLine,
                                        VOICE_LINE_MANDATORY_FIELDS,
                                        VOICE_LINE_OPTIONAL_FIELDS
                                );
                            } catch (VertxException ex) {
                                throw new VertxException(
                                        "Voice->Line " + lineNumber + ": " + ex.getMessage()
                                );
                            }

                            for (String voiceLineFieldName : aLine.getFieldNames()) {
                                if (FIELD_NAME_ENABLE.equals(voiceLineFieldName)) {
                                    String enable = aLine.getString(FIELD_NAME_ENABLE);
                                    if ("Enabled".equals(enable)) {
                                        try {
                                            VertxJsonUtils.validateFields(
                                                    aLine,
                                                    VOICE_LINE_MANDATORY_FIELDS_WHEN_ENABLED,
                                                    VOICE_LINE_OPTIONAL_FIELDS
                                            );
                                        } catch (VertxException ex) {
                                            throw new VertxException(
                                                    "Voice->Line " + lineNumber + ": " + ex.getMessage()
                                            );
                                        }

                                    } else if (!"Disabled".equals(enable)) {
                                        if (lineNumber.equals("1")) {
                                            throw VOICE_LINE_1_ENABLE_INVALID_VALUE;
                                        } else {
                                            throw VOICE_LINE_2_ENABLE_INVALID_VALUE;
                                        }
                                    }


                                } else if (FIELD_NAME_SIP.equals(voiceLineFieldName)) {
                                    try {
                                        VertxJsonUtils.validateFields(
                                                aLine.getObject(FIELD_NAME_SIP),
                                                VOICE_LINE_SIP_MANDATORY_FIELDS,
                                                VOICE_LINE_SIP_OPTIONAL_FIELDS
                                        );
                                    } catch (VertxException ex) {
                                        throw new VertxException(
                                                "Voice->Line " + lineNumber + "->SIP: " + ex.getMessage()
                                        );
                                    }
                                } else if (FIELD_NAME_CALLING_FEATURES.equals(voiceLineFieldName)) {
                                    JsonObject callingFeatures = aLine.getObject(FIELD_NAME_CALLING_FEATURES);
                                    try {
                                        VertxJsonUtils.validateFields(
                                                callingFeatures,
                                                VOICE_LINE_CALLING_FEATURES_MANDATORY_FIELDS,
                                                VOICE_LINE_CALLING_FEATURES_OPTIONAL_FIELDS
                                        );
                                    } catch (VertxException ex) {
                                        throw new VertxException(
                                                "Voice->Line " + lineNumber + "->CallingFeatures: "
                                                        + ex.getMessage()
                                        );
                                    }

                                    if (callingFeatures.getBoolean("X_000631_DirectConnectEnable", false)) {
                                        /**
                                         * Direct Connect Number/Timer are mandatory when direct connect is enabled
                                         */
                                        if (!callingFeatures.containsField("X_000631_DirectConnectNumber") ||
                                                !callingFeatures.containsField("X_000631_DirectConnectTimer")) {
                                            String missingParam;
                                            if (!callingFeatures.containsField("X_000631_DirectConnectNumber")) {
                                                missingParam = "X_000631_DirectConnectNumber";
                                            } else {
                                                missingParam = "X_000631_DirectConnectTimer";
                                            }
                                            throw new VertxException(
                                                    "Voice->Line " + lineNumber + "->CallingFeatures: "
                                                            + missingParam + " must be specified when " +
                                                            "X_000631_DirectConnectEnable is true!");
                                        }
                                    }
                                } else if (FIELD_NAME_VOICE_PROCESSING.equals(voiceLineFieldName)) {
                                    JsonObject voiceProcessing = aLine.getObject(FIELD_NAME_VOICE_PROCESSING);
                                    try {
                                        VertxJsonUtils.validateFields(
                                                voiceProcessing,
                                                VOICE_LINE_VOICE_PROCESSING_MANDATORY_FIELDS,
                                                VOICE_LINE_VOICE_PROCESSING_OPTIONAL_FIELDS
                                        );
                                    } catch (VertxException ex) {
                                        throw new VertxException(
                                                "Voice->Line " + lineNumber + "->VoiceProcessing: "
                                                        + ex.getMessage()
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
