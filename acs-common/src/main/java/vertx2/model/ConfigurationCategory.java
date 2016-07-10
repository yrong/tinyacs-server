package vertx2.model;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import vertx2.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  SXA-CC
 *
 * Data Model For Configuration Categories.
 *
 * @author: ronyang
 */
public class ConfigurationCategory {
    private static final Logger log = LoggerFactory.getLogger(ConfigurationCategory.class.getName());

    /**
     * DB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "sxacc-configuration-categories";

    /**
     * Field Name Constants
     */
    public static final String FIELD_NAME_TR098_PATH_PREFIX = "tr098PathPrefix";
    public static final String FIELD_NAME_HAS_SUB_INSTANCES = "hasSubInstances";
    public static final String FIELD_NAME_SERVICE_TYPE = "serviceType";
    public static final String FIELD_NAME_SERVICE_VALUES = "serviceValues";
    public static final String FIELD_NAME_SUB_INSTANCE_INDEX_DROP_DOWN_LIST_NAME = "subInstanceIndexDropDownListName";
    public static final String FIELD_NAME_SUB_INSTANCE_INDEX_VALUES = "subInstanceIndexValues";
    public static final String FIELD_NAME_MULTI_INSTANCE = "multiInstance";
    public static final String FIELD_NAME_KEY_PARAMETER = "keyParameter";
    public static final String FIELD_NAME_KEY_PARAMETER_NAME = "name";
    public static final String FIELD_NAME_KEY_PARAMETER_ALLOW_OVERWRITE = "allowOverwrite";
    public static final String FIELD_NAME_PARAMETERS = "parameters";
    public static final String FIELD_NAME_PARAMETER_REQUIRES = "requires";
    public static final String FIELD_NAME_PARAMETER_IMPLIES = "implies";
    public static final String FIELD_NAME_PARAMETER_DISPLAY_ONLY = "displayOnly";
    public static final String FIELD_NAME_PARAMETER_INSTANCE_ALIAS = "instanceAlias";
    public static final String FIELD_NAME_PARAMETER_TYPE = "type";
    public static final String FIELD_NAME_PARAMETER_DISPLAY_NAME = "displayName";
    public static final String FIELD_NAME_PARAMETER_MAX_STRING_LENGTH = "maxStringLength";
    public static final String FIELD_NAME_PARAMETER_MIN_STRING_LENGTH = "minStringLength";
    public static final String FIELD_NAME_PARAMETER_STRING_PATTERN = "stringPattern";
    public static final String FIELD_NAME_PARAMETER_MIN_VALUE = "minValue";
    public static final String FIELD_NAME_PARAMETER_MAX_VALUE = "maxValue";
    public static final String FIELD_NAME_PARAMETER_DEFAULT_VALUE = "defaultValue";
    public static final String FIELD_NAME_PARAMETER_VALUE_ENUMS = "valueEnums";
    public static final String FIELD_NAME_PARAMETER_INVISIBLE = "invisible";
    public static final String FIELD_NAME_PARAMETER_TR_098_PATH_OVERRIDE = "tr098PathOverride";
    public static final String FIELD_NAME_PARAMETER_UNIT = "unit";
    public static final String FIELD_NAME_PARAMETER_EXCLUDED_VALUES = "excludedValueEnums";
    public static final String FIELD_NAME_PARAMETER_VALIDATION_ERROR_MESSAGE = "validationErrorMessage";
    public static final String FIELD_NAME_PARAMETER_MANDATORY = "mandatory";
    public static final String FIELD_NAME_PARAMETER_HIDDEN = "hidden";

    /**
     * Category Type Constants
     */
    public static final String PARAM_NAME_SERVICE_NAME = "Name";
    public static final String VIDEO_SERVICE = "Video Service";
    public static final String VOICE_SERVICE = "Voice Service";

    /**
     * Index Fields
     */
    public static final String[] INDEX_FIELDS = {
            AcsConstants.FIELD_NAME_NAME
    };

    /**
     * Editable Fields
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{
        add(AcsConstants.FIELD_NAME_NAME);
        add(AcsConstants.FIELD_NAME_DESCRIPTION);
        add(FIELD_NAME_TR098_PATH_PREFIX);
        add(FIELD_NAME_PARAMETERS);
    }};

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator MANDATORY_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PARAMETERS, VertxJsonUtils.JsonFieldType.JsonArray);

    public static final VertxJsonUtils.JsonFieldValidator OPTIONAL_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(FIELD_NAME_TR098_PATH_PREFIX, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_SUB_INSTANCE_INDEX_DROP_DOWN_LIST_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_SERVICE_TYPE, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_SERVICE_VALUES, VertxJsonUtils.JsonFieldType.JsonObject)
            .append(FIELD_NAME_MULTI_INSTANCE, VertxJsonUtils.JsonFieldType.Boolean)
            .append(FIELD_NAME_KEY_PARAMETER, VertxJsonUtils.JsonFieldType.JsonObject)
            .append(FIELD_NAME_SUB_INSTANCE_INDEX_VALUES, VertxJsonUtils.JsonFieldType.JsonArray);

    /**
     * Define static JSON Field Validators for each individual parameter
     */
    public static final VertxJsonUtils.JsonFieldValidator PARAMETER_MANDATORY_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PARAMETER_TYPE, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator PARAMETER_OPTIONAL_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PARAMETER_DISPLAY_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PARAMETER_INVISIBLE, VertxJsonUtils.JsonFieldType.Boolean)
            .append(FIELD_NAME_PARAMETER_UNIT, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PARAMETER_EXCLUDED_VALUES, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_PARAMETER_VALIDATION_ERROR_MESSAGE, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PARAMETER_MANDATORY, VertxJsonUtils.JsonFieldType.Boolean)
            .append(FIELD_NAME_PARAMETER_HIDDEN, VertxJsonUtils.JsonFieldType.Boolean)
            .append(FIELD_NAME_PARAMETER_INSTANCE_ALIAS, VertxJsonUtils.JsonFieldType.Boolean)
            .append(FIELD_NAME_PARAMETER_MAX_STRING_LENGTH, VertxJsonUtils.JsonFieldType.Integer)
            .append(FIELD_NAME_PARAMETER_MIN_STRING_LENGTH, VertxJsonUtils.JsonFieldType.Integer)
            .append(FIELD_NAME_PARAMETER_STRING_PATTERN, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PARAMETER_MAX_VALUE, VertxJsonUtils.JsonFieldType.Integer)
            .append(FIELD_NAME_PARAMETER_MIN_VALUE, VertxJsonUtils.JsonFieldType.Integer)
            .append(FIELD_NAME_PARAMETER_DEFAULT_VALUE, VertxJsonUtils.JsonFieldType.Any)
            .append(FIELD_NAME_PARAMETER_DISPLAY_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PARAMETER_VALUE_ENUMS, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_PARAMETER_REQUIRES, VertxJsonUtils.JsonFieldType.Any)
            .append(FIELD_NAME_PARAMETER_IMPLIES, VertxJsonUtils.JsonFieldType.JsonObject)
            .append(FIELD_NAME_PARAMETER_DISPLAY_ONLY, VertxJsonUtils.JsonFieldType.Boolean)
            .append(FIELD_NAME_PARAMETER_TR_098_PATH_OVERRIDE, VertxJsonUtils.JsonFieldType.JsonArray);

    /**
     * Validate a Configuration Category Struct Instance.
     *
     * Throws CcException if validation fails.
     *
     * @param configCategory
     * @throws vertx2.CcException
     */
    public static void validate(JsonObject configCategory) throws SxaVertxException {
        // Validate Field Types
        VertxJsonUtils.validateFields(configCategory, MANDATORY_FIELDS, OPTIONAL_FIELDS);

        // Validate All Parameters
        JsonArray parameters = configCategory.getArray(FIELD_NAME_PARAMETERS);
        for (int i = 0; i < parameters.size(); i ++) {
            JsonObject aParameter = parameters.get(i);

            // Validate A Parameter
            try {
                VertxJsonUtils.validateFields(
                        aParameter,
                        PARAMETER_MANDATORY_FIELDS,
                        PARAMETER_OPTIONAL_FIELDS
                );
            } catch (SxaVertxException ex) {
                String paramName = aParameter.getString(AcsConstants.FIELD_NAME_NAME);
                if (paramName == null) {
                    throw new SxaVertxException("Found a Parameter Definition with no \"name\"!");
                } else {
                    throw new SxaVertxException("Definition of parameter " + paramName + ": " + ex.getMessage());
                }
            }

            /**
             * Convert all the "requires" field to String from JSON Object so we can store special operators like "$in"
             */
            JsonObject requires = aParameter.getObject(FIELD_NAME_PARAMETER_REQUIRES);
            if (requires != null) {
                aParameter.removeField(FIELD_NAME_PARAMETER_REQUIRES);
                aParameter.putString(FIELD_NAME_PARAMETER_REQUIRES, requires.encode());
            }
        }
    }

    /**
     * When storing to MongoDB, the "requires" field has been converted to String.
     *
     * When reading it from MongoDB, we need to convert it back to JSON Object.
     *
     * @param dbObject
     * @return
     */
    public static JsonObject convertRawDbObject(JsonObject dbObject) {
        JsonArray parameters = dbObject.getArray(FIELD_NAME_PARAMETERS);
        JsonArray newParameters = new JsonArray();
        for (int i = 0; i < parameters.size(); i ++) {
            JsonObject aParameter = parameters.get(i);

            Object requires = aParameter.getField(FIELD_NAME_PARAMETER_REQUIRES);

            if (requires != null && (requires instanceof  String)) {
                aParameter.removeField(FIELD_NAME_PARAMETER_REQUIRES);
                aParameter.putObject(FIELD_NAME_PARAMETER_REQUIRES, new JsonObject((String)requires));
            }
            newParameters.add(aParameter);
        }

        dbObject.removeField(FIELD_NAME_PARAMETERS);
        dbObject.putArray(FIELD_NAME_PARAMETERS, newParameters);
        return dbObject;
    }
}