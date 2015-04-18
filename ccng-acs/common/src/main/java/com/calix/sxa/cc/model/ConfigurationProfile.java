package com.calix.sxa.cc.model;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.cc.cache.ConfigurationCategoryCache;
import com.calix.sxa.cc.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Project:  SXA-CC
 *
 * Configuration Profile Data Model.
 *
 * @author: jqin
 */
public class ConfigurationProfile {
    private static final Logger log = LoggerFactory.getLogger(ConfigurationProfile.class.getName());

    /**
     * DB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "sxacc-configuration-profiles";

    /**
     * Field Name Constants
     */
    public static final String FIELD_NAME_CONFIGURATIONS = "configurations";
    public static final String FIELD_NAME_CATEGORY = "category";
    public static final String FIELD_NAME_PARAMETER_VALUES = AcsConstants.FIELD_NAME_PARAM_VALUES;
    public static final String FIELD_NAME_SERVICES = "services";
    public static final String FIELD_NAME_DYNAMIC_OBJECTS = "dynamicObjects";

    /**
     * Index Fields
     */
    public static final String[] INDEX_FIELDS = {
            AcsConstants.FIELD_NAME_ORG_ID,
            AcsConstants.FIELD_NAME_NAME
    };

    /**
     * Editable Fields
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{
        //add(AcsConstants.FIELD_NAME_NAME);
        add(AcsConstants.FIELD_NAME_DESCRIPTION);
        //add(FIELD_NAME_CONFIGURATIONS);
    }};

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator MANDATORY_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_CONFIGURATIONS, VertxJsonUtils.JsonFieldType.JsonArray);

    public static final VertxJsonUtils.JsonFieldValidator OPTIONAL_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
            //.append(AcsConstants.FIELD_NAME_INCLUDES, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_SERVICES, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_DYNAMIC_OBJECTS, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_PARAMETER_VALUES, VertxJsonUtils.JsonFieldType.JsonObject);

    /**
     * Validate a Configuration Profile Struct Instance.
     *
     * Throws CcException if validation fails.
     *
     * @param configProfile
     * @throws com.calix.sxa.cc.CcException
     */
    public static void validate(JsonObject configProfile) throws SxaVertxException {
        // Validate Field Types
        VertxJsonUtils.validateFields(configProfile, MANDATORY_FIELDS, OPTIONAL_FIELDS);

        /**
         * TODO: Validate actual parameters against a local Configuration Category Cache.
         */
    }

    /**
     * Convert a profile to a "ParameterValues" Struct.
     *
     * @param profile
     * @param categoryCache
     * @return
     */
    public static void processParameterValues(JsonObject profile, ConfigurationCategoryCache categoryCache)
            throws SxaVertxException {
        JsonObject paramValues = new JsonObject();
        JsonArray services = new JsonArray();
        JsonArray dynamicObjects = new JsonArray();
        JsonArray configurations = profile.getArray(FIELD_NAME_CONFIGURATIONS);
        HashMap<Number, String> vlanToServiceNameMap = new HashMap<>();

        // Traverse all categories in this profile
        for (int i = 0; i < configurations.size(); i ++) {
            JsonObject aCategory = configurations.get(i);

            String categoryName = aCategory.getString(FIELD_NAME_CATEGORY);
            JsonObject rawPerCategoryParamValues = aCategory.getObject(FIELD_NAME_PARAMETER_VALUES);

            // Get the Configuration Category from cache
            JsonObject categoryDef = (JsonObject) categoryCache.hashMap.get(categoryName);
            if (categoryDef == null) {
                log.error("Unable to find config category " + categoryName + " in cache!");
                continue;
            }

            // Parameters for this category
            JsonArray categoryParameters = categoryDef.getArray(ConfigurationCategory.FIELD_NAME_PARAMETERS);

            // Extract TR098 Path Prefix (default to "")
            String tr098PathPrefix = categoryDef.getString(ConfigurationCategory.FIELD_NAME_TR098_PATH_PREFIX, "");
            if (tr098PathPrefix.contains("${")) {
                // Extract the index parameter name from path prefix
                String indexParamName = tr098PathPrefix.substring(
                        tr098PathPrefix.indexOf("${") + 2,
                        tr098PathPrefix.lastIndexOf("}")
                );
                if (rawPerCategoryParamValues.containsField(indexParamName)) {
                    String index = rawPerCategoryParamValues.getField(indexParamName).toString();
                    tr098PathPrefix = tr098PathPrefix.replace("${" + indexParamName + "}", index);
                } else {
                    throw new SxaVertxException(
                            "Invalid Profile Instance! (" + indexParamName + " is undefined in "+ categoryName + ")"
                    );
                }
            }

            // Process all raw parameters in this category
            JsonObject perCategoryParamValues = new JsonObject();
            for (String paramName : rawPerCategoryParamValues.getFieldNames().toArray(new String[0])) {
                JsonObject paramDef = null;
                for (int j =0; j < categoryParameters.size(); j ++) {
                    paramDef = categoryParameters.get(j);
                    if (paramName.equals(paramDef.getString(AcsConstants.FIELD_NAME_NAME))) {
                        break;
                    } else {
                        paramDef = null;
                    }
                }

                if (paramDef == null) {
                    log.error("Unable to find definition for parameter " + paramName + "!");
                    continue;
                }

                // For Display only?
                if (paramDef.getBoolean(ConfigurationCategory.FIELD_NAME_PARAMETER_DISPLAY_ONLY, false)) {
                    continue;
                }

                if (paramDef.containsField(ConfigurationCategory.FIELD_NAME_PARAMETER_TR_098_PATH_OVERRIDE)) {
                    JsonArray tr098Names;

                    Object tr098Override = paramDef.getField(
                            ConfigurationCategory.FIELD_NAME_PARAMETER_TR_098_PATH_OVERRIDE);
                    if (tr098Override instanceof String) {
                        tr098Names = new JsonArray().add(tr098Override);
                    } else {
                        tr098Names = (JsonArray)tr098Override;
                    }
                    for (int k = 0; k < tr098Names.size(); k ++) {
                        VertxJsonUtils.deepAdd(
                                perCategoryParamValues,
                                (String) tr098Names.get(k),
                                rawPerCategoryParamValues.getValue(paramName)
                        );
                    }
                } else {
                    // TR098 name is "PREFIX + NAME"
                    String tr098Name = tr098PathPrefix + paramName;
                    VertxJsonUtils.deepAdd(
                            perCategoryParamValues,
                            tr098Name,
                            rawPerCategoryParamValues.getValue(paramName)
                    );
                }

                // Does the parameter name contain '.'?
                if (paramName.contains(".")) {
                    // Convert '.' to "${DOT}"
                    VertxJsonUtils.renameField(
                            rawPerCategoryParamValues,
                            paramName,
                            paramName.replace(".", "${DOT}")
                    );
                }
            }

            // WAN Service?
            String serviceType = categoryDef.getString(ConfigurationCategory.FIELD_NAME_SERVICE_TYPE);
            if (serviceType != null) {
                JsonObject serviceValues = categoryDef.getObject(ConfigurationCategory.FIELD_NAME_SERVICE_VALUES);
                String serviceName = serviceValues.getString("Name");

                try {
                    Integer vlanId = perCategoryParamValues.getInteger("X_000631_VlanMuxID");
                    if (vlanId == null) {
                        throw new SxaVertxException("VLAN ID is undefined for " + serviceName + "!");
                    }
                    String vlanIdString = (vlanId > 0)?
                            "VLAN " + String.valueOf(vlanId)
                            :
                            "Untagged VLAN";
                    if (vlanToServiceNameMap.containsKey(vlanId)) {
                        throw new SxaVertxException(vlanIdString + " is already used by "
                                + vlanToServiceNameMap.get(vlanId) + "!");
                    } else {
                        vlanToServiceNameMap.put(vlanId, serviceName);
                    }
                } catch (Exception ex) {
                    throw new SxaVertxException("Illegal Profile Format!");
                }

                /**
                 * Add service parameters plus service-values into service array
                 */
                VertxJsonUtils.merge(
                        perCategoryParamValues,
                        categoryDef.getObject(ConfigurationCategory.FIELD_NAME_SERVICE_VALUES));

                services.add(perCategoryParamValues);
            } else if (tr098PathPrefix.contains("{i}")) {
                /**
                 * Dynamic  Objects
                 */
                // Add the "keyParameter" field (if any) into the "parameterValues" struct
                if (categoryDef.containsField(ConfigurationCategory.FIELD_NAME_KEY_PARAMETER)) {
                    perCategoryParamValues.putObject(
                            ConfigurationCategory.FIELD_NAME_KEY_PARAMETER,
                            categoryDef.getObject(ConfigurationCategory.FIELD_NAME_KEY_PARAMETER)
                    );
                }
                // Add the TR098 Path Prefix
                perCategoryParamValues.putString(
                        ConfigurationCategory.FIELD_NAME_TR098_PATH_PREFIX,
                        categoryDef.getString(ConfigurationCategory.FIELD_NAME_TR098_PATH_PREFIX)
                );
                dynamicObjects.add(perCategoryParamValues);
            } else {
                /**
                 * Regular Profile Instance
                 */
                VertxJsonUtils.merge(paramValues, perCategoryParamValues);
            }
        }

        if (paramValues.size() > 0) {
            profile.putObject(ConfigurationProfile.FIELD_NAME_PARAMETER_VALUES, paramValues);
        }

        if (services.size() > 0) {
            /**
             * TODO: Check for VLAN Conflicts
             */
            if (services.size() > 1) {

            }

            profile.putArray(ConfigurationProfile.FIELD_NAME_SERVICES, services);
        }

        if (dynamicObjects.size() > 0) {
            profile.putArray(ConfigurationProfile.FIELD_NAME_DYNAMIC_OBJECTS, dynamicObjects);
        }
    }
}
