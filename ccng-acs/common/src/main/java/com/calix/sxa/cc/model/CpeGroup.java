package com.calix.sxa.cc.model;

import com.calix.sxa.cc.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  SXA-CC
 *
 * This class defines the data model for the CPE Group Objects, as well as many util methods such as comparing/matching
 * a CPE to a group's CPE filter.
 *
 * Each CPE Group object shall include the following attributes:
 * - Organization Id
 * - Group Name
 * - Group Description
 * - CPE Matching Filter
 *
 * @author: Ron
 */

public class CpeGroup extends MultiTenantObject {
    private static final Logger log = LoggerFactory.getLogger(CpeGroup.class.getName());

    // CPE Group Name
    public String name;
    // CPE Group Description
    public String description;
    // CPE Matching Filter
    public JsonObject cpeFilter;
    // Dynamic vs. Static
    public boolean bDynamic;

    /**
     * Cpe Group Constant Strings
     */
    public static final String DB_COLLECTION_NAME = "sxacc-groups";
    public static final String FIELD_NAME_GROUP_TYPE = "type";
    public static final String GROUP_TYPE_STATIC = "static";
    public static final String GROUP_TYPE_DYNAMIC = "dynamic";

    /**
     * A group is uniquely identified by "orgId" + "group name".
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
        add(AcsConstants.FIELD_NAME_CPE_FILTER);
    }};

    /**
     * Comparison/Logical Operators
     */
    public static final String OPERATOR_PREFIX = "$";
    public static final String OPERATOR_OR = OPERATOR_PREFIX + "or";
    public static final String OPERATOR_GREATER_THAN = OPERATOR_PREFIX + "gt";
    public static final String OPERATOR_GREATER_THAN_OR_EQUAL = OPERATOR_PREFIX + "gte";
    public static final String OPERATOR_LESS_THAN = OPERATOR_PREFIX + "lt";
    public static final String OPERATOR_LESS_THAN_OR_EQUAL = OPERATOR_PREFIX + "lte";
    public static final String OPERATOR_IN = OPERATOR_PREFIX + "in";
    public static final String OPERATOR_NOT_IN = OPERATOR_PREFIX + "nin";
    public static final String OPERATOR_REGEX = OPERATOR_PREFIX + "regex";
    public static final String OPERATOR_OPTIONS = OPERATOR_PREFIX + "options";

    /**
     * TODO: Add validation Method to validate filter content.
     */

    /**
     * Construct CpeGroup from JsonObject
     *
     * @param jsonObject
     */
    public CpeGroup(JsonObject jsonObject) {
        name = jsonObject.getString(AcsConstants.FIELD_NAME_NAME);
        cpeFilter = new JsonObject(jsonObject.getString(AcsConstants.FIELD_NAME_CPE_FILTER));
        description = jsonObject.getString(AcsConstants.FIELD_NAME_DESCRIPTION);

        // Check type (dynamic vs. static)
        String type = jsonObject.getString(FIELD_NAME_GROUP_TYPE);
        if (GROUP_TYPE_STATIC.equals(type)) {
            bDynamic = false;
        } else {
            bDynamic = true;
        }
    }

    /**
     * check if the cpe satisfies the group matching criteria
     *
     * @param cpe
     * @return  true if the cpe satisfied the group matching criteria
     */
    public boolean match(Cpe cpe){
        return match(cpe.cpeJsonObj, cpeFilter);
    }

    /**
     * Static version:
     *
     * check if the cpe satisfies the group matching criteria.
     *
     * @param cpeJsonObject
     * @param filter
     * @return
     */
    public static boolean match(JsonObject cpeJsonObject, JsonObject filter) {
        boolean isOr = false;
        if (filter.getFieldNames().size() == 1 && filter.containsField(OPERATOR_OR))
            isOr = true;

        return recursiveMatch(cpeJsonObject, filter, isOr);
    }

    /**
     * recursive check if the cpe deviceId value satisfied the matching criteria
     * currently the matching criteria only applies to the DeviceId structure
     * @param deviceId
     * @param filter

     * @return  true if the cpe deviceId value satisfied the matching criteria
     */
    public static boolean recursiveMatch(JsonObject deviceId,JsonObject filter,boolean isOr) {
        Object valueCpe, valueFilter;
        boolean result = !isOr;
        boolean inner_result = false;

        for (String fieldName : filter.getFieldNames()) {
            valueFilter = filter.getField(fieldName);
            if(fieldName.startsWith(OPERATOR_OR)&&valueFilter instanceof  JsonArray){
                for( Object value : (JsonArray)valueFilter) {
                    result = recursiveMatch(deviceId, (JsonObject)value,true);
                }
            }
            else {
                valueCpe = deviceId.getField(fieldName);
                inner_result = basicMatch(valueFilter, valueCpe);
                if(!isOr) {
                    if (!inner_result) {
                        result = false;
                        break;
                    }
                }
                if(isOr) {
                    if (inner_result) {
                        result = true;
                        break;
                    }
                }
            }
        }
        return result;
    }

    /**
     * basic check value according to some operators
     * currently only support "gt", "gte", "lt", "lte", "in"
     *
     * @param filterValue
     * @param cpeValue

     * @return  true if the value satisfied the operator
     */
    public static boolean basicMatch(Object filterValue,Object cpeValue) {
        boolean result = false;

        // Default to ""
        if (cpeValue == null)
            cpeValue = "";

        if (filterValue instanceof JsonObject) {
            for (String operator : ((JsonObject) filterValue).getFieldNames()) {
                Object subFilterValue = ((JsonObject) filterValue).getField(operator);

                //log.debug("operator " + operator + ", sub filter value: " + subFilterValue);

                // Compare Strings
                int compareResult = cpeValue.toString().compareTo(subFilterValue.toString());

                switch (operator) {
                    case OPERATOR_GREATER_THAN:
                        if (compareResult > 0) {
                            result = true;
                        }
                        break;

                    case OPERATOR_GREATER_THAN_OR_EQUAL:
                        if (compareResult >= 0) {
                            result = true;
                        }
                        break;

                    case OPERATOR_LESS_THAN:
                        if (compareResult < 0) {
                            result = true;
                        }
                        break;

                    case OPERATOR_LESS_THAN_OR_EQUAL:
                        if (compareResult <= 0) {
                            result = true;
                        }
                        break;

                    case OPERATOR_IN:
                        for (Object value : ((JsonArray) subFilterValue)) {
                            if (cpeValue.equals(value)) {
                                result = true;
                                break;
                            }
                        }
                        break;

                    case OPERATOR_NOT_IN:
                        for (Object value : ((JsonArray) subFilterValue)) {
                            if (!cpeValue.equals(value)) {
                                result = true;
                                break;
                            }
                        }
                        break;

                    case OPERATOR_OPTIONS:
                        // Do nothing. Used by MongoDB only
                        break;

                    case OPERATOR_REGEX:
                        // We only support "contains" via regex

                        /**
                         * Special Case for SXACC-1296
                         */
                        String regex = subFilterValue.toString();
                        if (regex.charAt(0) == '^' && regex.charAt(regex.length() - 1) == '$') {
                            // Remove the leading '^' and trailing '$'
                            regex = regex.substring(1, regex.length() - 1);
                        }

                        if (cpeValue.toString().contains(regex)) {
                            result = true;
                        }
                        break;
                }
            }
        } else {
            if (filterValue.equals(cpeValue)) {
                result = true;
            }
        }
        return result;
    }

    /**
     * check if this device group matches a model implicitly or explicitly.
     *
     * @param model
     * @param bImplicitly   Consider match if the group filter does not contain any models
     */
    public boolean bGroupMatchOnModel(String model, boolean bImplicitly) {
        if (!cpeFilter.containsField(CpeDeviceType.FIELD_NAME_MODEL_NAME)) {
            // No model matcher
            if (bImplicitly)
                return true;
            else
                return false;
        } else {
            // Has model matcher
            Object modelMatcher = cpeFilter.getField(CpeDeviceType.FIELD_NAME_MODEL_NAME);
            return basicMatch(modelMatcher, model);
        }
    }

    /**
     * check if this device group matches any GPON model implicitly or explicitly.
     *
     * @param bImplicitly   Consider match if the group filter does not contain any models
     */
    public boolean bGroupMatchOnGponModel(boolean bImplicitly) {
        for (String aGponModel: CpeDeviceType.ALL_GPON_MODELS) {
            if (bGroupMatchOnModel(aGponModel, bImplicitly) == false) {
                continue;
            }
            return true;
        }
        return false;
    }

    /**
     * Generate a MongoDB CPE Collection Query Matcher object by the filter.
     *
     * @return  mongodb query matcher
     */
    public JsonObject toCpeQueryMatcher() {
        if (bDynamic) {
            return cpeFilter;
        } else {
            /**
             * TODO:
             */
            return null;
        }
    }
}
