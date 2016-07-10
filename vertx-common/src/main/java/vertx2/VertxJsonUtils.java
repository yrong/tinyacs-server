package vertx2;

import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Vert.X JSON Utils
 *
 * @author: ronyang
 */
public class VertxJsonUtils {
    private static final Logger log = LoggerFactory.getLogger(VertxJsonUtils.class.getName());

    /**
     * JSON Field Type Enum
     */
    public static enum JsonFieldType {
        String,
        Integer,
        Boolean,
        JsonObject,
        JsonArray,
        DateTime,
        Binary,
        Any,
        Null
    }

    /**
     * Static Exceptions
     */
    public static final VertxException NULL_POINTER = new VertxException("Null Pointer!");
    public static final VertxException FIELD_TYPE_MISMATCH = new VertxException("Field Type Mismatch!");
    public static final VertxException MISSING_MANDATORY_FIELD = new VertxException("Missing Mandatory Field!");
    public static final VertxException UNKNOWN_FIELD = new VertxException("Unknown Field!");

    /**
     * Append a new field to a JSON Object
     *
     * @param dest          The Target JSON Object
     * @param fieldName     Name of the new Field
     * @param value         Value of the new Field
     *
     * @return              The Target JSON Object
     */
    public static JsonObject append(JsonObject dest, String fieldName, Object value) {
        if (fieldName == null) {
            log.error("fieldName is null!");
            return null;
        }

        if (dest == null) {
            // Create a new JSON Object if needed
            dest = new JsonObject();
        }

        try {
            dest.putValue(fieldName, value);
        } catch (org.vertx.java.core.VertxException ex) {
            log.error(ex.getMessage());
            return null;
        }

        return dest;
    }

    /**
     * Check if the type of given field exists and has String value; if yes, return the String value.
     *
     * Otherwise return null.
     *
     * No exception will be thrown.
     *
     * @param obj
     * @param fieldName
     */
    public static String checkAndGetStringField(JsonObject obj, String fieldName) {
        try {
            return obj.getString(fieldName);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Check if the type of given field exists and has Boolean value; if yes, return the Boolean value.
     *
     * Otherwise return null.
     *
     * No exception will be thrown.
     *
     * @param obj
     * @param fieldName
     */
    public static Boolean checkAndGetBooleanField(JsonObject obj, String fieldName) {
        try {
            return obj.getBoolean(fieldName);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Check if the type of given field exists and has JsonObject value; if yes, return the JsonObject value.
     *
     * Otherwise return null.
     *
     * No exception will be thrown.
     *
     * @param obj
     * @param fieldName
     */
    public static JsonObject checkAndGetJsonObjectField(JsonObject obj, String fieldName) {
        try {
            return obj.getObject(fieldName);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Check if the type of given field exists and has Boolean value; if yes, return the Boolean value.
     *
     * Otherwise return null.
     *
     * No exception will be thrown.
     *
     * @param obj
     * @param fieldName
     */
    public static JsonArray checkAndGetArrayField(JsonObject obj, String fieldName) {
        try {
            return obj.getArray(fieldName);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Check if the type of given field exists and has Number value; if yes, return the Number value.
     *
     * Otherwise return null.
     *
     * No exception will be thrown.
     *
     * @param obj
     * @param fieldName
     */
    public static Number checkAndGetNumberField(JsonObject obj, String fieldName) {
        try {
            return obj.getNumber(fieldName);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * Validate a given JSON Object with mandatory/optional field name/type pairs.
     * 
     * @param obj               The target JSON object to be validated.
     * @param mandatoryFields   Mandatory Field Name/Type pairs
     * @param optionalFields    Optional Field Name/Type pairs
     *                           
     * @throws VertxException if:
     *          - obj or mandatoryFields is null, or
     *          - one of the mandatory fields is missing, or
     *          - one of the mandatory fields has different type, or          
     *          - one of the optional fields has different type, or          
     *          - obj contains one or more unknown fields          
     */
    public static void validateFields(
            final JsonObject obj,
            final JsonFieldValidator mandatoryFields,
            final JsonFieldValidator optionalFields)
            throws VertxException {
        // Check for null pointer
        if (obj == null) {
            throw NULL_POINTER;
        }
        
        // Get all field names from the target JSON object
        final Set<String> allFieldNames = obj.getFieldNames();
        
        // Validate Mandatory Fields
        if (mandatoryFields != null) {
            for (String mandatoryFieldName : mandatoryFields.keySet()) {
                if (!allFieldNames.contains(mandatoryFieldName)) {
                    log.error("Missing Mandatory Field " + mandatoryFieldName + "!");
                    //throw MISSING_MANDATORY_FIELD;
                    throw new VertxException("Missing Mandatory Field " + mandatoryFieldName + "!");
                }

                Object value = obj.getField(mandatoryFieldName);
                if (!validateFieldType(value, mandatoryFields.get(mandatoryFieldName))) {
                    throw new VertxException("Field Type Mismatch! Filed name: \"" + mandatoryFieldName
                            + "\": expect " + mandatoryFields.get(mandatoryFieldName).name()
                            + ", actual type is " + value.getClass().getSimpleName() + "!");
                }
            }
        }
        
        // Validate Optional Fields
        if (optionalFields != null) {
            for (String optionalFieldName : optionalFields.keySet()) {
                if (!allFieldNames.contains(optionalFieldName)) {
                    continue;
                }

                Object value = obj.getField(optionalFieldName);
                if (!validateFieldType(value, optionalFields.get(optionalFieldName))) {
                    throw new VertxException("Field Type Mismatch! Filed name: \"" + optionalFieldName
                            + "\": expect " + optionalFields.get(optionalFieldName).name()
                            + ", actual type is " + value.getClass().getSimpleName() + "!");
                }
            }
        }

        // Check for unknown fields
        for (String aField : allFieldNames) {
            if (mandatoryFields != null && mandatoryFields.containsKey(aField)) {
                continue;
            }

            if (optionalFields != null && optionalFields.containsKey(aField)) {
                continue;
            }

            log.error("Found an unknown field " + aField + "!");
            throw new VertxException("Unknown Field " + aField + "!");
        }
    }

    /**
     * Inner Class that extends HashMap<String, JsonFieldType> for validating JSON Fields
     */
    public static class JsonFieldValidator extends HashMap<String, JsonFieldType> {
        /**
         * Constructor with a single field name/type pair.
         *
         * @param name
         * @param type
         */
        public JsonFieldValidator(String name, JsonFieldType type) {
            this.put(name, type);
        }

        /**
         * Constructor with no argument.
         */
        public JsonFieldValidator() {
        }

        /**
         * Append another field name/type pair.
         *
         * @param name
         * @param type
         * @return  This JsonFieldValidator.
         */
        public JsonFieldValidator append(String name, JsonFieldType type) {
            this.put(name, type);
            return this;
        }

        /**
         * Clone/Copy.
         */
        public JsonFieldValidator copy() {
            /**
             * Create a new empty validator instance
             */
            JsonFieldValidator newValidator = new JsonFieldValidator();

            /**
             * Iterate and add entries of this validator to the new one
             */
            Set<Map.Entry<String, JsonFieldType>> entries = this.entrySet();
            Iterator<Map.Entry<String, JsonFieldType>> iterator = entries.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonFieldType> anEntry = iterator.next();
                newValidator.append(anEntry.getKey(), anEntry.getValue());
            }

            /**
             * Return the new one
             */
            return newValidator;
        }
    }

    /**
     * Validate the type of the field against the expected type.
     *
     * @param fieldValue
     * @param expectType
     *
     * @return  true if types match, otherwise returns false.
     */
    public static boolean validateFieldType(Object fieldValue, JsonFieldType expectType) {
        if (expectType.equals(JsonFieldType.Any)) {
            return true;
        }

        if (fieldValue != null && fieldValue.getClass().getSimpleName().equals(expectType.name())) {
            return true;
        }

        // Integer and Long are equivalent
        if (expectType == JsonFieldType.Integer && fieldValue instanceof Long) {
            return true;
        }

        // Binary Array
        if (expectType == JsonFieldType.Binary && fieldValue instanceof byte[]) {
            return true;
        }

        // "Date" is a special case
        if (expectType == JsonFieldType.DateTime) {
            if (fieldValue instanceof String) {
                try {
                    iso8601StringToDate((String)fieldValue);
                    return true;
                } catch (Exception ex) {
                    log.error("Invalid ISO8061 DateTime String " + fieldValue
                            + "! (caught " + ex.getMessage() + " exception)");
                    return false;
                }
            } else {
                return true;
            }
        }

        return false;
    }

    /**
     * Add a child field by a deep path.
     *
     * For example, path "InternetGatewayDevice.LANDevice.1.WLANConfiguration.2.SSID" and value "JackQin_SSID2" will
     * insert the following JSON object into the target JSON Object:
     *
     *  "InternetGatewayDevice": {
     *      "LANDevice": {
     *          "1": {
     *              "WLANConfiguration": {
     *                  "2": {
     *                      "SSID": "JackQin_SSID2"
     *                  }
     *              }
     *          }
     *      }
     *  }     *
     *
     * @param dest
     * @param path
     * @param value
     * @return
     */
    public static JsonObject deepAdd(JsonObject dest, String path, Object value) {
        if (dest == null) {
            dest = new JsonObject();
        }

        // Create the object hierarchy
        JsonObject nextJson = dest;
        String[] subPaths = StringUtil.split(path, '.');
        for (int i = 0; i < (subPaths.length - 1); i ++) {
            String subPath = subPaths[i];
            if (nextJson.getObject(subPath) == null) {
                nextJson.putObject(subPath, new JsonObject());
            }
            nextJson = nextJson.getObject(subPath);
        }

        // Append the new field
        append(nextJson, subPaths[subPaths.length - 1], value);

        return dest;
    }

    /**
     * Return a deeply embedded field value within a multi-level JSON Object.
     *
     * Path String is segmented via ".".
     *
     * @param jsonObject
     * @param path
     * @param <T>
     * @return
     */
    public static <T> T deepGet(JsonObject jsonObject, String path) {
        JsonObject nextJson = jsonObject;
        String[] subPaths = StringUtil.split(path, '.');
        for (int i = 0; i < (subPaths.length - 1); i ++) {
            String subPath = subPaths[i];
            nextJson = nextJson.getObject(subPath);
            if (nextJson == null) {
                return null;
            }
        }
        return nextJson.getField(subPaths[subPaths.length - 1]);
    }

    /**
     * Remove a deeply embedded field within a multi-level JSON Object.
     *
     * Path String is segmented via ".".
     *
     * @param jsonObject
     * @param path
     * @return
     */
    public static void deepRemove(JsonObject jsonObject, String path) {
        JsonObject nextJson = jsonObject;
        String[] subPaths = StringUtil.split(path, '.');
        for (int i = 0; i < (subPaths.length - 1); i ++) {
            String subPath = subPaths[i];
            nextJson = nextJson.getObject(subPath);
            if (nextJson == null) {
                return;
            }
        }
        nextJson.removeField(subPaths[subPaths.length - 1]);
    }

    /**
     * Merge/Add a JSON Object into an existing JSON Object.
     *
     * If there is a conflict, use the the value from extra JSON Object.
     *
     * For example:
     *
     * "dest": {
     *     "a": "value of a",
     *     "b": "value of b"
     * }
     *
     * "extra": {
     *     "b": "new value of b",
     *     "c": "value of c"
     * }
     *
     * The merge result will be:
     * "result": {
     *     "a": "value of a",
     *     "b": "new value of b",
     *     "c": "value of c"
     * }
     *
     * @param dest      The existing JSON Object
     * @param extra     The extra/additional JSON Object to be merged/added into dest
     */
    public static void merge(JsonObject dest, JsonObject extra) {
        // Traverse all fields in A
        String[] oldFieldNames = new String[dest.getFieldNames().size()];
        for (int i = 0; i < oldFieldNames.length; i ++) {
            oldFieldNames[i] = (String)dest.getFieldNames().toArray()[i];
        }

        for (String fieldName : oldFieldNames) {
            if (!extra.containsField(fieldName)) {
                //log.info(fieldName + " is in dest but missing in extra!");
                // Keep the existing value
                continue;
            }

            // Get the value from A and B
            Object valueA = dest.getField(fieldName);
            Object valueB = extra.getField(fieldName);

            // Is the value a JSON Array?
            if (valueA instanceof JsonArray) {
                if (valueB instanceof JsonArray) {
                    // Merge 2 JSON Arrays
                    for (int i = 0; i < ((JsonArray) valueB).size(); i ++) {
                        ((JsonArray) valueA).add(((JsonArray) valueB).get(i));
                    }
                } else {
                    // log.info(fieldName + " is a JSON Array in dest but is a " + valueB.getClass().getSimpleName()
                    // + " in extra!");
                    dest.removeField(fieldName);
                    append(dest, fieldName, extra.getField(fieldName));
                }
            } else if (valueA instanceof JsonObject) {
                // Is the value a JSON Object?
                if (valueB instanceof JsonObject) {
                    merge((JsonObject)valueA, (JsonObject)valueB);
                } else {
                    log.info(fieldName + " is a JSON Object in dest but is a " +
                            valueB.getClass().getSimpleName() + " in extra!");
                    // Overwrite the existing value
                    dest.removeField(fieldName);
                    append(dest, fieldName, valueB);
                }
            } else {
                // For everything else, just compare them after converting to plain strings
                if (!valueA.toString().equals(valueB.toString())) {
                    //log.info("Primitive Field " + fieldName + " mismatch! ("
                    //        + valueA.toString() + " vs. " + valueB.toString() + ")");
                    dest.removeField(fieldName);
                    append(dest, fieldName, extra.getField(fieldName));
                }
            }
        }

        // Traverse all fields in extra
        for (String fieldName : extra.getFieldNames()) {
            if (!dest.containsField(fieldName)) {
                //log.info(fieldName + " is in objB but missing in objA!");
                append(dest, fieldName, extra.getField(fieldName));
            }
        }

    }

    /**
     * Convert a given JSON Object which contains field names with "." in it to a String field
     * @param original
     * @return
     */
    public static void convertFieldToString(JsonObject original, String fieldName) {
        Object field = original.getField(fieldName);
        if (field != null) {
            String str = field.toString();
            original.removeField(fieldName);
            original.putString(fieldName, str);
        }
    }

    /**
     * Compare 2 JSON Object recursively and print mismatches.
     *
     * @param objA
     * @param objB
     * @param skipFields
     *
     * @return true if the 2 objects are identical, or false.
     */
    public static boolean compare(JsonObject objA, JsonObject objB, Collection<String> skipFields) {
        return compare(objA, objB, skipFields, true);
    }

    /**
     * Compare 2 JSON Object recursively.
     *
     * @param objA
     * @param objB
     * @param skipFields
     *
     * @return true if the 2 objects are identical, or false.
     */
    public static boolean compare(
            JsonObject objA,
            JsonObject objB,
            Collection<String> skipFields,
            boolean bPrintMisMatches) {
        // Traverse all fields in A
        for (String fieldName : objA.getFieldNames()) {
            if (skipFields.contains(fieldName)) {
                continue;
            }

            if (!objB.containsField(fieldName)) {
                if (bPrintMisMatches)
                    log.info(fieldName + " is in objA but missing in objB!");
                return false;
            }

            // Get the value from A and B
            Object valueA = objA.getField(fieldName);
            Object valueB = objB.getField(fieldName);

            // Is the value a JSON Array?
            if (valueA instanceof JsonArray) {
                if (valueB instanceof JsonArray) {
                    boolean subResult = ((JsonArray) valueA).encode().equals(((JsonArray) valueB).encode());
                    if (subResult == false) {
                        if (bPrintMisMatches)
                            log.info("JSON Array " + fieldName + " mismatch! (" + ((JsonArray) valueA).encode()
                                + " vs. " + ((JsonArray) valueB).encode() + ")");
                        return false;
                    }
                } else {
                    log.info(fieldName + " is a JSON Array in objA but is a " +
                            valueB.getClass().getSimpleName() + " in objB!");
                    return false;
                }
            } else if (valueA instanceof JsonObject) {
                // Is the value a JSON Object?
                if (valueB instanceof JsonObject) {
                    boolean subResult = compare((JsonObject)valueA, (JsonObject)valueB, skipFields);
                    if (subResult == false) {
                        if (bPrintMisMatches)
                            log.info("JSON Object Field " + fieldName + " mismatch! (" + valueA + " vs. " + valueB + ")");
                        return false;
                    }
                } else {
                    if (bPrintMisMatches)
                        log.info(fieldName + " is a JSON Object in objA but is a " +
                            valueB.getClass().getSimpleName() + " in objB!");
                    return false;
                }
            } else {
                // For everything else, just compare them after converting to plain strings
                if (!valueA.toString().equals(valueB.toString())) {
                    if (bPrintMisMatches)
                        log.info("Primitive Field " + fieldName + " mismatch! ("
                            + valueA.toString() + " vs. " + valueB.toString() + ")");
                    return false;
                }
            }
        }

        // Traverse all fields in B
        for (String fieldName : objB.getFieldNames()) {
            if (skipFields.contains(fieldName)) {
                continue;
            }
            if (!objA.containsField(fieldName)) {
                log.info(fieldName + " is in objB but missing in objA!");
                return false;
            }
        }

        return true;
    }

    /**
     * Convert ISO8601 DateTime String to Java Date
     */
    public static final DateFormat ISO8601_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
    public static final DateFormat ISO8601_DATE_FORMAT_WITH_MS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    public static Date iso8601StringToDate(String dateTimeString) throws ParseException {
        try {
            return ISO8601_DATE_FORMAT.parse(dateTimeString);
        } catch (ParseException ex) {
            return ISO8601_DATE_FORMAT_WITH_MS.parse(dateTimeString);
        }
    }

    /**
     * Convert MongoDB "$date" object to a plain String
     */
    public static void convertMongoDateToString(JsonObject jsonObject, String dateFieldName) {
        String isoDateString = jsonObject.getObject(dateFieldName).getString(VertxMongoUtils.MOD_MONGO_DATE);
        jsonObject.removeField(dateFieldName);
        jsonObject.putString(dateFieldName, isoDateString);
    }

    /**
     * Get a new ISO8601 Date String with the current date.
     * @return
     */
    public static String getIso8601DateString() {
        return ISO8601_DATE_FORMAT_WITH_MS.format(new Date());
    }

    /**
     * Get a new ISO8601 Date String with the provided date String which may end with "+00:00".
     * @return
     */
    public static String getIso8601DateString(String rawDateString) {
        try {
            Date date = iso8601StringToDate(rawDateString);
            return ISO8601_DATE_FORMAT_WITH_MS.format(date);
        } catch (ParseException e) {
            log.error("Invalid Date String " + rawDateString + "!");
            return "";
        }
    }

    /**
     * Rename a field.
     *
     * @param parentObject
     * @param oldFieldName
     * @param newFieldName
     */
    public static void renameField(JsonObject parentObject, String oldFieldName, String newFieldName) {
        Object obj = parentObject.getField(oldFieldName);

        if (obj == null) {
            return;
        }

        parentObject.putValue(newFieldName, obj);
        parentObject.removeField(oldFieldName);
    }

    /**
     * Convert the "." in field names to "$DOT" if saving to MongoDB recursively, or
     * Convert the "$DOT" in field names to "." if the JSON Object was read from MongoDB.
     *
     * @param jsonObject
     * @param toMongoDB
     */
    public static void convertDotInFieldNames(JsonObject jsonObject, boolean toMongoDB) {
        if (jsonObject == null) {
            return;
        }

        String[] fieldNames = jsonObject.getFieldNames().toArray(new String[0]);
        for (String fieldName : fieldNames) {
            if (toMongoDB && fieldName.contains(".")) {
                renameField(jsonObject, fieldName, fieldName.replace(".", "${DOT}"));
            } else if (toMongoDB == false) {
                if (fieldName.contains("${DOT}")) {
                    renameField(jsonObject, fieldName, fieldName.replace("${DOT}", "."));
                } else if (fieldName.contains("$DOT")) {
                    renameField(jsonObject, fieldName, fieldName.replace("$DOT", "."));
                }
            }

            if (jsonObject.getField(fieldName) instanceof JsonObject) {
                convertDotInFieldNames(jsonObject.getObject(fieldName), toMongoDB);
            } else if (jsonObject.getField(fieldName) instanceof JsonArray) {
                for (Object arrayElement: jsonObject.getArray(fieldName)) {
                    if (arrayElement instanceof JsonObject) {
                        convertDotInFieldNames((JsonObject)arrayElement, toMongoDB);
                    }
                }
            }
        }
    }
}
