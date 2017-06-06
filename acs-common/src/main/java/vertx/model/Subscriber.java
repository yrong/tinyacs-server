package vertx.model;

import io.vertx.ext.mongo.MongoClient;
import vertx.VertxException;
import vertx.VertxJsonUtils;
import vertx.VertxMongoUtils;
import vertx.util.AcsConstants;
import vertx.util.AcsMiscUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  cwmp
 *
 * @author: ronyang
 */
public class Subscriber {
    private static final Logger log = LoggerFactory.getLogger(Subscriber.class.getName());

    /**
     * DB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "sxa-subscribers";

    /**
     * Field Name Constants
     */
    public static final String FIELD_NAME_LOCATIONS = "locations";
    public static final String FIELD_NAME_CUSTOM_ID = "customId";
    public static final String FIELD_NAME_NAME = "name";
    public static final String FIELD_NAME_TYPE = "type";
    public static final String FIELD_NAME_APP_DATA = "appData";

    // Sub fields within locations/contacts
    public static final String FIELD_NAME_LOCATIONS_PRIMARY = "primary";
    public static final String FIELD_NAME_LOCATIONS_DEVICES = "devices";
    public static final String FIELD_NAME_LOCATIONS_ADDRESS = "address";
    public static final String FIELD_NAME_LOCATIONS_CONTACTS = "contacts";
    public static final String FIELD_NAME_LOCATIONS_CONTACTS_EMAIL = "email";
    public static final String FIELD_NAME_LOCATIONS_CONTACTS_PHONE = "phone";
    public static final String FIELD_NAME_LOCATIONS_CONTACTS_FIRST_NAME = "firstName";
    public static final String FIELD_NAME_LOCATIONS_CONTACTS_LAST_NAME = "lastName";
    public static final String FIELD_NAME_LOCATIONS_CONTACTS_PRIMARY = "primary";

    // Sub Fields within "appData"
    public static final String FIELD_NAME_APP_SERVICE_CONNECT = "ServiceConnect";
    public static final String FIELD_NAME_APP_CONSUMER_CONNECT = "ConsumerConnect";
    // Service Connect App Specific Fields
    public static final String FIELD_NAME_SERVICES = "services";
    public static final String FIELD_NAME_SERVICE_PREFIX = "servicePrefix";
    public static final String SERVICE_PREFIX_FULL_PATH =
            Subscriber.FIELD_NAME_APP_DATA + "."
            + Subscriber.FIELD_NAME_APP_SERVICE_CONNECT + "."
            + Subscriber.FIELD_NAME_SERVICE_PREFIX;

    /**
     * The "customId" and "servicePrefix" fields, if present, must be unique across the same organization.
     */
    public static final String[] INDEX_FIELDS = {
            AcsConstants.FIELD_NAME_ORG_ID,
            FIELD_NAME_NAME
    };

    /**
     * Editable Fields
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{
        add(FIELD_NAME_NAME);
        add(FIELD_NAME_TYPE);
        add(FIELD_NAME_CUSTOM_ID);
        add(FIELD_NAME_LOCATIONS);
        add(FIELD_NAME_APP_DATA);
    }};

    /**
     * Static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator MANDATORY_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator OPTIONAL_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_TYPE, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_CUSTOM_ID, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_LOCATIONS, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_APP_DATA, VertxJsonUtils.JsonFieldType.JsonObject);

    // Validators for the "locations" field
    public static final VertxJsonUtils.JsonFieldValidator LOCATIONS_MANDATORY_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(FIELD_NAME_LOCATIONS_ADDRESS, VertxJsonUtils.JsonFieldType.JsonObject);

    public static final VertxJsonUtils.JsonFieldValidator LOCATIONS_OPTIONAL_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(FIELD_NAME_LOCATIONS_CONTACTS, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_LOCATIONS_DEVICES, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_LOCATIONS_CONTACTS, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_LOCATIONS_PRIMARY, VertxJsonUtils.JsonFieldType.Boolean);

    // Validators for the "locations>contacts" field
    public static final VertxJsonUtils.JsonFieldValidator CONTACTS_MANDATORY_FIELDS = null;

    public static final VertxJsonUtils.JsonFieldValidator CONTACTS_OPTIONAL_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(FIELD_NAME_LOCATIONS_CONTACTS_EMAIL, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_LOCATIONS_CONTACTS_PHONE, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_LOCATIONS_CONTACTS_FIRST_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_LOCATIONS_CONTACTS_LAST_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_LOCATIONS_CONTACTS_PRIMARY, VertxJsonUtils.JsonFieldType.Boolean);

    /**
     * Validate a Subscriber JSON Object.
     * @param subscriber
     * @throws VertxException
     */
    public static void validate(JsonObject subscriber) throws VertxException {
        // Validate the request by simply instantiate a new Subscriber POJO
        VertxJsonUtils.validateFields(subscriber, MANDATORY_FIELDS, OPTIONAL_FIELDS);

        JsonArray locations = subscriber.getJsonArray(Subscriber.FIELD_NAME_LOCATIONS);
        if (locations != null) {
            for (int i = 0; i < locations.size(); i ++) {
                JsonObject aLocation = locations.getJsonObject(i);
                VertxJsonUtils.validateFields(aLocation, LOCATIONS_MANDATORY_FIELDS, LOCATIONS_OPTIONAL_FIELDS);
            }
        }
    }

    /**
     * Get the app data for a specific application.
     * @param subscriber
     * @param appName
     * @return
     */
    public static JsonObject getAppDataObjectByAppName(JsonObject subscriber, String appName) {
        if (subscriber == null || appName == null) {
            return null;
        }

        JsonObject allAppData = subscriber.getJsonObject(FIELD_NAME_APP_DATA);
        if (allAppData == null) {
            return null;
        }

        return allAppData.getJsonObject(appName);
    }

    /**
     * Get a single App Data field.
     *
     * @param subscriber
     * @param appName
     * @param fieldName
     * @param <T>
     * @return
     */
    public static <T> T  getAppDataField(JsonObject subscriber, String appName, String fieldName) {
        JsonObject appData = getAppDataObjectByAppName(subscriber, appName);
        if (appData == null) {
            return null;
        }

        return (T)appData.getValue(fieldName);
    }

    /**
     * Types of the device id strings
     */
    public enum DeviceIdStringType{
        FSAN("FSAN"),
        REG_ID("Registration Id"),
        INTERNAL_ID("Device Id"),
        INVALID("Invalid Device Id");

        // Each Enum Value shall has a String attribute
        public String typeString;

        /**
         * Constructor which requires a type string.
         * @param typeString
         */
        private DeviceIdStringType (String typeString) {
            this.typeString = typeString;
        }

    }

    /**
     * Get the type of the given device id string.
     *
     * @param orgId
     * @param deviceId
     */
    public static DeviceIdStringType getDeviceIdStringType(String orgId, String deviceId) {
        String internalIdPrefix = orgId + "-" + Cpe.CALIX_OUI + "-";
        if (deviceId.startsWith(internalIdPrefix)) {
            if (AcsMiscUtils.isFSANString(deviceId.substring(internalIdPrefix.length()))) {
                // internal device id
                return DeviceIdStringType.INTERNAL_ID;
            } else {
                log.error("Malformed Internal Device id String " + deviceId + "!");
            }
        } else if (AcsMiscUtils.isFSANString(deviceId)) {
            // FSAN
            return DeviceIdStringType.FSAN;
        } else if (AcsMiscUtils.isRegIdString(deviceId)) {
            // Reg ID
            return DeviceIdStringType.REG_ID;
        }

        return DeviceIdStringType.INVALID;
    }

    /**
     * Get a matcher (for query the device collection) by orgId and deviceId
     * @param orgId
     * @param deviceId
     * @return
     */
    public static JsonObject getDeviceMatcherByDeviceId(String orgId, String deviceId) {
        /**
         * Try to find the device first
         */
        JsonObject matcher = new JsonObject();
        switch (getDeviceIdStringType(orgId, deviceId)) {
            case REG_ID:
                matcher.put(Cpe.DB_FIELD_NAME_REGISTRATION_ID, deviceId);
                break;
            case FSAN:
                matcher.put(Cpe.DB_FIELD_NAME_SN, deviceId);
                break;
            case INTERNAL_ID:
                matcher.put(AcsConstants.FIELD_NAME_ID, deviceId);
                break;
            default:
                log.error("found invalid device id " + deviceId + "!");;
                return null;
        }

        return matcher;
    }

    /**
     * Get a matcher (for query the subscriber collection) by CPE Device Data JSON Object
     *
     * @param deviceData
     */
    public static JsonObject getDeviceMatcherByDeviceData(JsonObject deviceData) {
        /**
         * Build an array that contains all the id strings that might be used to associated with subscriber
         */
        JsonArray deviceIdArray = new JsonArray()
                .add(deviceData.getString(AcsConstants.FIELD_NAME_ID))
                .add(deviceData.getString(Cpe.DB_FIELD_NAME_SN));
        String regId = deviceData.getString(Cpe.DB_FIELD_NAME_REGISTRATION_ID);
        if (regId != null && regId.length() > 1) {
            deviceIdArray.add(regId);
        }

        // element match on "locations.devices"
        return getDeviceMatcherByDeviceIdArray(deviceData.getString(AcsConstants.FIELD_NAME_ORG_ID), deviceIdArray);
    }

    /**
     * Get a matcher (for query the subscriber collection) by a Device ID Array
     *
     * @param deviceIdArray
     */
    public static JsonObject getDeviceMatcherByDeviceIdArray(String orgId, JsonArray deviceIdArray) {
        // element match on "locations.devices"
        return new JsonObject()
                .put(AcsConstants.FIELD_NAME_ORG_ID, orgId)
                .put(
                        Subscriber.FIELD_NAME_LOCATIONS,
                        new JsonObject().put(
                                VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_ELEM_MATCH,
                                new JsonObject().put(
                                        Subscriber.FIELD_NAME_LOCATIONS_DEVICES,
                                        new JsonObject().put(
                                                VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_IN,
                                                deviceIdArray
                                        )
                                )
                        )
                );
    }

    /**
     * Get all the device id strings linked to the given subscriber.
     *
     * @param subscriber
     * @return
     */
    public static JsonArray getAllDeviceIdStrings(JsonObject subscriber) {
        final JsonArray allDeviceIdStrings = new JsonArray();

        JsonArray locations = subscriber.getJsonArray(Subscriber.FIELD_NAME_LOCATIONS);
        if (locations != null) {
            for (int i = 0; i < locations.size(); i++) {
                JsonObject aLocation = locations.getJsonObject(i);

                // Add the associated devices to Array
                JsonArray deviceIdArray = aLocation.getJsonArray(
                        Subscriber.FIELD_NAME_LOCATIONS_DEVICES
                );
                if (deviceIdArray != null) {
                    for (int j = 0; j < deviceIdArray.size(); j++) {
                        allDeviceIdStrings.add(deviceIdArray.getValue(j));
                    }
                }
            }
        }

        return allDeviceIdStrings;
    }

    /**
     * Query the Subscriber Data.
     *
     * @param mongoClient
     * @param deviceData
     * @param handler
     */
    public static void querySubscriberData(
            MongoClient mongoClient,
            final JsonObject deviceData,
            Handler<JsonObject> handler) {
        try {
            VertxMongoUtils.findOne(
                    mongoClient,
                    Subscriber.DB_COLLECTION_NAME,
                    Subscriber.getDeviceMatcherByDeviceData(deviceData),
                    handler,
                    null
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }
}
