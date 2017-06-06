package vertx.model;

import io.vertx.ext.mongo.MongoClient;
import vertx.VertxException;
import vertx.VertxMongoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  cwmp ACS
 *
 * This class defines the CPE Device Type Objects, as well as some MongoDB CRUD operation Utils.
 *
 * @author: ronyang
 */
public class CpeDeviceType extends MultiTenantObject {
    private static final Logger log = LoggerFactory.getLogger(CpeDeviceType.class.getName());

    /**
     * Field Name Constant(s)
     */
    public static final String FIELD_NAME_MANUFACTURER = "manufacturer";
    public static final String FIELD_NAME_OUI = "manufacturerOUI";
    public static final String FIELD_NAME_PRODUCT_CLASS = "productClass";
    public static final String FIELD_NAME_MODEL_NAME = "modelName";
    public static final String FIELD_NAME_HW_VER = "hardwareVersion";
    public static final String FIELD_NAME_SW_VER = "softwareVersion";
    public static final String FIELD_NAME_CPE_COUNT = "cpeCount";

    /**
     * All GPON Models
     */
    public static final String[] ALL_GPON_MODELS = {
            "844G-1",
            "844G-2",
            "854G-1",
            "854G-2"
    };

    /**
     * All E Models
     */
    public static final String[] ALL_E_MODELS = {
            "844E-1",
            "844E-2"
    };

    /**
     * All attributes are index fields.
     */
    public static final String[] INDEX_FIELDS = {
            FIELD_NAME_ORG_ID,
            FIELD_NAME_MANUFACTURER,
            FIELD_NAME_OUI,
            FIELD_NAME_PRODUCT_CLASS,
            FIELD_NAME_MODEL_NAME,
            FIELD_NAME_HW_VER,
            FIELD_NAME_SW_VER
    };

    /**
     * Device Types are not subject to edit
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{}};


    /**
     * Basic CPE Device Type info
     */
    public String manufacturer;
    public String oui;
    public String productClass;

    /**
     * ModelName/HwVersion/SwVersion are required, but may not be available sometime
     */
    public String modelName = "";
    public String hwVersion = "";
    public String swVersion = "";

    /**
     * Default constructor
     */
    public CpeDeviceType() {

    }

    /**
     * Constructor that requires more parameters
     */
    public CpeDeviceType(String orgId, String manufacturer, String oui, String productClass, String modelName,
                         String hwVersion, String swVersion) {
        this.orgId = orgId;
        this.manufacturer = manufacturer;
        this.oui = oui;
        this.productClass = productClass;
        this.modelName = modelName;
        this.hwVersion = hwVersion;
        this.swVersion = swVersion;
    }

    /**
     * Constructor Method that requires a JSON Object.
     *
     * @param jsonObject
     */
    public CpeDeviceType(JsonObject jsonObject) {
        orgId = jsonObject.getString(FIELD_NAME_ORG_ID);
        manufacturer = jsonObject.getString(FIELD_NAME_MANUFACTURER);
        oui = jsonObject.getString(FIELD_NAME_OUI);
        productClass = jsonObject.getString(FIELD_NAME_PRODUCT_CLASS);
        modelName = jsonObject.getString(FIELD_NAME_MODEL_NAME);
        hwVersion = jsonObject.getString(FIELD_NAME_HW_VER);
        swVersion = jsonObject.getString(FIELD_NAME_SW_VER);
    }

    /**
     * Get the JSON Object for this Device Type
     */
    public JsonObject toDeviceTypeObject(){
        JsonObject deviceTypeJsonObject = new JsonObject()
                .put(FIELD_NAME_ORG_ID, orgId)
                .put(FIELD_NAME_MANUFACTURER, manufacturer)
                .put(FIELD_NAME_OUI, oui)
                .put(FIELD_NAME_PRODUCT_CLASS, productClass)
                .put(FIELD_NAME_MODEL_NAME, modelName)
                .put(FIELD_NAME_HW_VER, hwVersion)
                .put(FIELD_NAME_SW_VER, swVersion);

        return deviceTypeJsonObject;
    }

    /**
     * Get the JSON Object for this Device Type
     */
    public JsonObject toJsonObject(){
        JsonObject jsonObject = toDeviceTypeObject();
        jsonObject.remove(FIELD_NAME_ORG_ID);
        return jsonObject;
    }

    /**
     * Compare with another device type, and return true if they match, or false if they don't match.
     *
     * @param anotherDeviceType
     * @return
     */
    public boolean isSame(CpeDeviceType anotherDeviceType) {
        if (this.orgId == anotherDeviceType.orgId &&
                this.manufacturer.equals(anotherDeviceType.manufacturer) &&
                this.oui.equals(anotherDeviceType.oui) &&
                (this.modelName != null && this.modelName.equals(anotherDeviceType.modelName)) &&
                (this.hwVersion != null && this.hwVersion.equals(anotherDeviceType.hwVersion)) &&
                (this.swVersion != null && this.swVersion.equals(anotherDeviceType.swVersion))) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Compare with another device type, and return true if this one is a parent of or is the same as the other one.
     *
     * @param anotherDeviceType
     * @return
     */
    public boolean isParent(CpeDeviceType anotherDeviceType) {
        if (this.orgId != null && !this.orgId.equals(anotherDeviceType.orgId)) {
            return false;
        }
        if (this.manufacturer != null && !this.manufacturer.equals(anotherDeviceType.manufacturer)) {
            return false;
        }
        if (this.oui != null && !this.oui.equals(anotherDeviceType.oui)) {
            return false;
        }
        if (this.modelName != null && !this.modelName.equals(anotherDeviceType.modelName)) {
            return false;
        }
        if (this.hwVersion != null && !this.hwVersion.equals(anotherDeviceType.hwVersion)) {
            return false;
        }
        if (this.swVersion != null && !this.swVersion.equals(anotherDeviceType.swVersion)) {
            return false;
        }

        return true;
    }

    @Override
    /**
     * Custom toString() method.
     */
    public String toString() {
        return super.toString() + manufacturer + " (" + oui + ") " + productClass + " " + modelName +
                " hwVer:" + hwVersion + " swVer:" + swVersion;
    }

    /**
     * Name of the MongoDB Collection
     */
    public static final String DB_COLLECTION_NAME = "CWMP-device-types";

    /**
     * Save the Device Type if new.
     *
     * If yes, save it as a new record.
     *
     * If not, increase the CPE count.
     *
     * @param mongoClient
     */
    public static void addIfNew(MongoClient mongoClient, final JsonObject deviceTypeJsonObject) {
        try {
            VertxMongoUtils.count(
                    mongoClient,
                    DB_COLLECTION_NAME,
                    deviceTypeJsonObject,
                    new Handler<Long>() {
                        @Override
                        public void handle(Long count) {
                            if (count != null && count > 0) {
                                log.info("Device Type " + toString() + " is already known to the system.");

                            } else {
                                log.info("Device Type " + toString() + " is new to the system, saving to DB...");
                                try {
                                    VertxMongoUtils.save(
                                            mongoClient,
                                            DB_COLLECTION_NAME,
                                            deviceTypeJsonObject,
                                            null
                                    );
                                } catch (VertxException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    });
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

}
