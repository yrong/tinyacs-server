package vertx2.model;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxMongoUtils;
import vertx2.CcException;
import vertx2.cwmp.CwmpMessageTypeEnum;
import vertx2.cwmp.CwmpNotificationValues;
import vertx2.util.AcsConstants;
import vertx2.util.AcsMiscUtils;
import vertx2.util.CpeDataModelMgmt;
import vertx2.util.GigaCenter;
import dslforumOrgCwmp12.*;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA-CC
 *
 * This class defines the CPE Device Objects.
 *
 * @author: ronyang
 */

public class Cpe extends MultiTenantObject{
    private static final Logger log = LoggerFactory.getLogger(Cpe.class.getName());

    /**
     * Calix OUI
     */
    public static final String CALIX_OUI = "000631";

    /**
     * Calix FSAN Prefix
     */
    public static final String CALIX_FSAN_PREFIX = "CXNK";

    /**
     * Deliminator used to separate orgId-OUI-SN when building CPE Key/ID
     */
    public static final String CPE_KEY_DELIMINATOR = "-";

    /**
     * Root Object Names
     */
    public static final String INTERNET_GATEWAY_DEVICE_ROOT = "InternetGatewayDevice";
    public static final String DEVICE_ROOT = "Device";

    /**
     * Field Name Constant(s)
     */
    public static final String DB_FIELD_NAME_ORG_ID = "orgId";
    public static final String DB_FIELD_NAME_INITIAL_PROVISIONING = "initialProvisioning";
    public static final String DB_FIELD_NAME_INITIAL_PROVISIONING_FORCE_APPLY = "bForceApply";
    public static final String DB_FIELD_NAME_PARAM_VALUES = "paramValues";
    public static final String DB_FIELD_NAME_PARAM_ATTRIBUTES = "paramAttributes";
    public static final String DB_FIELD_NAME_LAST_INFORM_TIME = "lastInformTime";
    public static final String DB_FIELD_NAME_PERIODIC_INFORM_INTERVAL = "periodicInformInterval";
    public static final String DB_FIELD_NAME_PERIODIC_INFORM_ENABLE = "periodicInformEnable";
    public static final String DB_FIELD_NAME_LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String DB_FIELD_NAME_CREATE_TIME = "createTime";
    public static final String DB_FIELD_NAME_WORKFLOW_EXEC = "workflows";
    public static final String DB_FIELD_NAME_SN = "serialNumber";
    public static final String DB_FIELD_NAME_ADDITIONAL_HW_VER = "additionalHardwareVersion";
    public static final String DB_FIELD_NAME_MAC_ADDRESS = "macAddress";
    public static final String DB_FIELD_NAME_CONNREQ_URL = "connectionRequestUrl";
    public static final String DB_FIELD_NAME_CONNREQ_USERNAME = "connectionRequestUsername";
    public static final String DB_FIELD_NAME_CONNREQ_PASSWORD = "connectionRequestPassword";
    public static final String DB_FIELD_NAME_IP_ADDRESS = "ipAddress";
    public static final String DB_FIELD_NAME_REGISTRATION_ID = "registrationId";
                                                    //InternetGatewayDevice.DeviceInfo.X_000631_RegistrationId
    public static final String DB_FIELD_NAME_CHANGE_COUNTER = "changeCounter";
    public static final String DB_FIELD_NAME_IP_SUBNET_MASK = "subnetMask";
    public static final String DB_FIELD_NAME_DEFAULT_GATEWAY = "defaultGateway";
    public static final String DB_FIELD_NAME_WAN_CONNECTION_PATH = "wanIpConnectionPath";
    public static final String DB_FIELD_NAME_LAST_DISCOVER_TIME = "lastDiscoverTime";
    public static final String DB_FIELD_NAME_PREV_SUBSCRIBER = "prevSubscriber";
    public static final String DB_FIELD_NAME_SERVICE_PLAN_STATUS = "servicePlanStatus";
    // Set when performing FSAN-based replacement
    public static final String DB_FIELD_NAME_TO_BE_REPLACED_BY = "toBeReplacedBy";
    public static final String DB_FIELD_NAME_DECOMMISSIONED = "bDecommissioned";

    /**
     * DB Collection for CPE Devices
     */
    public static final String CPE_COLLECTION_NAME = "sxacc-devices";

    /**
     * Periodic Inform Constants
     */
    public static final String PERIODIC_INFORM_INTERVAL_PARAM_PATH =
            "InternetGatewayDevice.ManagementServer.PeriodicInformInterval";
    public static final String PERIODIC_INFORM_ENABLE_PARAM_PATH =
            "InternetGatewayDevice.ManagementServer.PeriodicInformEnable";

    /**
     * Customized CPE id string which contains OrgId + OUI + SN
     */
    public String key;

    // Device Info which includes all fields from Device Type plus a serial #
    public DeviceId deviceId;

    // Data Model Root Object "Device" vs. "InternetGatewayDevice".
    public String rootObjectName = "InternetGatewayDevice";

    /**
     * Is discovery needed?
     */
    public boolean bNeedDiscovery = false;
    public boolean bNeedReDiscovery = false;
    public boolean bDiscoveryDone = false;
    public boolean bDecommissioned = false;

    /**
     * JSON Object for this CPE (received from mod-mongo-persistor)
     */
    public JsonObject cpeJsonObj;

    /**
     * JSON Objects for updating MongoDB
     */
    public JsonObject sets = null;
    public JsonObject unsets = null;
    public JsonObject pulls = null;
    public JsonObject pushes = null;
    public JsonObject timestamps = null;

    // Inform Time in ms
    public long informTime = System.currentTimeMillis();

    /**
     * Data Model
     */
    public CpeDeviceDataModel dataModel;


    /**
     * Last ParameterKey Sent to CPE.
     *
     * Quote from TR-069:
     * "
     * ParameterKey provides the ACS a reliable and extensible means to track changes made by the ACS. The value of
     * ParameterKey MUST be equal to the value of the ParameterKey argument from the most recent successful
     * SetParameterValues, AddObject, or DeleteObject method call from the ACS.
     * The CPE MUST set ParameterKey to the value specified in the corresponding method arguments if and only if the
     * method completes successfully and no fault response is generated. If a method call does not complete successfully
     * (implying that the changes requested in the method did not take effect), the value of ParameterKey MUST NOT be
     * modified.
     *
     * The CPE MUST only modify the value of ParameterKey as a result of SetParameterValues, AddObject, DeleteObject,
     * or due to a factory reset. On factory reset, the value of ParameterKey MUST be set to an empty string.
     */
    public String parameterKey;

    /**
     * TODO: Add CPE specific config/event parameters/objects/events here
     */

    /**
     * Default constructor that requires a JSON Object (could be the result of MongoDB query)
     *
     * @param jsonObject
     */
    public Cpe(JsonObject jsonObject) {
        // Save the JSON Object
        this.cpeJsonObj = jsonObject;

        // Extract DeviceId
        deviceId = new DeviceId(jsonObject);

        // Extract orgId
        orgId = jsonObject.getString(DB_FIELD_NAME_ORG_ID);

        // Is it decommissioned?
        bDecommissioned = jsonObject.getBoolean(DB_FIELD_NAME_DECOMMISSIONED, false);

        // CPE Key
        key = getCpeKey();

        // Lookup data model by Device Id
        dataModel = CpeDataModelMgmt.findDataModelByDeviceType(deviceId);;
        if (dataModel == null) {
            /**
             * No data model found for this CPE's device type
             */
            log.info("No data model found for CPE " + toString());
        }
    }

    /**
     * Constructor that requires all parameters in DeviceId struct (used when discovering a new CPE).
     *
     * @param orgId
     * @param manufacturer
     * @param oui
     * @param productClass
     * @param modelName
     * @param hwVersion
     * @param swVersion
     * @param sn
     */
    public Cpe(String orgId, String manufacturer, String oui, String productClass, String modelName,
               String hwVersion, String swVersion, String sn) {
        this.deviceId = new DeviceId(
                orgId, manufacturer, oui, productClass, modelName, hwVersion, swVersion, sn);
        this.orgId = orgId;
        this.key = getCpeKey();

        /**
         * Create MongoDB Object
         */
        this.cpeJsonObj = this.deviceId.toJsonObject();

        // Lookup data model by Device Id
        dataModel = CpeDataModelMgmt.findDataModelByDeviceType(deviceId);;
        if (dataModel == null) {
            /**
             * No data model found for this CPE's device type
             */
            log.info("No data model found for CPE " + toString());
        }
    }

    /**
     * Constructor by CWMP DeviceId Struct
     * @param orgId
     * @param deviceIdStruct
     */
    public Cpe(String orgId, dslforumOrgCwmp12.DeviceIdStruct  deviceIdStruct) {
        this(orgId,
                deviceIdStruct.getManufacturer(),
                deviceIdStruct.getOUI(),
                deviceIdStruct.getProductClass(),
                null,
                null,
                null,
                deviceIdStruct.getSerialNumber());
    }

    /**
     * * Get a Cpe object by Inform request.
     *
     * @param orgId
     * @param informRequest
     * @param eventBus
     * @param findHandler
     * @return
     * @throws CcException
     */
    public static void getCpeByInform(
            String orgId,
            InformDocument.Inform informRequest,
            EventBus eventBus,
            VertxMongoUtils.FindOneHandler findHandler
    ) throws SxaVertxException {
        if (informRequest.getDeviceId() == null) {
            informRequest.dump();
            throw new CcException("Inform request has no deviceId element: " + informRequest);
        }

        /**
         * Extract child elements out of the DeviceId element
         */
        String manufacturer = informRequest.getDeviceId().getManufacturer();
        String oui = informRequest.getDeviceId().getOUI();
        String sn = informRequest.getDeviceId().getSerialNumber();
        if (manufacturer == null || oui == null || sn == null) {
            informRequest.dump();
            throw new CcException("Inform request has invalid deviceId element: " + informRequest);
        }

        /**
         * Construct CPE Key
         */
        String key = getCpeKey(orgId, oui, sn);

        /**
         * Extract Root Object Name from the 1st parameter in the Parameter List if any
         */
        ParameterValueList parameterList = informRequest.getParameterList();
        if (parameterList != null) {
            ParameterValueStruct paramValue = parameterList.getParameterValueStructArray(0);
            String paramName = paramValue.getName();
        } else {
            log.error("This Inform Request has no ParameterList! for CPE: " + key);
        }

        /**
         * Lookup existing CPE from DB first
         */
        VertxMongoUtils.findOne(
                eventBus,
                CPE_COLLECTION_NAME,
                // Matcher which contains the CPE Key as the id
                new JsonObject().putString(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, key),
                // Async FindOne Result Handler
                findHandler,
                // Keys
                null
        );

        /**
         * TODO: Check ParameterKey
         */
    }

    /**
     * Device Info Inner Class that extends CPE Device Type by adding CPE SN
     */
    public static class DeviceId extends CpeDeviceType {
        // Serial #
        public String sn;
        public String additionalHwVersion = null;
        // Connection Request URL/Username/Password
        public String connRequestUrl = null;
        public String connRequestUsername = null;
        public String connRequestPassword = null;
        // External/Public IP Address
        public String ipAddress = null;
        public String ipSubnetMask = null;
        public String defaultGateway = null;

        // WAN IP Connection Path
        public String wanIpConnectionPath = null;
        // MAC Address (currently not used)
        public String macAddress = null;

        // Registration ID
        public String registrationId = "";

        // Periodic Inform Interval
        public boolean bPeriodicInformEnabled = false;
        public int informInterval = 0;

        // Change Counter
        public Integer changeCounter = null;

        /**
         * Constructor Method that requires everything.
         *
         * @param orgId
         * @param oui
         * @param manufacturer
         * @param productClass
         * @param modelName
         * @param hwVersion
         * @param swVersion
         * @param sn
         */
        public DeviceId(String orgId, String manufacturer, String oui, String productClass, String modelName,
                        String hwVersion, String swVersion, String sn) {
            super(orgId, manufacturer, oui, productClass, modelName, hwVersion, swVersion);
            this.sn = sn;
        }

        /**
         * Constructor Method that requires a JSON Object.
         *
         * @param jsonObject
         */
        public DeviceId(JsonObject jsonObject) {
            super(jsonObject);
            sn = jsonObject.getString(DB_FIELD_NAME_SN);
            registrationId = jsonObject.getString(DB_FIELD_NAME_REGISTRATION_ID, "");
            additionalHwVersion = jsonObject.getString(DB_FIELD_NAME_ADDITIONAL_HW_VER);
            ipAddress = jsonObject.getString(DB_FIELD_NAME_IP_ADDRESS);
            macAddress = jsonObject.getString(DB_FIELD_NAME_MAC_ADDRESS);
            wanIpConnectionPath = jsonObject.getString(DB_FIELD_NAME_WAN_CONNECTION_PATH);
            connRequestUrl = jsonObject.getString(DB_FIELD_NAME_CONNREQ_URL);
            connRequestUsername = jsonObject.getString(DB_FIELD_NAME_CONNREQ_USERNAME);
            connRequestPassword = jsonObject.getString(DB_FIELD_NAME_CONNREQ_PASSWORD);
            informInterval = jsonObject.getInteger(DB_FIELD_NAME_PERIODIC_INFORM_INTERVAL, 0);
            bPeriodicInformEnabled = jsonObject.getBoolean(DB_FIELD_NAME_PERIODIC_INFORM_ENABLE, informInterval > 0);
            changeCounter = jsonObject.getInteger(DB_FIELD_NAME_CHANGE_COUNTER);
        }

        /**
         * Get the JSON Object for this DeviceId
         */
        public JsonObject toJsonObject(){
            JsonObject jsonObject = super.toJsonObject()
                    .putString(DB_FIELD_NAME_SN, sn)
                    .putString(Cpe.DB_FIELD_NAME_REGISTRATION_ID, registrationId)
                    .putString(DB_FIELD_NAME_ADDITIONAL_HW_VER, additionalHwVersion)
                    .putString(DB_FIELD_NAME_IP_ADDRESS, ipAddress)
                    .putString(DB_FIELD_NAME_MAC_ADDRESS, macAddress)
                    .putString(DB_FIELD_NAME_WAN_CONNECTION_PATH, wanIpConnectionPath)
                    .putString(DB_FIELD_NAME_CONNREQ_URL, connRequestUrl)
                    .putString(DB_FIELD_NAME_CONNREQ_USERNAME, connRequestUsername)
                    .putString(DB_FIELD_NAME_CONNREQ_PASSWORD, connRequestPassword)
                    .putNumber(DB_FIELD_NAME_PERIODIC_INFORM_INTERVAL, informInterval)
                    .putBoolean(DB_FIELD_NAME_PERIODIC_INFORM_ENABLE, bPeriodicInformEnabled);

            if (changeCounter != null) {
                jsonObject.putNumber(DB_FIELD_NAME_CHANGE_COUNTER, changeCounter);
            }

            return jsonObject;
        }

        /**
         * Add all Device ID fields to the parent CPE JSON Object
         */
        public void addToJsonObject(JsonObject parentCpe){
            if (productClass == null || productClass.equals("(null)")) {
                // to work around an ONT issue where product class is reported as "(null)"
                productClass = modelName + "_SG";
            }

            parentCpe
                    .putString(FIELD_NAME_ORG_ID, orgId)
                    .putString(FIELD_NAME_MANUFACTURER, manufacturer)
                    .putString(FIELD_NAME_OUI, oui)
                    .putString(FIELD_NAME_PRODUCT_CLASS, productClass)
                    .putString(FIELD_NAME_MODEL_NAME, modelName)
                    .putString(FIELD_NAME_HW_VER, hwVersion)
                    .putString(FIELD_NAME_SW_VER, swVersion)
                    .putString(DB_FIELD_NAME_SN, sn)
                    .putString(DB_FIELD_NAME_REGISTRATION_ID, registrationId)
                    .putString(DB_FIELD_NAME_ADDITIONAL_HW_VER, additionalHwVersion)
                    .putString(DB_FIELD_NAME_IP_ADDRESS, ipAddress)
                    .putString(DB_FIELD_NAME_WAN_CONNECTION_PATH, wanIpConnectionPath)
                    .putString(DB_FIELD_NAME_CONNREQ_URL, connRequestUrl)
                    .putString(DB_FIELD_NAME_CONNREQ_USERNAME, connRequestUsername)
                    .putString(DB_FIELD_NAME_CONNREQ_PASSWORD, connRequestPassword)
                    .putNumber(DB_FIELD_NAME_PERIODIC_INFORM_INTERVAL, informInterval)
                    .putBoolean(DB_FIELD_NAME_PERIODIC_INFORM_ENABLE, bPeriodicInformEnabled);

            if (changeCounter != null) {
                parentCpe.putNumber(DB_FIELD_NAME_CHANGE_COUNTER, changeCounter);
            }
        }

        @Override
        public String toString(){
            return super.toString() + " sn:" + sn;
        }
    }

    @Override
    /**
     * Custom toString() method.
     */
    public String toString() {
        return deviceId.toString();
    }

    /**
     * Return an unique string key to identify a given CPE (non-static version).
     */
    public String getCpeKey() {
        return getCpeKey(orgId, deviceId.oui, deviceId.sn);
    }

    /**
     * Extract OrgId from CPE Key
     */
    public static String getOrgIdByCpeKey(String key) {
        return key.substring(0, key.indexOf(CPE_KEY_DELIMINATOR));
    }

    /**
     * Extract OrgId from CPE Key
     */
    public static String getOuiByCpeKey(String key) {
        return key.substring(key.indexOf(CPE_KEY_DELIMINATOR) + 1, key.lastIndexOf(CPE_KEY_DELIMINATOR));
    }

    /**
     * Extract CPE SN from CPE Key
     */
    public static String getSnByCpeKey(String key) {
        return key.substring(key.lastIndexOf(CPE_KEY_DELIMINATOR) + 1);
    }

    /**
     * Is the given string a valid CPE key?
     * @param key
     * @return
     */
    public static boolean isValidCpeKey(String key) {
        if (key == null) {
            return false;
        }

        String[] fields = StringUtil.split(key, '-');
        if (fields == null || fields.length != 3) {
            return false;
        }

        if (!fields[1].equals(Cpe.CALIX_OUI)) {
            return false;
        }

        if (!AcsMiscUtils.isFSANString(fields[2])) {
            return false;
        }

        return true;

    }

    /**
     * Get a CPE-Identifier JSON Object
     */
    public JsonObject getCpeIdentifier() {
        return new JsonObject()
                .putString(CpeIdentifier.FIELD_NAME_OUI, deviceId.oui)
                .putString(CpeIdentifier.FIELD_NAME_SN, deviceId.sn)
                .putString(CpeIdentifier.FIELD_NAME_MAC_ADDRESS, deviceId.macAddress);
    }

    /**
     * Get the JSON Object that is just enough to send a connection request
     */
    public JsonObject toBasicJsonObjectForConnReq(){
        return new JsonObject()
                .putString(AcsConstants.FIELD_NAME_ID, key)
                .putString(DB_FIELD_NAME_SN, deviceId.sn)
                .putString(DB_FIELD_NAME_CONNREQ_URL, deviceId.connRequestUrl)
                .putString(DB_FIELD_NAME_CONNREQ_USERNAME, deviceId.connRequestUsername)
                .putString(DB_FIELD_NAME_CONNREQ_PASSWORD, deviceId.connRequestPassword);
    }

    /**
     * Static Version of the above method:
     * Get the JSON Object that is just enough to send a connection request
     */
    public static JsonObject toBasicJsonObjectForConnReq(JsonObject cpeJsonObj){
        return new JsonObject()
                .putString(AcsConstants.FIELD_NAME_ID, cpeJsonObj.getString(AcsConstants.FIELD_NAME_CPE_ID))
                .putString(DB_FIELD_NAME_SN, cpeJsonObj.getString(DB_FIELD_NAME_SN))
                .putString(DB_FIELD_NAME_CONNREQ_URL, cpeJsonObj.getString(DB_FIELD_NAME_CONNREQ_URL))
                .putString(DB_FIELD_NAME_CONNREQ_USERNAME, cpeJsonObj.getString(DB_FIELD_NAME_CONNREQ_USERNAME))
                .putString(DB_FIELD_NAME_CONNREQ_PASSWORD, cpeJsonObj.getString(DB_FIELD_NAME_CONNREQ_PASSWORD));
    }

    /**
     * Return an unique string key to identify a given CPE (static version).
     *
     * @param orgId
     * @param oui
     * @param sn
     * @return
     */
    public static String getCpeKey(String orgId, String oui, String sn) {
        return orgId +CPE_KEY_DELIMINATOR + oui + CPE_KEY_DELIMINATOR + sn;
    }

    /**
     * A static frequently used query key to exclude all known root objects
     */
    private static final JsonObject queryKeyNoRootObjects = new JsonObject()
            .putNumber("Device", 0)
            .putNumber("InternetGatewayDevice", 0);

    /**
     * Persist/Save new CPE to MongoDB
     *
     * @param eventBus
     */
    public void saveNewCpeToDb(EventBus eventBus) {
        /**
         * Persist it
         */
        log.info("Saving New CPE " + cpeJsonObj.toString());
        try {
            VertxMongoUtils.save(eventBus, CPE_COLLECTION_NAME, cpeJsonObj, null);
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }

        /**
         * Add Creation Time
         */
        addTimeStamp(AcsConstants.FIELD_NAME_CREATE_TIME);
    }

    /**
     * Add a new field (to be persisted later).
     * @param fieldName
     * @param value
     */
    public void addSet(String fieldName, Object value) {
        sets = VertxMongoUtils.addSet(sets, fieldName, value);
    }

    /**
     * Add DeviceId fields to sets (to be persisted later).
     */
    public void addDeviceIdToSets() {
        if (sets == null) {
            sets = deviceId.toJsonObject();
        } else {
            deviceId.addToJsonObject(sets);
        }
    }

    /**
     * Check a parameterValue Struct to see if anything needs to be added to "sets"
     * @param valueStruct
     * @param messageType   "Get"/"Set"/"Inform"
     */
    public void addParamValueStructToSets(ParameterValueStruct valueStruct, CwmpMessageTypeEnum messageType) {
        // Only save to the "paramValues" field in MongoDB if it is set by us
        boolean bSaveRawValueToMongoDB = messageType.equals(CwmpMessageTypeEnum.SET_PARAMETER_VALUES);

        if (valueStruct != null && valueStruct.getValue() != null) {
            try {
                String paramName = valueStruct.getName();
                String paramValueString = "";
                if (valueStruct.getValue() != null) {
                    paramValueString = valueStruct.getValue().getStringValue();
                }

                // Check for NULL names/values
                if (paramName == null) {
                    log.error(getCpeKey() + ": " + messageType.typeString + ": Param name is null!"
                            + paramName + " - " + paramValueString);
                    return;
                }
                
                //log.debug(getCpeKey() + ": " + messageType + ":" +paramName + ":" + paramValueString);

                boolean bParamValueChanged = false;

                // Check if any deviceId field is updated
                if (paramName.equals("InternetGatewayDevice.DeviceInfo.HardwareVersion")) {
                    if (!bNeedDiscovery && !deviceId.hwVersion.equals(paramValueString)) {
                        log.info("CPE " + toString() + ": HW Version upgraded from " +
                                deviceId.hwVersion + " to " + paramValueString);
                        deviceId.hwVersion = paramValueString;
                        addSet(Cpe.DeviceId.FIELD_NAME_HW_VER, paramValueString);
                        bParamValueChanged = true;
                    }
                } else if (paramName.equals("InternetGatewayDevice.DeviceInfo.SoftwareVersion")) {
                    if (!bNeedDiscovery && !deviceId.swVersion.equals(paramValueString)) {
                        log.info("CPE " + toString() + ": SW Version upgraded from " +
                                deviceId.swVersion + " to " + paramValueString);
                        deviceId.swVersion = paramValueString;
                        addSet(Cpe.DeviceId.FIELD_NAME_SW_VER, paramValueString);
                        bParamValueChanged = true;
                    }
                    deviceId.swVersion = paramValueString ;
                } else if (paramName.equals("InternetGatewayDevice.DeviceInfo.X_000631_RegistrationId")) {
                    if (!deviceId.registrationId.equals(paramValueString)) {
                        log.info("CPE " + toString() + ": RegistrationId changed from " +
                                deviceId.registrationId + " to " + paramValueString);
                        deviceId.registrationId = paramValueString;
                        addSet(Cpe.DB_FIELD_NAME_REGISTRATION_ID, paramValueString);
                        bParamValueChanged = true;
                    }
                    deviceId.registrationId = paramValueString;
                } else if (paramName.equals("InternetGatewayDevice.ManagementServer.ParameterKey")) {
                    if (parameterKey != null &&
                            !parameterKey.equals(paramValueString )) {
                        log.error("Mismatch ParameterKey! ours: " + parameterKey +
                                ", but CPE reported " + paramValueString );
                        addSet(Cpe.DB_FIELD_NAME_PARAM_VALUES + "." + paramName, paramValueString);
                        bParamValueChanged = true;
                    }
                } else if (paramName.equals("InternetGatewayDevice.ManagementServer.ConnectionRequestURL")) {
                    if (!paramValueString.equals(deviceId.connRequestUrl)) {
                        deviceId.connRequestUrl = paramValueString;
                        addSet(Cpe.DB_FIELD_NAME_CONNREQ_URL, paramValueString);
                        bParamValueChanged = true;
                    }
                } else if (paramName.equals("InternetGatewayDevice.ManagementServer.ConnectionRequestUsername")) {
                    if (!paramValueString.equals(deviceId.connRequestUsername)) {
                        deviceId.connRequestUsername = paramValueString;
                        addSet(Cpe.DB_FIELD_NAME_CONNREQ_USERNAME, paramValueString);
                        bParamValueChanged = true;
                    }
                } else if (paramName.equals("InternetGatewayDevice.ManagementServer.ConnectionRequestPassword")) {
                    if (!paramValueString.equals(deviceId.connRequestPassword)) {
                        deviceId.connRequestPassword = paramValueString;
                        addSet(Cpe.DB_FIELD_NAME_CONNREQ_PASSWORD, paramValueString);
                        bParamValueChanged = true;
                    }
                } else if (paramName.equals("InternetGatewayDevice.ManagementServer.PeriodicInformInterval")) {
                    if (deviceId.informInterval != Integer.valueOf(paramValueString)) {
                        deviceId.informInterval = Integer.valueOf(paramValueString);
                        addSet(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_INTERVAL, deviceId.informInterval);
                        bParamValueChanged = true;
                    }
                } else if (paramName.equals("InternetGatewayDevice.ManagementServer.PeriodicInformEnable")) {
                    if (deviceId.bPeriodicInformEnabled != Boolean.valueOf(paramValueString)) {
                        deviceId.bPeriodicInformEnabled = Boolean.valueOf(paramValueString);
                        addSet(Cpe.DB_FIELD_NAME_PERIODIC_INFORM_ENABLE, deviceId.bPeriodicInformEnabled);
                        bParamValueChanged = true;
                    }
                } else if (messageType.equals(CwmpMessageTypeEnum.INFORM) && paramName.endsWith(".ExternalIPAddress")) {
                    if (!CwmpMessageTypeEnum.SET_PARAMETER_VALUES.equals(messageType)) {
                        // Check for device IP address change if reading from device via Get/Inform
                        String wanIpConnectionPath = paramName.substring(0, paramName.indexOf(".ExternalIPAddress"));
                        if (!wanIpConnectionPath.equals(deviceId.wanIpConnectionPath)) {
                            log.info(getCpeKey() + ": wanIpConnectionPath changed from " + deviceId.wanIpConnectionPath
                                    + " to " + wanIpConnectionPath);
                            deviceId.wanIpConnectionPath = wanIpConnectionPath;
                            addSet(Cpe.DB_FIELD_NAME_WAN_CONNECTION_PATH, wanIpConnectionPath);
                            bParamValueChanged = true;
                        }

                        if (!paramValueString.equals(deviceId.ipAddress)) {
                            log.info(getCpeKey() + ": wanIpConnectionPath changed from " + deviceId.ipAddress
                                    + " to " + paramValueString);
                            deviceId.ipAddress = paramValueString;
                            addSet(Cpe.DB_FIELD_NAME_IP_ADDRESS, paramValueString);
                            bParamValueChanged = true;
                        }
                    }
                } else if (paramName.equals(GigaCenter.CHANGE_COUNTER)) {
                    int newChangeCounterValue = Integer.valueOf(paramValueString);
                    log.info(key + ": ChangeCounter value :" + newChangeCounterValue);
                    if (deviceId.changeCounter == null || newChangeCounterValue != deviceId.changeCounter) {
                        log.info(key + ": ChangeCounter value changed from "
                                + deviceId.changeCounter + " to " + newChangeCounterValue);
                        deviceId.changeCounter = newChangeCounterValue;
                        addSet(Cpe.DB_FIELD_NAME_CHANGE_COUNTER, deviceId.changeCounter);
                        bParamValueChanged = true;
                    }
                } else if (paramName.equals("InternetGatewayDevice.DeviceSummary")) {
                    // do not save
                } else if (paramName.equals("InternetGatewayDevice.DeviceInfo.SpecVersion")) {
                    // do not save
                } else if (paramName.equals("InternetGatewayDevice.DeviceInfo.ProvisioningCode")) {
                    // do not save
                } else {
                    bParamValueChanged = true;
                }

                if (bSaveRawValueToMongoDB) {   // && bParamValueChanged) {
                    // Save values into the "paramValues" field only if set by us
                    addSet(Cpe.DB_FIELD_NAME_PARAM_VALUES + "." + paramName, paramValueString);
                }
            } catch (Exception ex) {
                log.error("Caught exception " + ex.getMessage() + " while processing value "
                        + valueStruct.getValue().toString());
                ex.printStackTrace();
            }
        }        
    }

    /**
     * Delete an existing field (to be persisted later).
     * @param fieldName
     */
    public void addUnSet(String fieldName) {
        unsets = VertxMongoUtils.addUnset(unsets, fieldName);
    }

    /**
     * Delete one or more element(s) from array (to be persisted later).
     * @param fieldName
     */
    public void addPull(String fieldName, Object value) {
        pulls = VertxMongoUtils.addPull(pulls, fieldName, value);
    }

    /**
     * Add a new element to array (to be persisted later).
     * @param fieldName
     */
    public void addPush(String fieldName, Object value) {
        pushes = VertxMongoUtils.addPush(pushes, fieldName, value);
    }

    /**
     * Add a new timestamp (to be persisted later).
     * @param fieldName
     */
    public void addTimeStamp(String fieldName) {
        //timestamps = VertxMongoUtils.addTimeStamp(timestamps, fieldName);
        addSet(fieldName, VertxMongoUtils.getDateObject());
    }

    /**
     * Persist changes of this CPE to MongoDB
     */
    public void updateDb(EventBus eventBus) {
        updateDb(eventBus, null);
    }

    /**
     * Persist changes of this CPE to MongoDB with a custom handler
     */
    private Long NON_NULL_LONG = new Long(1);
    public void updateDb(EventBus eventBus, Handler<Long> handler) {
        JsonObject updates = VertxMongoUtils.getUpdatesObject(sets, unsets, timestamps, pulls, pushes);

        //log.debug("Updates:\n" + updates.encodePrettily());

        if (updates != null && updates.size() > 0) {
            try {
                VertxMongoUtils.update(eventBus, CPE_COLLECTION_NAME, key, updates, handler);
            } catch (SxaVertxException e) {
                e.printStackTrace();
            }

            // Clear data so they won't be persisted again
            sets = null;
            unsets = null;
            timestamps = null;
            pulls = null;
            pushes = null;
        } else {
            log.debug("No updates to be persisted.");

            // Call the handler now
            if (handler != null) {
                handler.handle(NON_NULL_LONG);
            }
        }
    }

    /**
     * Delete all existing parameter values/attributes as well as workflow execution logs from DB
     */
    public static final JsonObject DELETE_ALL_PARAM_VALUES_AND_ATTRIBUTES =
            VertxMongoUtils.getUpdatesObject(
                    null,
                    VertxMongoUtils.addUnset(
                            VertxMongoUtils.addUnset(
                                    VertxMongoUtils.addUnset(null, DB_FIELD_NAME_PARAM_VALUES),
                                    DB_FIELD_NAME_PARAM_ATTRIBUTES
                            ),
                            DB_FIELD_NAME_WORKFLOW_EXEC
                    ),
                    null,
                    null,
                    null);
    public void deleteAllParamValuesAndAttributes(EventBus eventBus) {
        try {
            VertxMongoUtils.update(
                    eventBus,
                    CPE_COLLECTION_NAME,
                    key,
                    DELETE_ALL_PARAM_VALUES_AND_ATTRIBUTES,
                    null
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Store a list of ParameterAttributeStruct.
     *
     * Called when processing GetParameterAttributesResponse message.
     *
     * @param parameterAttributeList
     */
    public void storeParameterAttributeList(ParameterAttributeList parameterAttributeList) {
        for (ParameterAttributeStruct parameterAttributeStruct : parameterAttributeList.getParameterAttributeStructArray()) {
            String name = parameterAttributeStruct.getName();
            /**
             * Update parameter attribute fields
             */
            addSet(DB_FIELD_NAME_PARAM_ATTRIBUTES + "." + name, parameterAttributeStruct.getNotification());
        }
    }

    /**
     * Get parameter value by deep json path (separated via ".")
     *
     * For example,
     *
     *     "InternetGatewayDevice" : {
     *       "DeviceInfo" : {
     *          "Manufacturer" : "Calix"
     *          }
     *       }
     *    }
     *
     *   Path "InternetGatewayDevice.DeviceInfo.Manufacturer" shall return "Calix".
     *
     * @param path
     *
     * @return  The parameter value which is always a String.
     */
    public String getParamValue(String path) {
        // Split the parameter path into parent object name and the parameter name within the parent object
        String[] paths = splitParamPath(path);
        //log.info(paths[0] + ", " + paths[1]);

        // Get the parent object
        JsonObject parentObj = getParamValueObject(paths[0], false);
        if (parentObj == null || parentObj.getField(paths[1]) == null) {
            return null;
        } else {
            return parentObj.getField(paths[1]).toString();
        }
    }

    /**
     * Set value.
     *
     * If the target path does not exist, create it all the way.
     * @param path
     * @param value
     */
    public void setValue(String path, String value) {
        // Split the parameter path into parent object name and the parameter name within the parent object
        String[] paths = splitParamPath(path);

        // Get the parent object
        JsonObject parentObj = getParamValueObject(paths[0], true);
        parentObj.putString(paths[1], value);
    }

    /**
     * Get Notification Attribute.
     *
     * If the target path does not exist, return off.
     * @param path
     * @return
     */
    public Integer getNotifAttr(String path) {
        // Split the parameter path into parent object name and the parameter name within the parent object
        String[] paths = splitParamPath(path);

        // Get the parent object
        JsonObject parentObj = getParamAttrObject(paths[0], false);
        if (parentObj == null) {
            // Is this a valid parameter (that has value)?
            if (getParamValue(path) != null) {
                return CwmpNotificationValues.NOTIFICATION_OFF;
            } else {
                // no such parameter
                log.error("Trying to get notif attribute for a non-exist parameter ! (" + path + ")");
                return null;
            }
        } else {
            return parentObj.getNumber(paths[1]).intValue();
        }
    }

    /**
     * Set Notification Attribute.
     *
     * If the target path does not exist, create it all the way.
     * @param path
     * @param attr
     */
    public void setNotifAttr(String path, int attr) {
        // Split the parameter path into parent object name and the parameter name within the parent object
        String[] paths = splitParamPath(path);

        // Get the parent object
        JsonObject parentObj = getParamAttrObject(paths[0], true);
        parentObj.putNumber(paths[1], attr);
    }

    /**
     * Get Object by path.
     *
     * If bCreate is set to true, create new object all the way if needed.
     *
     * @param root
     * @param path
     * @param bCreate
     * @return
     */
    public JsonObject getObject(JsonObject root, String path, boolean bCreate) {
        if (root == null) {
            if (bCreate == false) {
                return null;
            } else {
                root = new JsonObject();
            }
        }

        String[] subPaths = StringUtil.split(path, '.');
        JsonObject nextJson = root;
        for (int i = 0; i < subPaths.length; i ++) {
            String subPath = subPaths[i];
            if (nextJson == null || nextJson.getObject(subPath) == null) {
                if (bCreate) {
                    nextJson.putObject(subPath, new JsonObject());
                } else {
                    return null;
                }
            }
            nextJson = nextJson.getObject(subPath);
        }
        return nextJson;
    }

    /**
     * Get Parameter Value Object by Partial TR Object Path.
     *
     * If bCreate is set to true, create new object all the way if needed.
     *
     * @param path
     * @param bCreate
     * @return
     */
    public JsonObject getParamValueObject(String path, boolean bCreate) {
        JsonObject values = cpeJsonObj.getObject(DB_FIELD_NAME_PARAM_VALUES);

        // Create a new "paramValues" JSON object if needed
        if (bCreate && values == null) {
            values = new JsonObject();
            cpeJsonObj.putObject(DB_FIELD_NAME_PARAM_VALUES, values);
        }

        return getObject(cpeJsonObj.getObject(DB_FIELD_NAME_PARAM_VALUES), path, bCreate);
    }

    /**
     * Get Parameter Attribute Object by Partial TR Object Path.
     *
     * If bCreate is set to true, create new object all the way if needed.
     *
     * @param path
     * @param bCreate
     * @return
     */
    public JsonObject getParamAttrObject(String path, boolean bCreate) {
        JsonObject attrs = cpeJsonObj.getObject(DB_FIELD_NAME_PARAM_ATTRIBUTES);

        // Create a new "paramAttributes" JSON object if needed
        if (bCreate && attrs == null) {
            attrs = new JsonObject();
            cpeJsonObj.putObject(DB_FIELD_NAME_PARAM_ATTRIBUTES, attrs);
        }

        return getObject(attrs, path, bCreate);
    }

    /**
     * Split a parameter name into parent object name (end with a '.') and the parameter name within the parent object.
     *
     * For example, given "InternetGatewayDevice.DeviceInfo.Manufacturer", the method returns a String array with 2
     * element: first element is "InternetGatewayDevice.DeviceInfo.", while the second is "Manufacturer".
     *
     * @param paramName
     * @return  A String Array with 2 element.
     */
    public String[] splitParamPath(String paramName) {
        String tmpParamName = paramName;
        int lastDotIndex = paramName.lastIndexOf('.');
        if (lastDotIndex == (paramName.length()-1)) {
            // partial path that ends with '.'
            tmpParamName = paramName.substring(0, lastDotIndex);
            lastDotIndex = tmpParamName.lastIndexOf('.');
        }

        if (lastDotIndex > 0) {
            String[] result = new String[2];
            result[0] = tmpParamName.substring(0, lastDotIndex);
            result[1] = tmpParamName.substring(lastDotIndex + 1, tmpParamName.length());
            return result;
        } else {
            log.error("Invalid parameter name " + paramName + "!");
            return null;
        }
    }
}