package vertx2.cpeserver.deviceop;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.VertxMongoUtils;
import vertx2.cpeserver.session.CwmpRequest;
import vertx2.cpeserver.session.CwmpSession;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import vertx2.cwmp.CwmpUtils;
import vertx2.model.*;
import vertx2.util.AcsConstants;
import dslforumOrgCwmp12.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;

/**
 * Project:  SXA-CC (aka CCFG)
 *
 * Apply WAN Service Profile.
 *
 * See http://wiki.calix.local/display/Compass/Service+Profiles for more details.
 *
 * @author: ronyang
 */
public class WanService {
    private static final Logger log = LoggerFactory.getLogger(WanService.class.getName());

    /**
     * Static Errors/Warnings
     */
    public static final JsonObject DB_TIMED_OUT =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, "Internal Error (DB Timed Out)!");
    public static final JsonObject NO_SERVICE_PLAN =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, "No Service Plan Assigned to Device!");
    public static final JsonObject INVALID_PROFILE =
            new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, "Invalid Service Profile!");
    public static final JsonObject INTERNAL_ERROR_NO_KEY_FOR_THE_NEW_WAN_CONN =
            new JsonObject().putString(
                    AcsConstants.FIELD_NAME_ERROR,
                    "Internal Error! (unable to get the Interface Key for the newly created WAN Connection)"
            );
    public static final JsonObject INTERNAL_ERROR_UNEXPECTED_DEVICE_RESPONSE =
            new JsonObject().putString(
                    AcsConstants.FIELD_NAME_ERROR,
                    "Internal Error! (unexpected response from device)"
            );

    /**
     * Parameter Names
     */
    public static final String PARAM_NAME_VLAN_ID = "X_000631_VlanMuxID";
    public static final String PARAM_NAME_VLAN_PRIORITY = "X_000631_VlanMux8021p";
    public static final String PARAM_NAME_IGMP_PROXY = "X_000631_IGMPProxy";
    public static final String PARAM_NAME_SERVICE_CONNECTION_TYPE = "ServiceConnectionType";
    public static final String PARAM_NAME_BRIDGE_INTERFACE_ARRAY = "BridgedInterface";
    public static final String PARAM_NAME_BRIDGE_NAME = "BridgeName";
    public static final String PARAM_NAME_BRIDGE_KEY = "BridgeKey";
    public static final String PARAM_NAME_BRIDGE_ENABLE = "BridgeEnable";
    public static final String PARAM_NAME_BRIDGE_VLAN_ID = "VLANID";
    public static final String PARAM_NAME_INTERFACE_REFERENCE = "InterfaceReference";
    public static final String PARAM_NAME_AVAILABLE_INTERFACE_KEY = "AvailableInterfaceKey";

    /**
     * Service Connection Type String Constants
     */
    public static final String SERVICE_CONNECTION_TYPE_DHCP = "DHCP";
    public static final String SERVICE_CONNECTION_TYPE_BRIDGED = "Bridged";
    /**
     * L2 Bridge Table Prefix
     */
    public static final String L2_BRIDGE_PREFIX = "InternetGatewayDevice.Layer2Bridging.";
    public static final String L2_BRIDGES_PREFIX = L2_BRIDGE_PREFIX + "Bridge.";
    public static final String L2_SSID_BRIDGE_NAME = "L2SSID";
    public static final String L2_AVAILABLE_INTERFACE = "AvailableInterface.";
    public static final String L2_INTERFACES_PREFIX = L2_BRIDGE_PREFIX + L2_AVAILABLE_INTERFACE;
    public static final String L2_BRIDGE_FILTER_PREFIX = "InternetGatewayDevice.Layer2Bridging.Filter.";
    public static final JsonObject L2SSID_BRIDGE_ENABLE_PARAMS = new JsonObject()
            .putString(PARAM_NAME_BRIDGE_NAME, L2_SSID_BRIDGE_NAME)
            .putBoolean(PARAM_NAME_BRIDGE_ENABLE, true)
            .putNumber(PARAM_NAME_BRIDGE_VLAN_ID, 0);

    /**
     * Hardcoded L2 Interface Indexes
     */
    public static final String INTERFACE_INDEX_LAN_ETH_PORT_1 = L2_AVAILABLE_INTERFACE + "1";
    public static final String INTERFACE_INDEX_LAN_ETH_PORT_2 = L2_AVAILABLE_INTERFACE + "2";
    public static final String INTERFACE_INDEX_LAN_ETH_PORT_3 = L2_AVAILABLE_INTERFACE + "3";
    public static final String INTERFACE_INDEX_LAN_ETH_PORT_4 = L2_AVAILABLE_INTERFACE + "4";
    public static final String INTERFACE_INDEX_IPTV_SSID = L2_AVAILABLE_INTERFACE + "15";

    /**
     * WLAN Prefix and Parameter Names
     */
    public static final String WLAN_CONFIGURATION_PREFIX = "InternetGatewayDevice.LANDevice.1.WLANConfiguration.";
    public static final String IPTV_SSID_PREFIX = WLAN_CONFIGURATION_PREFIX + "11.";
    public static final String IPTV_SSID_PREFIX_WITHOUT_TRAILING_DOT = WLAN_CONFIGURATION_PREFIX + "11";
    public static final String TWO_POINT_4_GHZ_RADIO_INDEX = "1";
    public static final String FIVE_GHZ_RADIO_INDEX = "9";
    public static final String PARAM_SSID_L2_BRIDGE_ENABLE = "X_000631_L2_Bridge_Enable";
    public static final String PARAM_RADIO_ENABLED = "RadioEnabled";

    public static final JsonObject SSID_L2_BRIDGE_ENABLE = new JsonObject()
            .putBoolean("Enable", true)
            .putBoolean(PARAM_SSID_L2_BRIDGE_ENABLE, true);
    public static final JsonObject L2SSID_BRIDGE_DISABLE = new JsonObject().putBoolean(PARAM_NAME_BRIDGE_ENABLE, false);
    public static final String TWO_POINT_4_GHZ_RADIO_PREFIX = WLAN_CONFIGURATION_PREFIX + TWO_POINT_4_GHZ_RADIO_INDEX + ".";
    public static final String FIVE_GHZ_RADIO_PREFIX = WLAN_CONFIGURATION_PREFIX + FIVE_GHZ_RADIO_INDEX + ".";
    public static final JsonObject RADIO_ENABLE = new JsonObject().putBoolean("RadioEnabled", true);
    public static final JsonObject FILTER_ENABLE = new JsonObject().putBoolean("FilterEnable", true);
    public static final JsonObject FILTER_DISABLE = new JsonObject()
            .putBoolean("FilterEnable", false)
            .putString("FilterBridgeReference", "-1");


    /**
     * Voice Service Constants
     */
    public static final String VOICE_SERVICE_PREFIX = "InternetGatewayDevice.Services.VoiceService.1.";
    public static final String VOICE_SERVICE_PROFILE_PREFIX = VOICE_SERVICE_PREFIX + "VoiceProfile.1.";
    public static final String VOICE_SERVICE_PHY_INTERFACE_PREFIX = VOICE_SERVICE_PREFIX + "PhyInterface.";
    public static final String VOICE_CODEC_CAPABILITIES_PREFIX = VOICE_SERVICE_PREFIX + "Capabilities.Codecs.";
    public static final String VOICE_SERVICE_WAN_IP_CONNECTION_PATH = "VoiceProfile.1.X_000631_WANIPConnectionInstance";

    /**
     * Voice Line Enabled
     */
    public static final String VOICE_LINE_ENABLED = "Enabled";

    /**
     * Start the procedure that processes one or more services.
     *
     * @param session
     * @param parentDeviceOp
     */
    public static void start(JsonObject parentDeviceOp, CwmpSession session) {
        /**
         * Create a new WanService POJO for each item in the "services" array
         */
        JsonArray serviceJsonObjects = parentDeviceOp.getArray(CpeDeviceOp.FIELD_NAME_SERVICES);
        WanService[] wanServices = new WanService[serviceJsonObjects.size()];
        for (int i = 0; i < wanServices.length; i++) {
            wanServices[i] = new WanService(parentDeviceOp, session, (JsonObject) serviceJsonObjects.get(i));
        }

        /**
         * Chain them together
         */
        for (int i = 0; i < (wanServices.length - 1); i++) {
            wanServices[i].nextService = wanServices[i + 1];
        }

        /**
         * Kick off the first one
         */
        wanServices[0].start();
    }

    /**
     * State Machine States
     */
    public enum FsmStateEnum {
        Null,
        QueryingServicePlan,
        ReadingAllParameterValues,
        DeletingUnusedObjects,
        AddingNewL2Bridge,
        ReadingNewL2BridgeKey,
        AddingNewVlan,
        ConfiguringVlan,
        EnablingIPTVL2Bridge,
        ReadingNewWanInterfaceKey,
        SetAllParamValues,
        DeletingExistingVideoVlan,
        DisablingExistingL2SSIDBridge,
        DeletingNewlyCreatedL2Bridge,
        DeletingNewlyCreatedVlan,
        Done
    }

    /**
     * POJO Attributes
     */
    public JsonObject parentDeviceOp;
    public WanService nextService;
    public CwmpSession session;
    public String wanIpConnectionPrefix;
    // Subscriber Service Plan
    public JsonObject subscriberServicePlan;
    public String serviceName;

    // Service Profile
    public JsonObject serviceProfile;
    public String serviceConnType = null;
    public Integer vlanId = null;
    public boolean bBridged = false;
    public boolean bVoiceService = false;
    JsonArray bridgedInterfaces = null;

    // Existing VLAN Paths
    String existingVlanPath = null;
    JsonObject existingVlanObject = null;
    boolean bReuseDataVlan = false;

    // Newly Created Object Paths
    String newVlanPath = null;
    String newBridgePath = null;

    // Existing Bridge Path
    String existingL2SSIDBridgePath = null;
    String layer2SsidBridgeKey = null;

    // For Bridged Only:
    JsonArray includedFilters = null;
    JsonArray excludedFilters = null;
    ArrayList unUsedObjects = null;

    // For Voice only:
    String existingVoiceBoundIf = null;
    JsonObject codecCapabilities = null;
    JsonObject allCodecs = null;

    /**
     * XxxResponse Handlers
     */
    GetParameterValuesResponseHandler getParameterValuesResponseHandler = null;
    AddObjectResponseHandler addObjectResponseHandler = null;
    SetParameterValuesResponseHandler setParameterValuesResponseHandler = null;
    DeleteObjectResponseHandler deleteObjectResponseHandler = null;

    /**
     * FSM State
     */
    public FsmStateEnum state = FsmStateEnum.Null;

    /**
     * Constructor
     */
    public WanService(final JsonObject parentDeviceOp, final CwmpSession session, JsonObject serviceProfile) {
        this.parentDeviceOp = parentDeviceOp;
        this.session = session;
        this.serviceProfile = serviceProfile;
        this.subscriberServicePlan = parentDeviceOp.getObject(CpeDeviceOp.FIELD_NAME_SERVICE_PLAN);
    }

    /**
     * Start the procedure
     */
    public void start() {
        wanIpConnectionPrefix = SxaCcTr098ModelExtensions.convertSxaCcAbstractNameToActualName(
                session.cpe,
                SxaCcTr098ModelExtensions.WAN_CONNECTION_DEVICE + ".WANIPConnection."
        );

        // Check if service plan was already saved in this session
        if (subscriberServicePlan == null) {
            subscriberServicePlan = session.subscriberServicePlan;
        }

        /**
         * Parse the Service Profile
         */
        serviceName = serviceProfile.getString(ConfigurationCategory.PARAM_NAME_SERVICE_NAME);
        if (serviceName == null) {
            log.error("Invalid Service (no name found)! " + serviceProfile);
            failed(CpeDeviceOp.CPE_DEVICE_OP_STATE_INVALID_REQUEST, INVALID_PROFILE);
            return;
        }
        switch (serviceName) {
            case ConfigurationCategory.VIDEO_SERVICE:
                vlanId = serviceProfile.getInteger(PARAM_NAME_VLAN_ID);
                if (SERVICE_CONNECTION_TYPE_BRIDGED.equals(
                        serviceProfile.getString(PARAM_NAME_SERVICE_CONNECTION_TYPE))) {
                    bBridged = true;
                    includedFilters = new JsonArray();
                    bridgedInterfaces = serviceProfile.getArray(PARAM_NAME_BRIDGE_INTERFACE_ARRAY);
                }
                break;

            case ConfigurationCategory.VOICE_SERVICE:
                vlanId = serviceProfile.getInteger(PARAM_NAME_VLAN_ID);
                bVoiceService = true;
                break;

            default:
                log.error("Found invalid Service! " + serviceProfile);
                DeviceOpUtils.callbackInvalidReq(
                        session,
                        parentDeviceOp,
                        "Invalid Service Name " + serviceName
                );
                return;
        }

        log.debug("Start applying " + serviceName + ", VLAN id: " + vlanId);
        if (vlanId == null) {
            log.error("No valid service defined in profile!");
            failed(CpeDeviceOp.CPE_DEVICE_OP_STATE_INVALID_REQUEST, INVALID_PROFILE);
            return;
        }

        // Determine and save the service connection type
        switch (serviceProfile.getString(PARAM_NAME_SERVICE_CONNECTION_TYPE)) {
            case SERVICE_CONNECTION_TYPE_DHCP:
                serviceConnType = "IP_Routed";
                break;

            case SERVICE_CONNECTION_TYPE_BRIDGED:
            default:
                serviceConnType = "IP_Bridged";
                break;
        }

        if (subscriberServicePlan == null) {
            /**
             * Query the service-plan associated with the device
             */
            state = FsmStateEnum.QueryingServicePlan;
            // Increase the session timer to allow long DB query
            session.startInactiveTimer(VertxMongoUtils.DEFAULT_TIMEOUT);
            try {
                VertxMongoUtils.findOne(
                        session.eventBus,
                        ServicePlan.DB_COLLECTION_NAME,
                        ServicePlan.buildDeviceIdMatcher(session.cpe),
                        new VertxMongoUtils.FindOneHandler(
                                new Handler<JsonObject>() {
                                    @Override
                                    public void handle(JsonObject queryResult) {
                                        if (queryResult == null) {
                                            log.error(session.cpeKey + " has no service plan assigned!");
                                            failed(CpeDeviceOp.CPE_DEVICE_OP_STATE_INVALID_REQUEST, NO_SERVICE_PLAN);
                                        } else if (queryResult.equals(VertxMongoUtils.FIND_ONE_TIMED_OUT)) {
                                            DeviceOpUtils.callbackInternalError(
                                                    session,
                                                    parentDeviceOp,
                                                    DB_TIMED_OUT
                                            );
                                        } else {
                                            subscriberServicePlan = queryResult;
                                            log.debug(session.cpeKey + ": Found service plan: " + subscriberServicePlan);
                                            if (nextService != null) {
                                                // Save it for the next service
                                                log.debug(session.cpeKey + ": Saving service plan for the next service.");
                                                session.subscriberServicePlan = subscriberServicePlan;
                                            }

                                            if (session.serviceParameterValues == null) {
                                                readAllParameterValues();
                                            } else {
                                                state = FsmStateEnum.Null;
                                                processAllParameterValues(session.serviceParameterValues);
                                            }
                                        }
                                    }
                                }
                        ),
                        null
                );
            } catch (SxaVertxException e) {
                log.error(e.getMessage());
            }
        } else {
            /**
             * The service plan has already been queried when processing the previous service
             */
            if (session.serviceParameterValues == null) {
                readAllParameterValues();
            } else {
                processAllParameterValues(session.serviceParameterValues);
            }
        }
    }

    /**
     * Read all VLAN/L2Bridge Parameters
     */
    public void readAllParameterValues() {
        /**
         * Read all WAN Connection Objects and L2 Bridges/AvailableInterfaces
         */
        state = FsmStateEnum.ReadingAllParameterValues;
        ParameterNames paramNames = ParameterNames.Factory.newInstance();
        paramNames.addString(wanIpConnectionPrefix);
        paramNames.addString(L2_BRIDGE_PREFIX);
        paramNames.addString(VOICE_CODEC_CAPABILITIES_PREFIX);
        paramNames.addString(VOICE_SERVICE_PREFIX + VOICE_SERVICE_WAN_IP_CONNECTION_PATH);
        // Parameters required to add IPTV SSID into L2 Bridge
        paramNames.addString(IPTV_SSID_PREFIX + PARAM_SSID_L2_BRIDGE_ENABLE);
        paramNames.addString(FIVE_GHZ_RADIO_PREFIX + PARAM_RADIO_ENABLED);
        state = FsmStateEnum.ReadingAllParameterValues;
        GetParameterValues.start(session,
                paramNames,
                new GetParameterValuesResponseHandler(),
                CwmpRequest.CWMP_REQUESTER_ACS
        );
    }

    /**
     * Get an instance of GetParameterValuesResponseHandler.
     */
    public GetParameterValuesResponseHandler getGetParameterValuesResponseHandler() {
        if (getParameterValuesResponseHandler == null) {
            getParameterValuesResponseHandler = new GetParameterValuesResponseHandler();
        }

        return getParameterValuesResponseHandler;
    }

    /**
     * Custom GetParameterValuesResponse Handler
     */
    public class GetParameterValuesResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        /**
         * Constructor
         */
        public GetParameterValuesResponseHandler() {
            deviceOp = parentDeviceOp;
        }

        /**
         * Abstract Response Handler
         */
        @Override
        public void responseHandler(final CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            FsmStateEnum oldState = state;

            /**
             * Extract the response payload from SOAP message
             */
            GetParameterValuesResponseDocument.GetParameterValuesResponse response =
                    responseMessage.soapEnv.getBody().getGetParameterValuesResponse();
            ParameterValueStruct[] parameterValueStructs = response.getParameterList().getParameterValueStructArray();
            JsonObject paramValues = GetParameterValuesNbi.parameterValueStructsToJsonObject(parameterValueStructs);

            switch (state) {
                case ReadingAllParameterValues:
                    if (nextService != null) {
                        // Save it for the next service
                        session.serviceParameterValues = paramValues;
                    }

                    processAllParameterValues(paramValues);
                    break;

                case ReadingNewL2BridgeKey:
                    // Reading new L2 BridgeKey
                    layer2SsidBridgeKey = parameterValueStructs[0].getValue().getStringValue();
                    log.debug("New Bridge Key: " + layer2SsidBridgeKey);

                    if (existingVlanObject == null) {
                        /**
                         * Read Bridge Interface Key for the newly created VLAN
                         */
                        state = FsmStateEnum.ReadingNewWanInterfaceKey;
                        ParameterNames paramNames = ParameterNames.Factory.newInstance();
                        paramNames.addString(L2_INTERFACES_PREFIX);
                        paramNames.addString(L2_BRIDGE_FILTER_PREFIX);
                        GetParameterValues.start(session,
                                paramNames,
                                new GetParameterValuesResponseHandler(),
                                CwmpRequest.CWMP_REQUESTER_ACS
                        );
                    } else {
                        // Configure all objects
                        state = FsmStateEnum.SetAllParamValues;
                        SetParameterValues.start(
                                session,
                                buildSetParameterValuesList(),
                                new SetParameterValuesResponseHandler(),
                                CwmpRequest.CWMP_REQUESTER_ACS
                        );
                    }
                    break;

                case ReadingNewWanInterfaceKey:
                    String newWanInterfaceFilterIndex = null;
                    String newWanInterfaceKey = null;
                    log.debug("Looking for " + newVlanPath);

                    JsonObject allFilters = VertxJsonUtils.deepGet(paramValues, L2_BRIDGE_FILTER_PREFIX);
                    JsonObject allAvailableInterfaces = VertxJsonUtils.deepGet(paramValues, L2_INTERFACES_PREFIX);
                    if (allFilters == null || allAvailableInterfaces == null) {
                        log.error(session.cpeKey + ": Invalid L2 Bridging Filters from Device!" + paramValues);
                        failed(
                                CpeDeviceOp.CPE_DEVICE_OP_STATE_INTERNAL_ERROR,
                                INTERNAL_ERROR_UNEXPECTED_DEVICE_RESPONSE
                        );
                        return;
                    }

                    /**
                     * Analyze all available interfaces
                     */
                    for (String interfaceIndex : allAvailableInterfaces.getFieldNames()) {
                        JsonObject anInterface = allAvailableInterfaces.getObject(interfaceIndex);
                        String interfaceReference = anInterface.getString(PARAM_NAME_INTERFACE_REFERENCE);
                        String interfaceKey = anInterface.getString(PARAM_NAME_AVAILABLE_INTERFACE_KEY);
                        if (interfaceKey == null || interfaceReference == null) {
                            log.error(session.cpeKey + ": Invalid Available Interface Instance!" + anInterface);
                            continue;
                        }
                        if (newVlanPath.startsWith(interfaceReference)) {
                            log.info(session.cpeKey + ": InterfaceKey for the new VLAN: " + interfaceKey);
                            newWanInterfaceKey = interfaceKey;
                        }
                    }

                    /**
                     * Analyze all filter instances
                     */
                    for (String filterIndex : allFilters.getFieldNames()) {
                        JsonObject aFilter = allFilters.getObject(filterIndex);
                        String filterKey = aFilter.getString("FilterKey");
                        if (newWanInterfaceKey.equals(filterKey)) {
                            log.info(session.cpeKey + ": Filter Index: " + filterIndex);
                            newWanInterfaceFilterIndex = filterIndex;
                            break;
                        }
                    }

                    if (newWanInterfaceFilterIndex == null) {
                        log.error(session.cpeKey + ": Unable to get the interfaceKey for the newly created WAN "
                                + "Connection (" + newVlanPath + ")!");
                        failed(
                                CpeDeviceOp.CPE_DEVICE_OP_STATE_INTERNAL_ERROR,
                                INTERNAL_ERROR_NO_KEY_FOR_THE_NEW_WAN_CONN
                        );
                    } else {
                        includedFilters.add(newWanInterfaceFilterIndex);

                        // Configure all objects
                        state = FsmStateEnum.SetAllParamValues;
                        SetParameterValues.start(
                                session,
                                buildSetParameterValuesList(),
                                new SetParameterValuesResponseHandler(),
                                CwmpRequest.CWMP_REQUESTER_ACS
                        );
                    }
            }

            /**
             * Debug Print
             */
            log.debug(session.cpeKey + ": state change: " + oldState.name() + " -> " + state.name());
        }
    }

    /**
     * Process the ParameterValueStruct array that contains all the WAN Connection Instances and L2Bridge Instances.
     *
     * @param paramValues
     */
    public void processAllParameterValues(JsonObject paramValues) {
        FsmStateEnum oldState = state;
        /**
         * Traverse all parameter values in the response from the CPE
         */
        /**
         * Create An Array with all the "InternetGatewayDevice.Layer2Bridging.Filter.{i}" instances
         */
        JsonObject allWanConnections = VertxJsonUtils.deepGet(paramValues, wanIpConnectionPrefix);
        if (allWanConnections == null) {
            log.error(session.cpeKey + ": Invalid WAN Connection GetParameterValuesResponse from Device!"
                    + allWanConnections);
            failed(
                    CpeDeviceOp.CPE_DEVICE_OP_STATE_INTERNAL_ERROR,
                    INTERNAL_ERROR_UNEXPECTED_DEVICE_RESPONSE
            );
            return;
        }

        existingVoiceBoundIf = VertxJsonUtils.deepGet(paramValues, VOICE_SERVICE_PREFIX + VOICE_SERVICE_WAN_IP_CONNECTION_PATH);

        /**
         * Analyze all WAN Connections (i.e. VLANs)
         */
        for (String wanConnIndex : allWanConnections.getFieldNames()) {
            JsonObject aWanConn = allWanConnections.getObject(wanConnIndex);
            String thisVlanId = aWanConn.getString(PARAM_NAME_VLAN_ID);
            if (vlanId == null) {
                log.error(session.cpeKey + ": Received an invalid WAN Connection Object ("
                        + wanConnIndex + ")!" + aWanConn);
                continue;
            }

            String vlanPath = wanIpConnectionPrefix + wanConnIndex + ".";
            String connType = aWanConn.getString("ConnectionType");

            // Determine the existing VLAN type and mode
            log.debug("Found an existing " + connType + " VLAN: " + vlanPath + " : " + vlanIdToString(thisVlanId));
            boolean bIsDefaultDataVlan = session.cpe.deviceId.wanIpConnectionPath.endsWith("." + wanConnIndex);
            boolean bIsVoiceVlan = existingVoiceBoundIf != null && existingVoiceBoundIf.equals(vlanPath);
            boolean bIsVideoVlan = false;
            if (!bIsDefaultDataVlan) {
                bIsVideoVlan = "true".equalsIgnoreCase(aWanConn.getString(PARAM_NAME_IGMP_PROXY)) |
                        "IP_Bridged".equals(aWanConn.getString("ConnectionType"));
            }
            boolean bIsSameVlanId = thisVlanId.equals(vlanId.toString());
            log.debug("bIsDefaultDataVlan : " + bIsDefaultDataVlan
                    + ", bIsVideoVlan: " + bIsVideoVlan
                    + ", bIsVoiceVlan: " + bIsVoiceVlan
                    + ", bIsSameVlanId: " + bIsSameVlanId);

            if (bIsSameVlanId) {
                if (bIsDefaultDataVlan && bVoiceService) {
                    DeviceOpUtils.callbackInvalidReq(
                            session,
                            parentDeviceOp,
                            vlanIdToString(thisVlanId) + " is already used by the Data Service"
                                    + " thus cannot be used by voice service!"
                    );
                    return;
                }
                if (bIsDefaultDataVlan && bBridged) {
                    DeviceOpUtils.callbackInvalidReq(
                            session,
                            parentDeviceOp,
                            vlanIdToString(thisVlanId) + " is already used by the Data Service"
                                    + " thus cannot be used by bridged video service!"
                    );
                    return;
                }

                if (serviceConnType.equals(connType) || "Unconfigured".equals(connType)) {
                    existingVlanPath = vlanPath;
                    log.debug("Reusing existing VLAN " + existingVlanPath + " for " + serviceName);
                    bReuseDataVlan = bIsDefaultDataVlan;
                    existingVlanObject = aWanConn;
                } else {
                    log.debug("Existing VLAN is configured in a different mode (" + connType + "). Delete first...");
                    addUnusedObject(vlanPath);
                }
            } else {
                // Consider unused if this VLAN is un-configured
                if ("Unconfigured".equals(aWanConn.getString("ConnectionStatus"))) {
                    addUnusedObject(vlanPath);
                } else if (bVoiceService && bIsVoiceVlan) {
                    // Configuring voice service
                    // Found the existing Voice VLAN
                    log.debug("Found existing Voice VLAN using another VLAN (" + vlanIdToString(thisVlanId) + ")");
                    addUnusedObject(vlanPath);
                } else if (!bVoiceService && bIsVideoVlan) {
                    // Configuring Video Service
                    log.debug("Found existing Video VLAN using another VLAN ("
                            + vlanIdToString(thisVlanId) + ") in "
                            + aWanConn.getString("ConnectionType") + " mode. Deleting it..");
                    addUnusedObject(vlanPath);
                } else if (!bIsDefaultDataVlan && !bIsVideoVlan && !bIsVoiceVlan) {
                    // Not the default data vlan, not a video or voice vlan either
                    //addUnusedObject(vlanPath);
                    log.info("TODO: Delete " + vlanPath
                            + ", IGMP: " + aWanConn.getString(PARAM_NAME_IGMP_PROXY)
                            + ", mode:" + aWanConn.getString("ConnectionType"));
                }
            }
        }

        /**
         * Analyze all bridges
         */
        JsonObject allBridges = VertxJsonUtils.deepGet(paramValues, L2_BRIDGES_PREFIX);
        JsonObject allFilters = VertxJsonUtils.deepGet(paramValues, L2_BRIDGE_FILTER_PREFIX);
        JsonObject allAvailableInterfaces = VertxJsonUtils.deepGet(paramValues, L2_INTERFACES_PREFIX);
        if (allBridges == null || allFilters == null || allAvailableInterfaces == null) {
            log.error(session.cpeKey + ": Invalid L2 Bridge GetParameterValuesResponse from Device!" + paramValues);
            DeviceOpUtils.callbackInternalError(
                    session,
                    parentDeviceOp,
                    INTERNAL_ERROR_UNEXPECTED_DEVICE_RESPONSE
            );
            return;
        }
        for (String bridgeIndex : allBridges.getFieldNames()) {
            JsonObject aBridge = allBridges.getObject(bridgeIndex);
            String bridgeName = aBridge.getString(PARAM_NAME_BRIDGE_NAME);
            if (bridgeName != null) {
                if ("Default".equals(bridgeName)) {
                    // Default bridge, leave it alone
                } else {    //if (L2_SSID_BRIDGE_NAME.equals(bridgeName)) {
                    // Found a non-default bridge
                    existingL2SSIDBridgePath = L2_BRIDGES_PREFIX + bridgeIndex + ".";
                    layer2SsidBridgeKey = aBridge.getString(PARAM_NAME_BRIDGE_KEY);
                //} else {
                //    addUnusedObject(L2_BRIDGES_PREFIX + bridgeIndex + ".");
                }
            }
        }

        if (bBridged) {
            /**
             * Enable IPTV SSID L2 Bridge if needed
             */
            if (bridgedInterfaces.contains(INTERFACE_INDEX_IPTV_SSID)) {
                String iptvSsidL2BridgeEnable = VertxJsonUtils.deepGet(
                        paramValues,
                        IPTV_SSID_PREFIX + PARAM_SSID_L2_BRIDGE_ENABLE
                ).toString();
                if (!"true".equals(iptvSsidL2BridgeEnable)) {
                    state = FsmStateEnum.EnablingIPTVL2Bridge;
                    SetParameterValues.start(
                            session,
                            buildIptvSsidL2BridgeEnableSetParameterValuesList(),
                            new SetParameterValuesResponseHandler(),
                            CwmpRequest.CWMP_REQUESTER_ACS
                    );
                    return;
                }
            }

            /**
             * Analyze all available interfaces
             */
            JsonArray bridgedInterfaceKeys = new JsonArray();
            for (String interfaceIndex : allAvailableInterfaces.getFieldNames()) {
                JsonObject anInterface = allAvailableInterfaces.getObject(interfaceIndex);
                String interfaceReference = anInterface.getString(PARAM_NAME_INTERFACE_REFERENCE);
                String interfaceKey = anInterface.getString(PARAM_NAME_AVAILABLE_INTERFACE_KEY);
                if (interfaceKey == null || interfaceReference == null) {
                    log.error(session.cpeKey + ": Invalid Available Interface Instance!" + anInterface);
                    continue;
                }

                if (existingVlanPath != null && existingVlanPath.startsWith(interfaceReference)) {
                    bridgedInterfaceKeys.add(interfaceKey);
                }

                if (bridgedInterfaces != null && bridgedInterfaces.contains(L2_INTERFACES_PREFIX + interfaceKey)) {
                    bridgedInterfaceKeys.add(interfaceKey);
                }
            }
            log.debug("bridgedInterfaceKeys: " + bridgedInterfaceKeys);

            /**
             * Analyze all filter instances
             */
            for (String filterIndex : allFilters.getFieldNames()) {
                JsonObject aFilter = allFilters.getObject(filterIndex);
                String bridgeRef = aFilter.getString("FilterBridgeReference");
                if (bridgedInterfaceKeys.contains(filterIndex)) {
                    // We want to include this filter into L2SSID Bridge if it is not already referencing the bridge
                    includedFilters.add(filterIndex);
                } else {
                    // We don't want to include this filter into L2SSID Bridge
                    JsonObject matchingInterface = allAvailableInterfaces.getObject(filterIndex);
                    if (matchingInterface != null &&
                            unUsedObjects != null &&
                            unUsedObjects.contains(matchingInterface.getString("InterfaceReference") + ".")) {
                        // This is an WAN interface to be deleted
                    } else if (layer2SsidBridgeKey != null && layer2SsidBridgeKey.equals(bridgeRef)) {
                        // Exclude this filter from L2SSID Bridge
                        if (excludedFilters == null) {
                            excludedFilters = new JsonArray();
                        }
                        excludedFilters.add(filterIndex);
                    }
                }
            }

            log.debug("Existing BridgeKey: " + layer2SsidBridgeKey
                    + ", Filter Instances: " + includedFilters
                    + ", Filters to be excluded: " + excludedFilters);
        }

        if (bVoiceService) {
            /**
             * Analyze all codec capabilities
             */
            JsonObject allRawCodecCapabilities = VertxJsonUtils.deepGet(paramValues, VOICE_CODEC_CAPABILITIES_PREFIX);
            if (allRawCodecCapabilities == null || allRawCodecCapabilities.size() == 0) {
                log.error(session.cpeKey + ": Received invalid codec capabilities!" + allRawCodecCapabilities);
                return;
            }

            codecCapabilities = new JsonObject();
            allCodecs = new JsonObject();
            for (String codecIndex : allRawCodecCapabilities.getFieldNames()) {
                JsonObject aCodec = allRawCodecCapabilities.getObject(codecIndex);
                String codecType = aCodec.getString("Codec");
                if (codecType == null) {
                    log.error(session.cpeKey + ": Received invalid codec capabilities!" + allRawCodecCapabilities);
                    return;
                }
                codecCapabilities.putString(codecType, codecIndex);
            }
            for (String codecType : codecCapabilities.getFieldNames()) {
                String codecIndex = codecCapabilities.getString(codecType);
                VertxJsonUtils.deepAdd(
                        allCodecs,
                        "Line.1.Codec.List." + codecIndex + ".Enable",
                        false
                );
                VertxJsonUtils.deepAdd(
                        allCodecs,
                        "Line.2.Codec.List." + codecIndex + ".Enable",
                        false
                );
            }
        }

        if (unUsedObjects != null && unUsedObjects.size() > 0) {
            log.debug("Deleting un-used bridge and/or WAN connection objects: " + unUsedObjects);
            state = FsmStateEnum.DeletingUnusedObjects;
            DeleteObject.start(
                    (String) unUsedObjects.get(0),
                    session,
                    new DeleteUnUsedObjectResponseHandler()
            );
        } else {
            postReadAllParameterValues();
        }

        /**
         * Debug Print
         */
        log.debug(session.cpeKey + ": state change: " + oldState.name() + " -> " + state.name());
    }

    /**
     * After reading all parameter values, we may or may not have to delete some unused objects.
     *
     * This method is common in both cases
     */
    public void postReadAllParameterValues() {

        if (bServiceEnabled()) {
            /**
             * Service is enabled in Service Plan and in profile.
             *
             * Do we need to create any new object?
             */
            if (existingVlanPath == null) {
                // Create a new VLAN object
                state = FsmStateEnum.AddingNewVlan;
                AddObject.start(
                        wanIpConnectionPrefix,
                        session,
                        getAddObjectResponseHandler()
                );
            } else if (bBridged && existingL2SSIDBridgePath == null) {
                // Create a new L2Bridge object
                state = FsmStateEnum.AddingNewL2Bridge;
                AddObject.start(
                        L2_BRIDGES_PREFIX,
                        session,
                        getAddObjectResponseHandler()
                );
            } else {
                // Configure existing objects
                state = FsmStateEnum.SetAllParamValues;
                SetParameterValues.start(
                        session,
                        buildSetParameterValuesList(),
                        new SetParameterValuesResponseHandler(),
                        CwmpRequest.CWMP_REQUESTER_ACS
                );
            }
        } else {
            /**
             * Service is disabled in Service Plan or not included in the profile.
             */
            if (existingVlanPath != null) {
                if (!bReuseDataVlan) {
                    // Delete the existing video VLAN if not reusing the data VLAN
                    state = FsmStateEnum.DeletingExistingVideoVlan;
                    DeleteObject.start(
                            existingVlanPath,
                            session,
                            getDeleteObjectResponseHandler()
                    );
                } else {
                    // Reusing the data VLAN, simply disable IGMP on it
                    state = FsmStateEnum.SetAllParamValues;
                    SetParameterValues.start(
                            session,
                            buildVlanSetParameterValuesList(),
                            new SetParameterValuesResponseHandler(),
                            CwmpRequest.CWMP_REQUESTER_ACS
                    );
                }
            } else if (bBridged && existingL2SSIDBridgePath != null) {
                // Disable the existing L2SSID Bridge
                state = FsmStateEnum.DisablingExistingL2SSIDBridge;
                SetParameterValues.start(
                        session,
                        buildL2SSIDBridgeSetParameterValuesList(),
                        getSetParameterValuesResponseHandler(),
                        CwmpRequest.CWMP_REQUESTER_ACS
                );
            } else {
                /**
                 * All done
                 */
                done();
            }
        }
    }

    /**
     * Get an AddObjectResponseHandler instance.
     */
    public AddObjectResponseHandler getAddObjectResponseHandler() {
        if (addObjectResponseHandler == null) {
            addObjectResponseHandler = new AddObjectResponseHandler();
        }

        return addObjectResponseHandler;
    }

    /**
     * AddObject Response Handler Class
     */
    public class AddObjectResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        /**
         * Constructor
         */
        public AddObjectResponseHandler() {
            // Save deviceOp instance
            deviceOp = parentDeviceOp;
        }

        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            FsmStateEnum oldState = state;
            long instanceNumber = responseMessage.soapEnv.getBody().getAddObjectResponse().getInstanceNumber();

            // Figure out what object we have created.
            String objectCreated;
            if (state.equals(FsmStateEnum.AddingNewL2Bridge)) {
                objectCreated = L2_BRIDGES_PREFIX + instanceNumber + ".";
            } else {
                objectCreated = wanIpConnectionPrefix + instanceNumber + ".";
            }
            log.debug(session.cpeKey + ": state " + state.name() + ", created " + objectCreated);

            // Build Parameter Value List used to configure the new object
            if (state.equals(FsmStateEnum.AddingNewVlan)) {
                /**
                 * Created a new VLAN object
                 */
                newVlanPath = objectCreated;

                // Configure the new VLAN object
                state = FsmStateEnum.ConfiguringVlan;
                SetParameterValues.start(
                        session,
                        buildVlanSetParameterValuesList(),
                        new SetParameterValuesResponseHandler(),
                        CwmpRequest.CWMP_REQUESTER_ACS
                );
            } else {
                /**
                 * Created a new L2 Bridge
                 */
                newBridgePath = objectCreated;

                // Read the BridgeKey from the newly created L2 Bridge
                ParameterNames paramNames = ParameterNames.Factory.newInstance();
                paramNames.addString(objectCreated + PARAM_NAME_BRIDGE_KEY);

                state = FsmStateEnum.ReadingNewL2BridgeKey;
                GetParameterValues.start(session,
                        paramNames,
                        getGetParameterValuesResponseHandler(),
                        CwmpRequest.CWMP_REQUESTER_ACS
                );
            }

            // Configure the new object
            log.debug(session.cpeKey + ": state change: " + oldState.name() + " -> " + state.name());
        }
    }

    /**
     * Get a SetParameterValuesResponseHandler instance.
     */
    public SetParameterValuesResponseHandler getSetParameterValuesResponseHandler() {
        if (setParameterValuesResponseHandler == null) {
            setParameterValuesResponseHandler = new SetParameterValuesResponseHandler();
        }

        return setParameterValuesResponseHandler;
    }

    /**
     * Inner SetParameterValuesResponse Handler Class
     */
    public class SetParameterValuesResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        /**
         * Constructor
         */
        public SetParameterValuesResponseHandler() {
            // Save deviceOp instance
            deviceOp = parentDeviceOp;
        }

        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            /**
             * Perform the default/standard actions
             */
            SetParameterValues.defaultHandler.responseHandler(session, request, responseMessage);

            FsmStateEnum oldState = state;

            if (state.equals(FsmStateEnum.EnablingIPTVL2Bridge)) {
                VertxJsonUtils.deepAdd(
                        session.serviceParameterValues,
                        IPTV_SSID_PREFIX + PARAM_SSID_L2_BRIDGE_ENABLE,
                        true
                );

                // Continue processing all parameter values
                processAllParameterValues(session.serviceParameterValues);
            } else if (state.equals(FsmStateEnum.ConfiguringVlan)) {
                if (bBridged) {
                    if (existingL2SSIDBridgePath != null || newBridgePath != null) {
                        /**
                         * Read Bridge Interface Key for the newly created VLAN
                         */
                        state = FsmStateEnum.ReadingNewWanInterfaceKey;
                        ParameterNames paramNames = ParameterNames.Factory.newInstance();
                        paramNames.addString(L2_INTERFACES_PREFIX);
                        paramNames.addString(L2_BRIDGE_FILTER_PREFIX);
                        GetParameterValues.start(session,
                                paramNames,
                                new GetParameterValuesResponseHandler(),
                                CwmpRequest.CWMP_REQUESTER_ACS
                        );
                    } else {
                        /**
                         * Create a new L2 Bridge
                         */
                        state = FsmStateEnum.AddingNewL2Bridge;
                        AddObject.start(
                                L2_BRIDGES_PREFIX,
                                session,
                                getAddObjectResponseHandler()
                        );
                    }
                } else {
                    state = FsmStateEnum.SetAllParamValues;
                    SetParameterValues.start(
                            session,
                            buildSetParameterValuesList(),
                            new SetParameterValuesResponseHandler(),
                            CwmpRequest.CWMP_REQUESTER_ACS
                    );
                }
            } else {
                /**
                 * All done
                 */
                done();
            }
            log.debug(session.cpeKey + ": state change: " + oldState.name() + " -> " + state.name());
        }

        /**
         * Abstract Fault Response Handler Class that must be extended by actual requests
         *
         * @param session
         * @param request
         * @param cwmpFault
         */
        @Override
        public void faultHandler(CwmpSession session, CwmpRequest request, FaultDocument.Fault cwmpFault)
                throws CwmpException {
            failed(CpeDeviceOp.CPE_DEVICE_OP_STATE_INVALID_REQUEST, CwmpUtils.cwmpFaultToJsonObject(cwmpFault));
        }
    }

    /**
     * Get a DeleteObjectResponseHandler instance
     */
    public DeleteObjectResponseHandler getDeleteObjectResponseHandler() {
        if (deleteObjectResponseHandler == null) {
            deleteObjectResponseHandler = new DeleteObjectResponseHandler();
        }

        return deleteObjectResponseHandler;
    }

    /**
     * Response handler for the "DeleteObject"
     */
    public class DeleteObjectResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        /**
         * Constructor
         */
        public DeleteObjectResponseHandler() {
            // Save deviceOp instance
            deviceOp = parentDeviceOp;
        }
        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            FsmStateEnum oldState = state;

            switch (state) {
                case DeletingExistingVideoVlan:
                    if (existingL2SSIDBridgePath != null) {
                        // Disable the existing L2SSID Bridge
                        state = FsmStateEnum.DisablingExistingL2SSIDBridge;
                        SetParameterValues.start(
                                session,
                                buildL2SSIDBridgeSetParameterValuesList(),
                                getSetParameterValuesResponseHandler(),
                                CwmpRequest.CWMP_REQUESTER_ACS
                        );
                    } else {
                        /**
                         * All done
                         */
                        done();
                    }
                    break;

                case DeletingNewlyCreatedL2Bridge:
                    log.info("Successfully Deleted the newly created L2 Bridge " + newBridgePath);
                    if (newVlanPath != null) {
                        state = FsmStateEnum.DeletingNewlyCreatedVlan;
                        DeleteObject.start(
                                newVlanPath,
                                session,
                                getDeleteObjectResponseHandler()
                        );
                    }
                    break;

                case DeletingNewlyCreatedVlan:
                    log.info("Successfully Deleted the newly created WAN Service VLAN " + newVlanPath);
                    break;
            }

            log.debug(session.cpeKey + ": state change: " + oldState.name() + " -> " + state.name());
        }
    }

    /**
     * Build a CWMP ParameterValueList that sets everything
     */
    public ParameterValueList buildSetParameterValuesList() {
        ParameterValueList parameterValueList = null;

        if (bServiceEnabled()) {
            // Add VLAN Settings
            parameterValueList = buildVlanSetParameterValuesList();

            /**
             * Voice Service Specific
             */
            if (bVoiceService) {
                /**
                 * All Codecs
                 */
                parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                        session.cpe,
                        allCodecs,
                        parameterValueList,
                        VOICE_SERVICE_PROFILE_PREFIX
                );

                /**
                 * Bound the VLAN with Voice Service
                 */
                String voiceWanPath = newVlanPath == null? existingVlanPath : newVlanPath;
                if (voiceWanPath != null) {
                    //voiceWanPath = voiceWanPath.substring(0, voiceWanPath.length() - 1);
                    parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                            session.cpe,
                            new JsonObject().putString(
                                    VOICE_SERVICE_WAN_IP_CONNECTION_PATH,
                                    voiceWanPath
                            ),
                            parameterValueList,
                            VOICE_SERVICE_PREFIX
                    );
                }

                /**
                 * Apply Subscriber Specific Voice Settings
                 */
                JsonObject subscriberVoiceSettings = subscriberServicePlan.getObject(ServicePlan.FIELD_NAME_VOICE);
                // Process Dial Plan
                String dialPlanId = subscriberVoiceSettings.getString(
                        ServicePlan.FIELD_NAME_DIAL_PLAN,
                        DialPlan.SYSTEM_DEFAULT_DIAL_PLAN_ID
                );
                subscriberVoiceSettings.removeField(ServicePlan.FIELD_NAME_DIAL_PLAN);
                JsonObject dialPlan = session.sessionVertice.dialPlanCache
                        .getDialPlanById(dialPlanId);
                if (dialPlan == null) {
                    log.error("Unable to find dial plan " + dialPlanId + "! Using system-default for now.");
                    dialPlan = DialPlan.SYSTEM_DEFAULT_DIAL_PLAN;
                }
                // Convert the SXACC dial-plan object to RG object and merge with other voice settings
                VertxJsonUtils.merge(subscriberVoiceSettings, DialPlan.toRgDataModelObject(dialPlan));

                parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                        session.cpe,
                        subscriberVoiceSettings,
                        parameterValueList,
                        VOICE_SERVICE_PROFILE_PREFIX
                );

                // Also set VoiceService.{i}.PhyInterface.{i}.X_000631_Enable (i.e. "admin-state")
                parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                        session.cpe,
                        new JsonObject()
                                .putValue(
                                        "1.X_000631_Enable",
                                        VertxJsonUtils.deepGet(subscriberVoiceSettings, "Line.1.Enable")
                                )
                                .putValue(
                                        "2.X_000631_Enable",
                                        VertxJsonUtils.deepGet(subscriberVoiceSettings, "Line.2.Enable")
                                ),
                        parameterValueList,
                        VOICE_SERVICE_PHY_INTERFACE_PREFIX
                );

            } else if (bBridged) {
                // Set the bridge object
                parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                        session.cpe,
                        L2SSID_BRIDGE_ENABLE_PARAMS,
                        parameterValueList,
                        newBridgePath != null ? newBridgePath : existingL2SSIDBridgePath
                );

                // Set filters
                JsonObject enableFilter = FILTER_ENABLE.copy().putString("FilterBridgeReference", layer2SsidBridgeKey);
                for (int i = 0; i < includedFilters.size(); i ++) {
                    parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                            session.cpe,
                            enableFilter,
                            parameterValueList,
                            L2_BRIDGE_FILTER_PREFIX + includedFilters.get(i) + "."
                    );
                }
                if (excludedFilters != null) {
                    for (int i = 0; i < excludedFilters.size(); i++) {
                        parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                                session.cpe,
                                FILTER_DISABLE,
                                parameterValueList,
                                L2_BRIDGE_FILTER_PREFIX + excludedFilters.get(i) + "."
                        );
                    }
                }

                // SSID specific settings
                for (int i = 0; i < bridgedInterfaces.size(); i ++) {
                    String anInterface = bridgedInterfaces.get(i);
                    int interfaceIndex = Integer.valueOf(anInterface.substring(anInterface.lastIndexOf(".") + 1));
                    int ssidIndex = interfaceIndex - 4;
                    if (ssidIndex > 0) {
                        // Enable bridging for this SSID
                        parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                                session.cpe,
                                SSID_L2_BRIDGE_ENABLE,
                                parameterValueList,
                                WLAN_CONFIGURATION_PREFIX + ssidIndex + "."
                        );

                        // Also enable the radio
                        parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                                session.cpe,
                                RADIO_ENABLE,
                                parameterValueList,
                                ssidIndex <= 8 ?
                                        TWO_POINT_4_GHZ_RADIO_PREFIX
                                        :
                                        FIVE_GHZ_RADIO_PREFIX
                        );
                    }
                }
            } else {
                /**
                 * Routed Video Service
                 */
                if (existingL2SSIDBridgePath != null) {
                    // Disable the existing L2SSID Bridge
                    parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                            session.cpe,
                            L2SSID_BRIDGE_DISABLE,
                            parameterValueList,
                            existingL2SSIDBridgePath
                    );
                }
            }
        } else {
            if (bVoiceService) {
                // Disable Voice Service by disabling both lines
            } else if (existingL2SSIDBridgePath != null) {
                // Disable the existing L2SSID Bridge
                parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                        session.cpe,
                        L2SSID_BRIDGE_DISABLE,
                        parameterValueList,
                        existingL2SSIDBridgePath
                );
            }
        }

        // Update the serviceParameterValues for this session
        if (parameterValueList != null && nextService != null) {
            for (ParameterValueStruct valueStruct : parameterValueList.getParameterValueStructArray()) {
                VertxJsonUtils.deepAdd(
                        session.serviceParameterValues,
                        valueStruct.getName(),
                        valueStruct.getValue().getStringValue()
                );
            }
        }

        return parameterValueList;
    }

    /**
     * Build a CWMP ParameterValueList that configures the VLAN object
     */
    public ParameterValueList buildVlanSetParameterValuesList() {
        ParameterValueList parameterValueList = null;
        JsonObject overrides = null;

        // Add VLAN Parameters
        JsonObject vlanParamValues = new JsonObject();

        if (!bServiceEnabled()) {
            /**
             * Disabling an existing video service that is reusing data VLAN
             *
             * Simply disable IGMP for now
             */
            vlanParamValues.putBoolean(PARAM_NAME_IGMP_PROXY, false);

            /**
             * Remove the notion of this service from the Service Name of this WAN Connection
             */
            if (existingVlanObject != null) {
                String name = existingVlanObject.getString("Name");
                if (name != null && name.contains(serviceName)) {
                    if (name.contains(" " + serviceName)) {
                        vlanParamValues.putString("Name", name.replace(serviceName, "Service"));
                    }
                }
            }
        } else {
            for (String name : serviceProfile.getFieldNames()) {
                // Get the value
                Object rawValue = serviceProfile.getField(name);

                switch (name) {
                    case PARAM_NAME_SERVICE_CONNECTION_TYPE:
                        // Only set Service Connection Type for newly created VLAN (by us)
                        if (existingVlanPath == null) {
                            switch (rawValue.toString()) {
                                case SERVICE_CONNECTION_TYPE_DHCP:
                                    vlanParamValues.putString("ConnectionType", "IP_Routed");
                                    vlanParamValues.putString("AddressingType", "DHCP");
                                    break;

                                case SERVICE_CONNECTION_TYPE_BRIDGED:
                                    vlanParamValues.putString("ConnectionType", "IP_Bridged");
                                    break;
                            }
                        }
                        break;

                    case PARAM_NAME_BRIDGE_INTERFACE_ARRAY:
                        // List of bridged interfaces
                        break;

                    case Cpe.INTERNET_GATEWAY_DEVICE_ROOT:
                        // Override parameters (for voice)
                        overrides = serviceProfile.getObject(name);
                        break;

                    default:
                        if (name.startsWith(SxaCcTr098ModelExtensions.RTP_CODEC_PREFIX)) {
                            /**
                             * Process Abstract Codec Parameters
                             */
                            JsonObject codec = (JsonObject) rawValue;
                            String codecType = codec.getString("Codec");
                            String codecIndex = codecCapabilities.getString(codecType);
                            if (codecIndex != null) {
                                // Found a enabled code in profile
                                int priority;
                                if (name.equals(SxaCcTr098ModelExtensions.FIRST_ORDER_RTP_CODEC)) {
                                    priority = 1;
                                } else if (name.equals(SxaCcTr098ModelExtensions.SECOND_ORDER_RTP_CODEC)) {
                                    priority = 2;
                                } else {
                                    priority = 3;
                                }
                                JsonObject thisCodec = codec.copy()
                                        .putBoolean("Enable", true)
                                        .putNumber("Priority", priority);
                                thisCodec.removeField("Codec");

                                // Add to both lines
                                VertxJsonUtils.deepAdd(
                                        allCodecs,
                                        "Line.1.Codec.List." + codecIndex,
                                        thisCodec
                                );
                                VertxJsonUtils.deepAdd(
                                        allCodecs,
                                        "Line.2.Codec.List." + codecIndex,
                                        thisCodec
                                );
                            }
                        } else {
                            vlanParamValues.putString(name, rawValue.toString());
                        }
                        break;
                }
            }

            // Set mandatory parameters
            if (existingVlanPath == null) {
                vlanParamValues.putBoolean("Enable", true);
                vlanParamValues.putString("ExternalIPAddress", "0.0.0.0");
                vlanParamValues.putString("DefaultGateway", "0.0.0.0");
                vlanParamValues.putString("SubnetMask", "0.0.0.0");
            }

            // Are we re-using the existing VLAN?
            if (bReuseDataVlan) {
                // Rename the WAN connection accordingly
                vlanParamValues.putString("Name", "Data and " + serviceProfile.getString("Name"));
            }
        }

        /**
         * Add the VLAN parameters
         */
        parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                session.cpe,
                vlanParamValues,
                parameterValueList,
                newVlanPath == null ? existingVlanPath : newVlanPath
        );

        /**
         * Add the override parameters if any
         */
        if (overrides != null) {
            parameterValueList = SetParameterValues.jsonObjToParameterValuesList(
                    session.cpe,
                    overrides,
                    parameterValueList,
                    Cpe.INTERNET_GATEWAY_DEVICE_ROOT + "."
            );
        }

        return parameterValueList;
    }

    /**
     * Build a CWMP ParameterValueList for configuring the L2SSID Bridge Instance.
     */
    public ParameterValueList buildL2SSIDBridgeSetParameterValuesList() {
        JsonObject paramValues = null;
        if (bBridged && bServiceEnabled()) {
            paramValues = L2SSID_BRIDGE_ENABLE_PARAMS;
        } else {
            paramValues = new JsonObject().putBoolean(PARAM_NAME_BRIDGE_ENABLE, false);
        }

        return SetParameterValues.jsonObjToParameterValuesList(
                session.cpe,
                paramValues,
                null,
                existingL2SSIDBridgePath
        );
    }

    /**
     * Build a CWMP ParameterValueList for enabling the L2 Bridging on IPTV SSID Instance.
     */
    public ParameterValueList buildIptvSsidL2BridgeEnableSetParameterValuesList() {
        return SetParameterValues.jsonObjToParameterValuesList(
                session.cpe,
                new JsonObject()
                        .putBoolean(PARAM_SSID_L2_BRIDGE_ENABLE, true),
                null,
                IPTV_SSID_PREFIX
        );
    }

    /**
     * Is Video Service Enabled via Service Plan?
     */
    public boolean bServiceEnabled() {
        String fieldName = ServicePlan.FIELD_NAME_VIDEO;
        if (bVoiceService) {
            fieldName = ServicePlan.FIELD_NAME_VOICE;
        }

        if (serviceProfile == null
                || subscriberServicePlan == null
                || !subscriberServicePlan.containsField(fieldName)) {
            return false;
        }

        JsonObject subscriberServiceSettings = subscriberServicePlan.getObject(fieldName);
        boolean bEnabled;
        if (!bVoiceService) {
            // Video
            bEnabled = subscriberServiceSettings.getBoolean(ServicePlan.FIELD_NAME_ENABLE, false);
        } else {
            // Voice
            String line1Enabled = VertxJsonUtils.deepGet(subscriberServiceSettings, "Line.1.Enable");
            String line2Enabled = VertxJsonUtils.deepGet(subscriberServiceSettings, "Line.2.Enable");
            if (VOICE_LINE_ENABLED.equals(line1Enabled) || VOICE_LINE_ENABLED.equals(line2Enabled)) {
                bEnabled = true;
            } else {
                bEnabled = false;
            }
        }

        return bEnabled;
    }

    /**
     * Util Method to get the parent object path for a given parameter name.
     */
    public static String getObjectPath(String parameterName) {
        return parameterName.substring(0, parameterName.lastIndexOf(".") + 1);
    }

    /**
     * Add an object to the list of unused objects so it will be deleted before creating service.
     * @param objPath
     */
    public void addUnusedObject(String objPath) {
        if (unUsedObjects == null) {
            unUsedObjects = new ArrayList();
        }
        unUsedObjects.add(objPath);
    }

    /**
     * Get a String identifying the VLAN by vlan id string.
     * @param vlanId
     * @return
     */
    public static String vlanIdToString(String vlanId) {
        if ("-1".equals(vlanId)) {
            return "Untagged VLAN";
        } else {
            return "VLAN " + vlanId;
        }
    }

    /**
     * Response handler for the "DeleteObject" request (for deleting unused objects)
     */
    public class DeleteUnUsedObjectResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        /**
         * Constructor that takes the object name.
         */
        public DeleteUnUsedObjectResponseHandler() {
        }

        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            log.info(session.cpeKey + ": Deleted " + unUsedObjects.get(0));
            unUsedObjects.remove(0);
            if (unUsedObjects.size() > 0) {
                DeleteObject.start(
                        (String) unUsedObjects.get(0),
                        session,
                        new DeleteUnUsedObjectResponseHandler()
                );
            } else {
                // Resume the process
                postReadAllParameterValues();
            }
        }
    }

    /**
     * All done. Call back.
     */
    public void done() {
        // All done
        log.debug(session.cpeKey + ": " + serviceName + " handling completed.");
        state = FsmStateEnum.Done;

        if (nextService == null) {
            session.serviceParameterValues = null;
            DeviceOpUtils.callback(
                    session,
                    parentDeviceOp,
                    CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                    null
            );
        } else {
            nextService.start();
        }
    }

    /**
     * Failed. Callback and cleanup.
     */
    public void failed(String opState, JsonObject result) {
        log.debug(session.cpeKey + ": Failed to apply " + serviceName + " due to " +
                result.getString(AcsConstants.FIELD_NAME_ERROR));
        state = FsmStateEnum.Done;

        if (newVlanPath != null) {
            log.error(session.cpeKey + ": Deleting the new VLAN object we have created (" + newVlanPath + ")..");
            DeleteObject.start(
                    newVlanPath,
                    session,
                    getDeleteObjectResponseHandler()
            );
        }

        if (newBridgePath != null) {
            log.error(session.cpeKey + ": Deleting the new Bridge object we have created (" + newBridgePath + ")..");
            DeleteObject.start(
                    newBridgePath,
                    session,
                    getDeleteObjectResponseHandler()
            );
        }

        session.serviceParameterValues = null;

        // Callback
        DeviceOpUtils.callback(
                session,
                parentDeviceOp,
                opState,
                result
        );
    }
}
