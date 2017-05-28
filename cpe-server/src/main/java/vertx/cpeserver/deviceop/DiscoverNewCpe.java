package vertx.cpeserver.deviceop;

import vertx.cpeserver.session.CwmpRequest;
import vertx.cpeserver.session.CwmpSession;
import vertx.cwmp.CwmpException;
import vertx.cwmp.CwmpFaultCodes;
import vertx.cwmp.CwmpMessage;
import vertx.model.Cpe;
import vertx.model.CpeDeviceType;
import vertx.util.CpeDataModelMgmt;
import dslforumOrgCwmp12.GetParameterValuesResponseDocument;
import dslforumOrgCwmp12.ParameterNames;
import dslforumOrgCwmp12.ParameterValueStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * CPE Auto Discovery Related Code, including the data model learning.
 *
 * Initially we use this class to perform all discovery related tasks, but later decided
 * to move the deep discovery part out of this class into a centralized worker so we can
 * control the pace and also perform some higher-level tasks (CPE server is more on the low
 * level side).
 *
 * @author: ronyang
 */
public class DiscoverNewCpe {
    private static final Logger log = LoggerFactory.getLogger(DiscoverNewCpe.class.getName());

    /**
     * A static/final instance of the Default response handler
     */
    private static final GetDeviceTypeResponseHandler defaultGetDeviceTypeResponseHandler =
            new GetDeviceTypeResponseHandler();

    /**
     * Start a CPE Auto Discovery Process.
     *
     *  1. Send a partial "GetParameterValues" to identify the device type.
     *  2. If device type (and data model) is already known, simply send a full "GetParameterValues" to
     *     retrieve all parameter values and store in DB
     *  3. If device type is new, send a full "GetParameterNames" to learn the data model.
     * @param session
     */
    public static void start(CwmpSession session) {
        log.info("Starting Auto-Discovery process for CPE " + session.cpeKey);

        /**
         * Build a new "GetParameterValues" Message to retrieve parameters needed to identify device type
         * TODO: Also retrieve FW version
         */
        ParameterNames paramNames = ParameterNames.Factory.newInstance();
        paramNames.addString(session.cpe.rootObjectName + ".DeviceInfo.ModelName");
        if (session.cpe.deviceId.hwVersion == null)
            paramNames.addString(session.cpe.rootObjectName + ".DeviceInfo.HardwareVersion");
        if (session.cpe.deviceId.additionalHwVersion == null)
            paramNames.addString(session.cpe.rootObjectName + ".DeviceInfo.AdditionalHardwareVersion");
        if (session.cpe.deviceId.productClass == null)
            paramNames.addString(session.cpe.rootObjectName + ".DeviceInfo.ProductClass");
        if (session.cpe.deviceId.swVersion == null)
            paramNames.addString(session.cpe.rootObjectName + ".DeviceInfo.SoftwareVersion");
        if (session.cpe.deviceId.manufacturer == null)
            paramNames.addString(session.cpe.rootObjectName + ".DeviceInfo.Manufacturer");
        paramNames.addString(Cpe.PERIODIC_INFORM_ENABLE_PARAM_PATH);
        paramNames.addString(Cpe.PERIODIC_INFORM_INTERVAL_PARAM_PATH);
        /*
        if (session.cpe.deviceId.connRequestUsername == null)
            paramNames.addString(session.cpe.rootObjectName + ".ManagementServer.ConnectionRequestUsername");
        if (session.cpe.deviceId.connRequestPassword == null)
            paramNames.addString(session.cpe.rootObjectName + ".ManagementServer.ConnectionRequestPassword");
        */
        paramNames.setArrayType("xsd:string[" + paramNames.sizeOfStringArray() + "]");

        GetParameterValues.start(
                session,
                paramNames,
                defaultGetDeviceTypeResponseHandler,
                CwmpRequest.CWMP_REQUESTER_LOCAL
        );
    }

    /**
     * Response handler for the "GetParameterValues" request (for retrieving the device type)
     */
    public static class GetDeviceTypeResponseHandler extends GetParameterValues.GetParameterValuesResponseHandler {
        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            /**
             * Call the super's handler first to store the values to the CPE object
             */
            super.responseHandler(session, request, responseMessage);

            /**
             * Update CPE Information device type information
             */
            GetParameterValuesResponseDocument.GetParameterValuesResponse response =
                    responseMessage.soapEnv.getBody().getGetParameterValuesResponse();
            for (ParameterValueStruct parameterValue : response.getParameterList().getParameterValueStructArray()) {
                if (parameterValue.getName().equals(session.cpe.rootObjectName + ".DeviceInfo.ModelName")) {
                    session.cpe.deviceId.modelName = parameterValue.getValue().getStringValue();
                } else if (parameterValue.getName().equals(session.cpe.rootObjectName + ".DeviceInfo.ProductClass")) {
                    session.cpe.deviceId.productClass = parameterValue.getValue().getStringValue();
                } else if (parameterValue.getName().equals(session.cpe.rootObjectName + ".DeviceInfo.ModelName")) {
                    session.cpe.deviceId.modelName = parameterValue.getValue().getStringValue();
                } else if (parameterValue.getName().equals(session.cpe.rootObjectName + ".DeviceInfo.HardwareVersion")) {
                    session.cpe.deviceId.hwVersion = parameterValue.getValue().getStringValue();
                } else if (parameterValue.getName().equals(session.cpe.rootObjectName + ".DeviceInfo.AdditionalHardwareVersion")) {
                    session.cpe.deviceId.additionalHwVersion = parameterValue.getValue().getStringValue();
                } else if (parameterValue.getName().equals(session.cpe.rootObjectName + ".DeviceInfo.SoftwareVersion")) {
                    session.cpe.deviceId.swVersion = parameterValue.getValue().getStringValue();
                } else if (parameterValue.getName().equals(session.cpe.rootObjectName + ".DeviceInfo.Manufacturer")) {
                    session.cpe.deviceId.manufacturer= parameterValue.getValue().getStringValue();
                } else if (parameterValue.getName().equals(Cpe.PERIODIC_INFORM_INTERVAL_PARAM_PATH)) {
                    try {
                        session.cpe.deviceId.informInterval =
                                Integer.valueOf(parameterValue.getValue().getStringValue());
                    } catch (Exception ex) {
                        log.error("Received illegal value for " + Cpe.PERIODIC_INFORM_INTERVAL_PARAM_PATH
                                + " (" + parameterValue.getValue().getStringValue() + ")!");
                    }
                } else if (parameterValue.getName().equals(Cpe.PERIODIC_INFORM_ENABLE_PARAM_PATH)) {
                    try {
                        session.cpe.deviceId.bPeriodicInformEnabled =
                                Boolean.valueOf(parameterValue.getValue().getStringValue());
                    } catch (Exception ex) {
                        log.error("Received illegal value for " + Cpe.PERIODIC_INFORM_ENABLE_PARAM_PATH
                                + " (" + parameterValue.getValue().getStringValue() + ")!");
                    }
                /*
                } else if (parameterValue.getName().equals(session.cpe.rootObjectName + ".ManagementServer.ConnectionRequestUsername")) {
                    session.cpe.deviceId.connRequestUsername= parameterValue.getValue().getStringValue();
                } else if (parameterValue.getName().equals(session.cpe.rootObjectName + ".ManagementServer.ConnectionRequestPassword")) {
                    session.cpe.deviceId.connRequestPassword= parameterValue.getValue().getStringValue();
                */
                } else {
                    log.error("Unexpected parameter: " + parameterValue.getName() + ":" +
                            parameterValue.getValue().getStringValue());
                    throw new CwmpException(
                            "Null or Invalid GetParameterValuesResponse message",
                            CwmpFaultCodes.ACS_INVALID_ARGS,
                            responseMessage.cwmpVersion
                    );
                }
            }

            // Use default connection request credentials
            session.cpe.deviceId.connRequestUsername = ManagementServerBootstrap.DEFAULT_USERNAME;
            session.cpe.deviceId.connRequestPassword = ManagementServerBootstrap.DEFAULT_PASSWORD;
            session.cpe.cpeJsonObj.putString("_id", session.cpe.getCpeKey());
            session.cpe.cpeJsonObj.putString(Cpe.DB_FIELD_NAME_ORG_ID, session.cpe.deviceId.getOrgId());
            session.cpe.deviceId.addToJsonObject(session.cpe.cpeJsonObj);
            log.info("Updated CPE info: " + session.cpe.toString());
            session.cpe.bDiscoveryDone = true;

            /**
             * Save the device type if new
             */
            CpeDeviceType.addIfNew(session.vertx.eventBus(), session.cpe.deviceId.toDeviceTypeObject());

            /**
             * Is the Device Type and Data Model Known?
             */
            session.cpe.dataModel = CpeDataModelMgmt.findDataModelByDeviceType(session.cpe.deviceId);
            if (session.cpe.dataModel == null) {
                /**
                 * No data model found for this CPE's device type
                 */
                log.info("No data model found for CPE " + session.cpe.toString());

                /**
                 * TODO: learn the data model from this CPE via "GetParameterNames"
                 */

                /**
                 * TODO: Reject this CPE
                 */
                //throw new CwmpException("Unknown combination of CPE Device Manufacturer/Model/OUI/HwVer/SwVer!");
            }

            /**
             * Save new CPE object to MongoDB
             */
            session.cpe.saveNewCpeToDb(session.eventBus);

            /**
             * TODO: Publish "Discovery Started" event
             */
            //session.cpe.publishEvent(MessageConstants.EVENT_CPE_DISCOVERY, "Discovery Started");

            /**
             * Retrieve all objects and parameter values by sending another "GetParameterValues" Message
             */
            //GetAllParameterValues.start(session);

            /**
             * Enqueue initial-provisioning parameter values
             */
            JsonObject initialProvisioning = session.cpe.cpeJsonObj.getObject(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING);
            if (initialProvisioning != null) {
                SetParameterValuesNbi.startNbiProvisioning(session, initialProvisioning);
            }

            /**
             * Run an extra get for WAN parameters
             */
            GetWanConnectionParameters.start(session);

            /**
             * Bootstrap the following Management Server Attributes:
             *
             * - Connection Request Username/Password
             * - Periodical Inform Enable/Interval
             */
            ManagementServerBootstrap.start(session);

            /**
             * Read the Calix ONT Registration ID (i.e. "RONTA") if any
             */
            GetRegistrationId.start(session);

            /**
             * Try to enable passive notification on change counter
             */
            EnableNotifOnChangeCounter.start(session);
        }
    }
}
