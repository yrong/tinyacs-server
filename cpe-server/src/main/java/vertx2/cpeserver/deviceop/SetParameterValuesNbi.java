package vertx2.cpeserver.deviceop;

import vertx2.VertxJsonUtils;
import vertx2.cpeserver.session.CwmpRequest;
import vertx2.cpeserver.session.CwmpSession;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import vertx2.model.*;
import vertx2.util.GigaCenter;
import dslforumOrgCwmp12.ParameterValueList;
import dslforumOrgCwmp12.ParameterValueStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;
import org.xmlsoap.schemas.soap.envelope.Body;

/**
 * Project:  cwmp-parent
 *
 * This class extends CpeSetParameterValues by posting async result to the provided callback URL
 *
 * @author: ronyang
 */
public class SetParameterValuesNbi extends SetParameterValues {
    private static final Logger log = LoggerFactory.getLogger(SetParameterValuesNbi.class.getName());

    /**
     * Enqueue/Start a device-op request.
     * @param deviceOp
     * @param session
     * @
     */
    public static void startDeviceOp(JsonObject deviceOp, CwmpSession session)
            throws CwmpException {
        log.debug("SetParameterValues deviceOp: " + deviceOp);

        /**
         * Convert to TR-069 ParameterValuesList
         */
        JsonObject rawParameterValues = deviceOp.getObject(CpeDeviceOp.FIELD_NAME_PARAM_VALUES);
        if (rawParameterValues != null && rawParameterValues.size() > 0) {
            /**
             * Convert to ParameterValueList
             */
            ParameterValueList paramValueList = jsonObjToParameterValuesList(session.cpe, rawParameterValues, null, "");
            start(session,
                    paramValueList,
                    new CpeSetParameterValuesNbiResponseHandler(deviceOp),
                    CwmpRequest.CWMP_REQUESTER_ACS
            );
        } else {
            /**
             * Does the device op involve any dynamic objects?
             */
            if (deviceOp.containsField(CpeDeviceOp.FIELD_NAME_DYNAMIC_OBJECTS)) {
                /**
                 * Process Dynamic Objects
                 */
                DynamicObject.start(deviceOp, session);
            } else {
                /**
                 * Does the device op involve any WAN Services?
                 */
                if (deviceOp.containsField(CpeDeviceOp.FIELD_NAME_SERVICES)) {
                    /**
                     * Process WAN Services
                     */
                    WanService.start(deviceOp, session);
                } else {
                    String error = CpeDeviceOp.FIELD_NAME_PARAM_VALUES + " not found or empty!\n"
                            + deviceOp.encodePrettily();
                    DeviceOpUtils.callbackInvalidReq(session, deviceOp, error);
                }
            }
        }
    }

    /**
     * Enqueue/Start an NBI provisioning request.
     *
     * @param nbiProvisioning
     * @param session
     * @
     */
    public static void startNbiProvisioning(CwmpSession session, JsonObject nbiProvisioning)
            throws CwmpException {
        if (nbiProvisioning == null) {
            return;
        }

        log.info("SetParameterValues nbiProvisioning: " + nbiProvisioning);

        // Clear the force flag
        if (nbiProvisioning.containsField(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING_FORCE_APPLY)) {
            nbiProvisioning.removeField(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING_FORCE_APPLY);
            session.cpe.addUnSet(Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING + "."
                    + Cpe.DB_FIELD_NAME_INITIAL_PROVISIONING_FORCE_APPLY);
        }

        /**
         * Convert to TR-069 ParameterValuesList
         */
        if (nbiProvisioning != null && nbiProvisioning.size() > 0) {
            // Do the conversion
            JsonObject rawParameterValues = new JsonObject();
            ParameterValueList parameterValueList = null;
            for (String nbiPath : nbiProvisioning.getFieldNames()) {
                String tr098Path = NbiDeviceProvisioning.nbiPathToTr098Path(nbiPath) + ".";

                // Look for cwmp Extensions
                if (CWMPTr098ModelExtensions.containCWMPAbstractName(tr098Path)) {
                    tr098Path = CWMPTr098ModelExtensions.convertCWMPAbstractNameToActualName(session.cpe, tr098Path);
                }

                parameterValueList = jsonObjToParameterValuesList(
                        session.cpe,
                        nbiProvisioning.getObject(nbiPath),
                        parameterValueList,
                        tr098Path
                );

                // Convert to GigaCenter Path (if applicable)
                for (ParameterValueStruct valueStruct : parameterValueList.getParameterValueStructArray()) {
                    valueStruct.setName(GigaCenter.convertTr098Path(valueStruct.getName()));
                }
            }

            // Enqueue
            start(session,
                    parameterValueList,
                    defaultHandler,
                    CwmpRequest.CWMP_REQUESTER_ACS);
        }
    }

    /**
     * Custom Response Handler that publishes results on Redis
     */
    public static class CpeSetParameterValuesNbiResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        /**
         * The deviceOps are passed in during the Construction.
         *
         * @param deviceOp
         */
        public CpeSetParameterValuesNbiResponseHandler(JsonObject deviceOp) {
            this.deviceOp = deviceOp;
        }

        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            /**
             * Perform the default/standard actions
             */
            SetParameterValues.defaultHandler.responseHandler(session, request, responseMessage);

            /**
             * Does the device op involve any dynamic objects?
             */
            if (deviceOp.containsField(CpeDeviceOp.FIELD_NAME_DYNAMIC_OBJECTS)) {
                /**
                 * Process Dynamic Objects
                 */
                DynamicObject.start(deviceOp, session);
            } else {
                /**
                 * Does the device op involve any WAN Services?
                 */
                if (deviceOp.containsField(CpeDeviceOp.FIELD_NAME_SERVICES)) {
                    /**
                     * Process WAN Services
                     */
                    WanService.start(deviceOp, session);
                } else {
                    /**
                     * Send the result to the provided callback URL if any
                     */
                    DeviceOpUtils.callback(
                            session,
                            deviceOp,
                            CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                            null
                    );
                }
            }
        }
    }

    /**
     * Dynamic Path Handler that processes various type of response messages received during dynamic path process.
     */
    public static class DynamicPathHandler extends CpeSetParameterValuesNbiResponseHandler {
        JsonObject keyParameter;
        String keyParameterName;
        String pathPrefix;
        ParameterValueList paramValueList;

        /**
         * The deviceOps are passed in during the Construction.
         *
         * @param deviceOp
         * @param keyParameter
         * @param pathPrefix
         */
        public DynamicPathHandler(
                CwmpSession session,
                JsonObject deviceOp,
                JsonObject keyParameter,
                String pathPrefix,
                ParameterValueList paramValueList) {
            super(deviceOp);

            this.keyParameter = keyParameter;
            this.keyParameterName = keyParameter.getString("name");

            if (CWMPTr098ModelExtensions.containCWMPAbstractName(pathPrefix)) {
                this.pathPrefix = CWMPTr098ModelExtensions.convertCWMPAbstractNameToActualName(
                        session.cpe, pathPrefix);
            } else {
                this.pathPrefix = pathPrefix;
            }

            this.paramValueList = paramValueList;
        }

        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            Body body = responseMessage.soapEnv.getBody();

            if (body.isSetGetParameterValuesResponse()) {
                /**
                 * Process "GetParameterValuesResponse"
                 */
                ParameterValueList parameterValueList = body.getGetParameterValuesResponse().getParameterList();
                if (parameterValueList != null) {
                    ParameterValueStruct[] parameterValueStructs = parameterValueList.getParameterValueStructArray();
                    if (parameterValueStructs != null && parameterValueStructs.length > 0) {

                        for (ParameterValueStruct valueStruct : parameterValueStructs) {
                            String name = valueStruct.getName();
                            if (name.startsWith(pathPrefix) && name.endsWith(keyParameterName)) {
                                // Found it
                                log.info(session.cpeKey + ": Found existing entry: " + name);
                                String path = name.substring(0, name.indexOf("." + keyParameterName) + 1);

                                /**
                                 * Convert to JSON Object
                                 */
                                JsonObject existingParamValues =
                                        GetParameterValuesNbi.parameterValueStructsToJsonObject(parameterValueStructs);
                                JsonObject existingObject = VertxJsonUtils.deepGet(existingParamValues, path);

                                /**
                                 * Does the existing object already match the profile instance?
                                 */


                                if (keyParameter.getBoolean(
                                        ConfigurationCategory.FIELD_NAME_KEY_PARAMETER_ALLOW_OVERWRITE,
                                        true)
                                        ) {
                                    /**
                                     * We can overwrite the existing entry
                                     */
                                    doSetParameterValues(session, path);
                                } else {
                                    /**
                                     * Cannot overwrite.
                                     *
                                     * Must delete first and re-create the object
                                     */
                                    DeleteObject.start(
                                            path,
                                            session,
                                            this
                                    );
                                }
                                return;
                            }
                        }
                    }
                }

                /**
                 * Did not find an existing entry with the same key parameter value.
                 *
                 * Create a new object
                 */
                AddObject.start(
                        pathPrefix,
                        session,
                        this
                );
            } else if (body.isSetAddObjectResponse()) {
                /**
                 * Process "AddObjectResponse"
                 */
                long instanceNumber = body.getAddObjectResponse().getInstanceNumber();
                String objectCreated = pathPrefix + instanceNumber + ".";
                log.info("The Object " + objectCreated + " has been created.");
                doSetParameterValues(session, objectCreated);
            } else if (body.isSetDeleteObjectResponse()) {
                /**
                 * Process "DeleteObjectResponse".
                 *
                 * Now it is safe to create a new object
                 */
                AddObject.start(
                        pathPrefix,
                        session,
                        this
                );
            } else if (body.isSetSetParameterValuesResponse()) {
                /**
                 * Done
                 */
                super.responseHandler(session, request, responseMessage);
            }
        }

        /**
         * Convert all dynamic path with real path and the send the "SetParameterValues" message.
         *
         * @param session
         * @param path      The real actual path (must end with ".")
         */
        public void doSetParameterValues(CwmpSession session, String path) {
            /**
             * Convert dynamic path with real path
             */
            ParameterValueStruct[] valueStructs = paramValueList.getParameterValueStructArray();
            for (ParameterValueStruct valueStruct : valueStructs) {
                String name = valueStruct.getName();
                if (name.startsWith(pathPrefix)) {
                    valueStruct.setName(name.replace(pathPrefix + "{i}.", path));
                }
            }
            paramValueList.setParameterValueStructArray(valueStructs);

            start(session,
                    paramValueList,
                    new CpeSetParameterValuesNbiResponseHandler(deviceOp),
                    CwmpRequest.CWMP_REQUESTER_ACS
            );
        }
    }
}
