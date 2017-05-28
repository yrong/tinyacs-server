package vertx.cpeserver.deviceop;

import vertx.VertxJsonUtils;
import vertx.cpeserver.session.CwmpRequest;
import vertx.cpeserver.session.CwmpSession;
import vertx.cwmp.CwmpException;
import vertx.cwmp.CwmpMessage;
import vertx.model.Cpe;
import vertx.model.CpeDeviceOp;
import vertx.model.CWMPTr098ModelExtensions;
import dslforumOrgCwmp12.GetParameterValuesResponseDocument;
import dslforumOrgCwmp12.ParameterNames;
import dslforumOrgCwmp12.ParameterValueStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp-parent
 *
 * This class extends CpeGetParameterValues by posting async result to the provided callback URL
 *
 * @author: ronyang
 */
public class GetParameterValuesNbi extends GetParameterValues {
    private static final Logger log = LoggerFactory.getLogger(GetParameterValuesNbi.class.getName());
    private static final String DEEP_DISCOVER_PARAM_NAME = Cpe.INTERNET_GATEWAY_DEVICE_ROOT + ".";

    /**
     * Enqueue/Start the request.
     * @param deviceOp
     * @param session
     * @
     */
    public static void start(JsonObject deviceOp, CwmpSession session) {
        JsonArray rawParameterNames = deviceOp.getArray(CpeDeviceOp.FIELD_NAME_PARAM_NAMES);

        if (rawParameterNames == null || rawParameterNames.size() == 0) {
            String error = CpeDeviceOp.FIELD_NAME_PARAM_NAMES + " not found or empty!\n" + deviceOp.encodePrettily();
            DeviceOpUtils.callbackInvalidReq(session, deviceOp, error);
        } else {
            ParameterNames paramNames = ParameterNames.Factory.newInstance();
            for (int i=0; i < rawParameterNames.size(); i ++) {
                String paramName = rawParameterNames.get(i);
                if (CWMPTr098ModelExtensions.containCWMPAbstractName(paramName)) {
                    paramName = CWMPTr098ModelExtensions.convertCWMPAbstractNameToActualName(session.cpe, paramName);
                }
                paramNames.addString(paramName);
            }

            start(session,
                    paramNames,
                    new CpeGetParameterValuesNbiResponseHandler(deviceOp),
                    CwmpRequest.CWMP_REQUESTER_ACS);
        }
    }

    /**
     * Custom Response Handler that publishes results on Redis
     */
    public static class CpeGetParameterValuesNbiResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        public JsonObject paramValues = null;

        /**
         * The deviceOps are passed in during the Construction.
         *
         * @param deviceOp
         */
        public CpeGetParameterValuesNbiResponseHandler(JsonObject deviceOp) {
            this.deviceOp = deviceOp;
        }

        public CpeGetParameterValuesNbiResponseHandler() {
        }

        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(final CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            /**
             * Perform the default/standard actions
             */
            GetParameterValues.defaultHandler.responseHandler(session, request, responseMessage);

            if (!DEEP_DISCOVER_PARAM_NAME.equals(deviceOp.getArray(CpeDeviceOp.FIELD_NAME_PARAM_NAMES).get(0))) {
                /**
                 * Extract the parameter values from the response message.
                 */
                extractParamValues(responseMessage);
            }

            /**
             * Add the actual path at top level for all the abstract paths in the request
             */
            JsonArray paramNames = deviceOp.getArray(CpeDeviceOp.FIELD_NAME_PARAM_NAMES);
            for (int i=0; i < paramNames.size(); i ++) {
                String paramName = paramNames.get(i);
                if (CWMPTr098ModelExtensions.containCWMPAbstractName(paramName)) {
                    paramValues.putString(
                            paramName,
                            CWMPTr098ModelExtensions.convertCWMPAbstractNameToActualName(session.cpe, paramName)
                    );
                }
            }

            /**
             * Send async deviceOp result to the callback URL if any
             */
            DeviceOpUtils.callback(
                    session,
                    deviceOp,
                    CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                    paramValues
            );
        }

        /**
         * Extract the parameter values from the response message.
         *
          * @param responseMessage
         */
        public void extractParamValues(CwmpMessage responseMessage) {
            /**
             * Extract the response payload from SOAP message
             */
            GetParameterValuesResponseDocument.GetParameterValuesResponse response =
                    responseMessage.soapEnv.getBody().getGetParameterValuesResponse();

            /**
             * Traverse all parameter values in the response from the CPE
             */
            paramValues = parameterValueStructsToJsonObject(response.getParameterList().getParameterValueStructArray());
        }
    }

    /**
     * Convert the given ParameterValueStruct Array to a JSON Object.
     *
     * @param parameterValueStructs
     */
    public static JsonObject parameterValueStructsToJsonObject(ParameterValueStruct[] parameterValueStructs) {
        JsonObject paramValues = new JsonObject();
        for (ParameterValueStruct parameterValue : parameterValueStructs) {
            String name = parameterValue.getName();
            if (parameterValue.getValue() != null) {
                String value = parameterValue.getValue().getStringValue();
                //log.debug(name + " : " + value);
                //parameterValueArray.add(new JsonObject().putString(name, value));
                VertxJsonUtils.deepAdd(paramValues, name, value);
            }
        }
        return paramValues;
    }
}
