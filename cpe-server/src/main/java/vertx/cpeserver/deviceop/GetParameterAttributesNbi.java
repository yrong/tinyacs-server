package vertx.cpeserver.deviceop;

import vertx.cpeserver.session.CwmpRequest;
import vertx.cpeserver.session.CwmpSession;
import vertx.cwmp.CwmpException;
import vertx.cwmp.CwmpMessage;
import vertx.model.CpeDeviceOp;
import vertx.VertxJsonUtils;
import dslforumOrgCwmp12.GetParameterAttributesResponseDocument;
import dslforumOrgCwmp12.ParameterNames;
import dslforumOrgCwmp12.ParameterAttributeStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp-parent
 *
 * This class extends CpeGetParameterAttributes by posting async result to the provided callback URL
 *
 * @author: ronyang
 */
public class GetParameterAttributesNbi extends GetParameterAttributes {
    private static final Logger log = LoggerFactory.getLogger(GetParameterAttributesNbi.class.getName());

    /**
     * Enqueue/Start the request.
     * @param deviceOp
     * @param session
     * @
     */
    public static void start(JsonObject deviceOp, CwmpSession session) {
        log.info("GetParameterAttributes deviceOp: " + deviceOp);

        JsonArray rawParameterNames = deviceOp.getJsonArray(CpeDeviceOp.FIELD_NAME_PARAM_NAMES);

        if (rawParameterNames == null || rawParameterNames.size() == 0) {
            String error = CpeDeviceOp.FIELD_NAME_PARAM_NAMES + " not fond or empty!\n" + deviceOp.encodePrettily();
            DeviceOpUtils.callbackInvalidReq(session, deviceOp, error);
        } else {
            ParameterNames paramNames = ParameterNames.Factory.newInstance();
            for (int i=0; i < rawParameterNames.size(); i ++) {
                 String parameterName = rawParameterNames.getString(i);
                 paramNames.addString(parameterName);
            }

            start(session,
                    paramNames,
                    new CpeGetParameterAttributesNbiResponseHandler(deviceOp),
                    CwmpRequest.CWMP_REQUESTER_ACS);
        }
    }

    /**
     * Custom Response Handler that publishes results on Redis
     */
    public static class CpeGetParameterAttributesNbiResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        /**
         * The deviceOps are passed in during the Construction.
         *
         * @param deviceOp
         */
        public CpeGetParameterAttributesNbiResponseHandler(JsonObject deviceOp) {
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
            GetParameterAttributes.defaultHandler.responseHandler(session, request, responseMessage);

            /**
             * Extract the response payload from SOAP message
             */
            GetParameterAttributesResponseDocument.GetParameterAttributesResponse response =
                    responseMessage.soapEnv.getBody().getGetParameterAttributesResponse();

            /**
             * Traverse all parameter values in the response from the CPE
             */
            JsonObject paramAttributes = new JsonObject();
            JsonArray parameterValueArray = new JsonArray();
            for (ParameterAttributeStruct attributeStruct : response.getParameterList().getParameterAttributeStructArray()) {
                String name = attributeStruct.getName();
                //parameterValueArray.add(new JsonObject().putNumber(name, attributeStruct.getNotification()));
                VertxJsonUtils.deepAdd(paramAttributes, name, attributeStruct.getNotification());
            }

            /**
             * Send async deviceOp result to the callback URL if any
             */
            DeviceOpUtils.callback(
                    session,
                    deviceOp,
                    CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                    paramAttributes //new JsonObject().putArray(CpeDeviceOp.FIELD_NAME_PARAM_ATTRIBUTES, parameterValueArray)
            );
        }
    }
}
