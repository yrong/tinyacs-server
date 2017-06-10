package vertx.cpeserver.deviceop;

import vertx.cpeserver.session.CwmpRequest;
import vertx.cpeserver.session.CwmpSession;
import vertx.cwmp.CwmpException;
import vertx.cwmp.CwmpMessage;
import vertx.model.CpeDeviceOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp-parent
 *
 * This class extends CpeSetParameterAttributes by posting async result to the provided callback URL
 *
 * @author: ronyang
 */
public class SetParameterAttributesNbi extends SetParameterAttributes {
    private static final Logger log = LoggerFactory.getLogger(SetParameterAttributesNbi.class.getName());

    /**
     * Enqueue/Start the request.

     * @param deviceOp
     * @param session
     */
    public static void start(JsonObject deviceOp, CwmpSession session)
            throws CwmpException {
        log.info("SetParameterAttributes deviceOp: " + deviceOp);

        /**
         * The parameter name/value pairs are stored in MongoDB as an array
         */
        JsonObject rawParamAttrs = deviceOp.getJsonObject(CpeDeviceOp.FIELD_NAME_PARAM_ATTRIBUTES);
        if (rawParamAttrs != null && rawParamAttrs.size() > 0) {
            start(session,
                    jsonObjToParameterAttributesList(rawParamAttrs, null, ""),
                    new CpeSetParameterAttributesNbiResponseHandler(deviceOp),
                    CwmpRequest.CWMP_REQUESTER_ACS);
        } else {
            String error = CpeDeviceOp.FIELD_NAME_PARAM_ATTRIBUTES + " not found or empty!\n"
                    + deviceOp.encodePrettily();
            DeviceOpUtils.callbackInvalidReq(
                    session,
                    deviceOp,
                    error
            );
        }
    }

    /**
     * Custom Response Handler that publishes results on Redis
     */
    public static class CpeSetParameterAttributesNbiResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        /**
         * The deviceOps are passed in during the Construction.
         *
         * @param deviceOp
         */
        public CpeSetParameterAttributesNbiResponseHandler(JsonObject deviceOp) {
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
            SetParameterAttributes.defaultHandler.responseHandler(session, request, responseMessage);

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
