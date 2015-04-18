package com.calix.sxa.cc.cpeserver.deviceop;

import com.calix.sxa.cc.cpeserver.session.CwmpRequest;
import com.calix.sxa.cc.cpeserver.session.CwmpSession;
import com.calix.sxa.cc.cwmp.CwmpException;
import com.calix.sxa.cc.cwmp.CwmpMessage;
import com.calix.sxa.cc.model.CpeDeviceOp;
import com.calix.sxa.cc.model.CpeDeviceOpTypeEnum;
import dslforumOrgCwmp12.RebootDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by ronyang on 2014/8/7.
 */
public class Reboot {

    private static final Logger log = LoggerFactory.getLogger(Reboot.class.getName());

    /**
     * Create the request and add it to the queue using a custom response handler.
     *
     * @param session
     * @param deviceOp
     * @
     * @throws com.calix.sxa.cc.cwmp.CwmpException
     */
    public static void start(JsonObject deviceOp, CwmpSession session)
            throws CwmpException {

        /**
         * Build a new "Reboot" Message
         */
        CwmpMessage rebootMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        rebootMessage.rpcMessageName = "Reboot";
        RebootDocument.Reboot reboot =
                rebootMessage.soapEnv.getBody().addNewReboot();

        /**
         * could be used later for correlation with the M_Boot Inform
         */
        reboot.setCommandKey(deviceOp.getString(CpeDeviceOp.FIELD_NAME_ID));

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                        CwmpRequest.CWMP_REQUESTER_ACS,
                        rebootMessage,
                        "Reboot",
                        new RebootResponseHandler(deviceOp)
                )
        );
    }

    /**
     * Response handler for the "Reboot" request
     */
    public static class RebootResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {

        /**
         * Constructor
         *
         * @param deviceOp
         */
        public RebootResponseHandler(JsonObject deviceOp) {
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
             * Reboot request has been accepted by CPE.
             *
             * Let us mark this device op as in-progress
             */
            DeviceOpUtils.saveInProgressDeviceOp(session, deviceOp, CpeDeviceOpTypeEnum.Reboot);
        }
    }
}
