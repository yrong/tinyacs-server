package vertx2.cpeserver.deviceop;

import vertx2.cpeserver.session.CwmpRequest;
import vertx2.cpeserver.session.CwmpSession;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import vertx2.model.CpeDeviceOpTypeEnum;
import dslforumOrgCwmp12.FactoryResetDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Created by ronyang on 2014/8/7.
 */
public class FactoryReset {

    private static final Logger log = LoggerFactory.getLogger(FactoryReset.class.getName());

    /**
     * Create the request and add it to the queue using a custom response handler.
     *
     * @param session
     * @param deviceOp
     * @
     * @throws vertx2.cwmp.CwmpException
     */
    public static void start(JsonObject deviceOp, CwmpSession session)
            throws CwmpException {

        /**
         * Build a new "Reboot" Message
         */
        CwmpMessage factoryResetMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        factoryResetMessage.rpcMessageName = "FactoryReset";
        FactoryResetDocument.FactoryReset factoryReset =
                factoryResetMessage.soapEnv.getBody().addNewFactoryReset();

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                        CwmpRequest.CWMP_REQUESTER_ACS,
                        factoryResetMessage,
                        "FactoryReset",
                        new FactoryResetResponseHandler(deviceOp)
                )
        );
    }

    /**
     * Response handler for the "Reboot" request
     */
    public static class FactoryResetResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {

        /**
         * Constructor
         *
         * @param deviceOp
         */
        public FactoryResetResponseHandler(JsonObject deviceOp) {
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
             * Factory Reset request has been accepted by CPE.
             *
             * Let us mark this device op as in-progress
             */
            DeviceOpUtils.saveInProgressDeviceOp(session, deviceOp, CpeDeviceOpTypeEnum.FactoryReset);
        }
    }
}
