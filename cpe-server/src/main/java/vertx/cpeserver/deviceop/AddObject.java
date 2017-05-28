package vertx.cpeserver.deviceop;

import vertx.cpeserver.session.CwmpRequest;
import vertx.cpeserver.session.CwmpSession;
import vertx.cwmp.CwmpException;
import vertx.cwmp.CwmpMessage;
import vertx.cwmp.CwmpMessageTypeEnum;
import vertx.cwmp.CwmpUtils;
import vertx.model.CpeDeviceOp;
import vertx.model.CWMPTr098ModelExtensions;
import dslforumOrgCwmp12.AddObjectDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

/**
 * Project:  ccng-acs
 *
 * This class defines methods for AddObject Operations.
 *
 * @author: ronyang
 */
public class AddObject {
    private static final Logger log = LoggerFactory.getLogger(AddObject.class.getName());
    
    public static final String ADD_OBJECT_MESSAGE_NAME = CwmpMessageTypeEnum.ADD_OBJECT.typeString;

    /**
     * Create the request and add it to the queue using a custom response handler.
     *
     * @param session
     * @param deviceOp
     * @
     * @throws vertx.cwmp.CwmpException
     */
    public static void start(JsonObject deviceOp, CwmpSession session)
            throws CwmpException {
        String objectName = deviceOp.getString(CpeDeviceOp.FIELD_NAME_OBJECT_NAME);
        log.info("Adding Object " + objectName + " for CPE " + session.cpe.toString());

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                CwmpRequest.CWMP_REQUESTER_ACS,
                buildAddObjectMessage(objectName, session),
                ADD_OBJECT_MESSAGE_NAME,
                new AddObjectResponseHandler(deviceOp, objectName)
            )
        );
    }

    /**
     * Create a new AddObject Request with objectPath and custom handler.
     * 
     * @param objectName
     * @param session
     * @param handler
     */
    public static void start(String objectName, CwmpSession session, CwmpRequest.Handlers handler) {
        log.info("Adding Object " + objectName + " for CPE " + session.cpe.toString());

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                        CwmpRequest.CWMP_REQUESTER_ACS,
                        buildAddObjectMessage(objectName, session),
                        ADD_OBJECT_MESSAGE_NAME,
                        handler
                )
        );
    }

    /**
     * Build a new ADD_OBJECT_MESSAGE_NAME Message
     */
    public static CwmpMessage buildAddObjectMessage(String objectName, CwmpSession session){
        CwmpMessage addObjectMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        addObjectMessage.rpcMessageName = ADD_OBJECT_MESSAGE_NAME;
        AddObjectDocument.AddObject addObject =
                addObjectMessage.soapEnv.getBody().addNewAddObject();

        // Insert the provided parameter names
        if (CWMPTr098ModelExtensions.containCWMPAbstractName(objectName)) {
            objectName = CWMPTr098ModelExtensions.convertCWMPAbstractNameToActualName(session.cpe, objectName);
        }
        addObject.setObjectName(objectName);
        addObject.setParameterKey(CwmpUtils.getParameterKey());
        return addObjectMessage;
    }

    /**
     * Response handler for the ADD_OBJECT_MESSAGE_NAME request (for retrieving the device type)
     */
    public static class AddObjectResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        String objectName;

        /**
         * Constructor that takes the object name.
         *
         * @param deviceOp
         */
        public AddObjectResponseHandler(JsonObject deviceOp, String objectName) {
            this.deviceOp = deviceOp;
            this.objectName = objectName;
        }

        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            int responseStatus = responseMessage.soapEnv.getBody().getAddObjectResponse().getStatus();
            long instanceNumber = responseMessage.soapEnv.getBody().getAddObjectResponse().getInstanceNumber();

            String objectCreated = objectName + instanceNumber;
            if (CWMPTr098ModelExtensions.containCWMPAbstractName(objectCreated)) {
                objectCreated = CWMPTr098ModelExtensions.convertCWMPAbstractNameToActualName(
                        session.cpe, objectCreated);
            }
            if (responseStatus == 0) {
                log.info("The Object " + objectCreated + " has been created.");
            } else {
                log.info("The Object " + objectCreated
                        + " creation has been validated and committed, but not yet applied. " +
                        "(for example, if a reboot is required before the new Object can be applied).");

                /**
                 * TODO: Handle this use case.
                 */
            }

            /**
             * send the result to the provided callback URL if any
             */
            DeviceOpUtils.callback(
                    session,
                    deviceOp,
                    CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                    new JsonObject().putNumber(CpeDeviceOp.FIELD_NAME_NEW_OBJECT_INDEX, instanceNumber)
            );
        }
    }
}
