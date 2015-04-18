package com.calix.sxa.cc.cpeserver.deviceop;

import com.calix.sxa.cc.cwmp.CwmpException;
import com.calix.sxa.cc.cwmp.CwmpMessage;
import com.calix.sxa.cc.cwmp.CwmpMessageTypeEnum;
import com.calix.sxa.cc.cwmp.CwmpUtils;
import com.calix.sxa.cc.cpeserver.session.CwmpRequest;
import com.calix.sxa.cc.cpeserver.session.CwmpSession;
import com.calix.sxa.cc.model.CpeDeviceOp;
import com.calix.sxa.cc.model.SxaCcTr098ModelExtensions;
import dslforumOrgCwmp12.DeleteObjectDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  ccng-acs
 *
 * This class defines methods for DeleteObject Operations.
 *
 * @author: jqin
 */
public class DeleteObject {
    private static final Logger log = LoggerFactory.getLogger(DeleteObject.class.getName());

    public static final String DELETE_OBJECT_MESSAGE_NAME = CwmpMessageTypeEnum.DELETE_OBJECT.typeString;

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
        String objectName = deviceOp.getString("objectName");
        log.info(session.cpe + ": Deleting Object " + objectName + "...");

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                CwmpRequest.CWMP_REQUESTER_ACS,
                buildDeleteObjectMessage(objectName, session),
                DELETE_OBJECT_MESSAGE_NAME,
                new DeleteObjectResponseHandler(deviceOp)
            )
        );
    }

    /**
     * Create the request and add it to the queue using a custom response handler.
     *
     * @param session
     * @param objectName
     * @param handler
     */
    public static void start(String objectName, CwmpSession session, CwmpRequest.Handlers handler) {
        log.info(session.cpe + ": Deleting Object " + objectName + "...");

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                        CwmpRequest.CWMP_REQUESTER_ACS,
                        buildDeleteObjectMessage(objectName, session),
                        DELETE_OBJECT_MESSAGE_NAME,
                        handler
                )
        );
    }

    /**
     * Build a new "DeleteObject" Message
     */
    public static CwmpMessage buildDeleteObjectMessage(String objectName, CwmpSession session) {
        CwmpMessage deleteObjectMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        deleteObjectMessage.rpcMessageName = "DeleteObject";
        DeleteObjectDocument.DeleteObject deleteObject =
                deleteObjectMessage.soapEnv.getBody().addNewDeleteObject();

        // Insert the provided parameter names
        if (SxaCcTr098ModelExtensions.containSxaCcAbstractName(objectName)) {
            objectName = SxaCcTr098ModelExtensions.convertSxaCcAbstractNameToActualName(session.cpe, objectName);
        }
        deleteObject.setObjectName(objectName);
        deleteObject.setParameterKey(CwmpUtils.getParameterKey());

        return deleteObjectMessage;
    }
    /**
     * Response handler for the "DeleteObject" request (for retrieving the device type)
     */
    public static class DeleteObjectResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        /**
         * Constructor that takes the object name.
         *
         * @param deviceOp
         */
        public DeleteObjectResponseHandler(JsonObject deviceOp) {
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
            String objectName = deviceOp.getString("objectName");
            if (SxaCcTr098ModelExtensions.containSxaCcAbstractName(objectName)) {
                objectName = SxaCcTr098ModelExtensions.convertSxaCcAbstractNameToActualName(session.cpe, objectName);
            }
            int responseStatus = responseMessage.soapEnv.getBody().getDeleteObjectResponse().getStatus();
            log.info("The DeleteObject request has been processed successfully (status=" + responseStatus +
                    ") by  CPE " + session.cpe.toString());
            log.info("Object Name: " + objectName);

            /**
             * Delete the object from DB
             */
            session.cpe.addUnSet(objectName.substring(0, objectName.length() - 1));

            /**
             * Send the result to the provided callback URL if any
             */
            DeviceOpUtils.callback(
                    session,
                    deviceOp,
                    CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                    null);
        }
    }
}
