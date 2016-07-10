package vertx2.cpeserver.deviceop;

import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import vertx2.cpeserver.session.CwmpRequest;
import vertx2.cpeserver.session.CwmpSession;
import vertx2.model.Cpe;
import dslforumOrgCwmp12.FaultDocument;
import dslforumOrgCwmp12.SetParameterAttributesDocument;
import dslforumOrgCwmp12.SetParameterAttributesList;
import dslforumOrgCwmp12.SetParameterAttributesStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA CC CPE Server
 *
 * This class defines default methods for setting some or all parameter notification attribute for a CPE.
 *
 * @author: ronyang
 */
public class SetParameterAttributes {
    private static final Logger log = LoggerFactory.getLogger(SetParameterAttributes.class.getName());

    /**
     * A static/final instance of the Default response handler 
     */
    public static final SetParameterAttributesResponseHandler defaultHandler = new SetParameterAttributesResponseHandler();

    /**
     * Create the request and add it to the queue using a custom response handler
     * @param session
     */
    public static void start(CwmpSession session,
                             SetParameterAttributesList parameterList,
                             CwmpRequest.Handlers responseHandler,
                             String requester)
            throws CwmpException {
        if (parameterList == null) {
            throw new CwmpException("Null Pointer!");
        }

        log.info("Sending SetParameterAttributes for CPE " + session.cpe.toString());

        /**
         * Build a new "SetParameterAttributes" Message
         */
        CwmpMessage setParameterAttributesMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        setParameterAttributesMessage.rpcMessageName = "SetParameterAttributes";
        SetParameterAttributesDocument.SetParameterAttributes setParameterAttributes =
                setParameterAttributesMessage.soapEnv.getBody().addNewSetParameterAttributes();
        // Insert the provided parameter attributes
        parameterList.setArrayType(
                "cwmp:ParameterAttributeStruct[" + parameterList.sizeOfSetParameterAttributesStructArray() + "]");
        setParameterAttributes.setParameterList(parameterList);

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                requester,
                setParameterAttributesMessage,
                "SetParameterAttributes",
                responseHandler)
        );
    }

    /**
     * Create a request with a custom list of parameter names using the default response handler.
     * @param session
     */
    public static void start(CwmpSession session, SetParameterAttributesList paramList)
            throws CwmpException {
        if (paramList == null) {
            throw new CwmpException("Null Pointer!");
        }
        start(session, paramList, defaultHandler, CwmpRequest.CWMP_REQUESTER_LOCAL);
    }

    /**
     * Response handler for the "SetParameterAttributes" request (for retrieving the device type)
     */
    public static class SetParameterAttributesResponseHandler implements CwmpRequest.Handlers {
        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            log.info("The SetParameterAttributes request has been processed successfully by  CPE " +
                    session.cpe.toString());

            /**
             * Store the new notification value
             */
            SetParameterAttributesList parameterAttributesList =
                    request.rpcMessage.soapEnv.getBody().getSetParameterAttributes().getParameterList();
            for (SetParameterAttributesStruct valueStruct :
                    parameterAttributesList.getSetParameterAttributesStructArray()) {
                /*
                log.debug("CPE " + session.cpe.toString() + "New notification attributes " +
                        valueStruct.getNotification() + ":" + valueStruct.getNotificationChange() +
                        " have been applied to parameter " + valueStruct.getName());
                */

                /**
                 * Store the new value as the last read value from CPE (assuming CPE has applied properly)
                 */
                session.cpe.addSet(Cpe.DB_FIELD_NAME_PARAM_ATTRIBUTES + "." + valueStruct.getName(),
                        valueStruct.getNotification());
            }
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
            log.info("Received fault response for SetParameterAttributes request!");
            /**
             * TODO: add real error handling code here
             */
        }

        /**
         * Abstract Timeout Handler Class that must be extended by actual requests
         * <p/>
         * Currently the servlet is configured to timeout inactive HTTP sessions after 3 minutes.
         * <p/>
         * If the request has already been sent to the CPE but no response has been received when the session is being
         * timed out, this handler shall be called.
         *
         * @param session
         * @param request
         */
        @Override
        public void timeoutHandler(CwmpSession session, CwmpRequest request) throws CwmpException {
            /**
             * Do nothing for now
             */
        }

        /**
         * Abstract Drain Handler Class that must be extended by actual requests
         * <p/>
         * When a session is being timed out, all the requests that are still in the queue (i.e. have not been sent to
         * the CPE) will be drained out of the queue and this handler will be called for those requests being drained.
         *
         * @param session
         * @param request
         */
        @Override
        public void drainHandler(CwmpSession session, CwmpRequest request) throws CwmpException {
            /**
             * Do nothing for now
             */
        }
    }

    /**
     * Util to do the actual paramAttributes Object -> SetParameterAttributesList.
     * @param paramAttributes
     * @param paramList
     * @param prefix
     * @return
     */
    public static SetParameterAttributesList jsonObjToParameterAttributesList(
            JsonObject paramAttributes,
            SetParameterAttributesList paramList,
            String prefix) {
        if (paramList == null) {
            paramList = SetParameterAttributesList.Factory.newInstance();
        }

        for (String fieldName : paramAttributes.getFieldNames()) {
            if (paramAttributes.getField(fieldName) instanceof JsonObject) {
                // The field is a JSON Object, dig in
                jsonObjToParameterAttributesList(
                        paramAttributes.getObject(fieldName),
                        paramList,
                        prefix + fieldName + "."
                );
            } else {
                // The field is a parameter value or attribute
                /**
                 * Add a new ParameterValueStruct
                 */
                SetParameterAttributesStruct attrStruct = paramList.addNewSetParameterAttributesStruct();
                attrStruct.setName(prefix + fieldName);
                attrStruct.setNotificationChange(true);
                attrStruct.setNotification(Integer.valueOf(paramAttributes.getField(fieldName).toString()));
            }
        }

        return paramList;
    }
}
