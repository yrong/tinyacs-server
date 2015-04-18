package com.calix.sxa.cc.cpeserver.deviceop;

import com.calix.sxa.cc.cpeserver.session.CwmpRequest;
import com.calix.sxa.cc.cpeserver.session.CwmpSession;
import com.calix.sxa.cc.cwmp.CwmpException;
import com.calix.sxa.cc.cwmp.CwmpFaultCodes;
import com.calix.sxa.cc.cwmp.CwmpMessage;
import com.calix.sxa.cc.cwmp.CwmpMessageTypeEnum;
import dslforumOrgCwmp12.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  ccng-acs
 *
 * This class defines methods for getting parameter values from a CPE, which is needed
 * during CPE auto discovery and probably when we want to refresh some or all parameters of a CPE.
 *
 * @author: jqin
 */
public class GetParameterValues {
    private static final Logger log = LoggerFactory.getLogger(GetParameterValues.class.getName());

    /**
     * A static/final instance of the Default response handler
     */
    public static final GetParameterValuesResponseHandler defaultHandler =
            new GetParameterValuesResponseHandler();

    /**
     * Create the request and add it to the queue using a custom response handler
     * @param session
     */
    public static void start(CwmpSession session,
                             ParameterNames parameterNames,
                             CwmpRequest.Handlers responseHandler,
                             String requester) {
        /**
         * Build a new "GetParameterValues" Message
         */
        CwmpMessage getParameterValuesMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        getParameterValuesMessage.rpcMessageName = "GetParameterValues";
        GetParameterValuesDocument.GetParameterValues getParameterValues =
                getParameterValuesMessage.soapEnv.getBody().addNewGetParameterValues();
        if (parameterNames == null) {
            // Create default parameter names which is based on the root object name
            ParameterNames paramNames = getParameterValues.addNewParameterNames();
            paramNames.addString(session.cpe.rootObjectName + ".");
            paramNames.setArrayType("xsd:string[1]");
        } else {
            // Insert the provided parameter names
            parameterNames.setArrayType("xsd:string[" + parameterNames.sizeOfStringArray() + "]");
            getParameterValues.setParameterNames(parameterNames);
        }

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                requester,
                getParameterValuesMessage,
                "GetParameterValues",
                responseHandler)
        );
    }

    /**
     * Create a request to get all parameter values using the default response handler.
     * @param session
     */
    public static void start(CwmpSession session)  {
        start(session, null, defaultHandler, CwmpRequest.CWMP_REQUESTER_LOCAL);
    }

    /**
     * Create a request with a custom list of parameter names and a response handler.
     * @param session
     */
    public static void start(CwmpSession session, ParameterNames paramNames)  {
        start(session, paramNames, defaultHandler, CwmpRequest.CWMP_REQUESTER_LOCAL);
    }

    /**
     * Create a request to get all parameter values using the default response handler.
     * @param session
     */
    public static void start(CwmpSession session, String requester)  {
        start(session, null, defaultHandler, requester);
    }

    /**
     * Create a request with a custom list of parameter names and a response handler.
     * @param session
     */
    public static void start(CwmpSession session, ParameterNames paramNames, String requester)  {
        start(session, paramNames, defaultHandler, requester);
    }

    /**
     * Response handler for the "GetParameterValues" request (for retrieving the device type)
     */
    public static class GetParameterValuesResponseHandler implements CwmpRequest.Handlers {
        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            /**
             * Validation
             */
            GetParameterValuesResponseDocument.GetParameterValuesResponse response =
                    responseMessage.soapEnv.getBody().getGetParameterValuesResponse();
            if (response == null || response.getParameterList() == null) {
                log.error("Null or Invalid GetParameterValuesResponse message!\n" +
                        responseMessage.soapEnv.getBody().xmlText());
                throw new CwmpException(
                        "Null or Invalid GetParameterValuesResponse message",
                        CwmpFaultCodes.ACS_INVALID_ARGS,
                        responseMessage.cwmpVersion
                );
            }

            /**
             * Store the learned parameter name/value pairs to the CPE object
             */
            //log.debug(session.cpeKey + ": Processing the learned parameter name/value pairs.");
            for (ParameterValueStruct valueStruct : response.getParameterList().getParameterValueStructArray()) {
                session.cpe.addParamValueStructToSets(valueStruct, CwmpMessageTypeEnum.GET_PARAMETER_VALUES);
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
            log.error("Received fault response for GetParameterValues request!");
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
}
