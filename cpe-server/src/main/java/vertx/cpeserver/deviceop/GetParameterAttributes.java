package vertx.cpeserver.deviceop;

import vertx.cpeserver.session.CwmpRequest;
import vertx.cpeserver.session.CwmpSession;
import vertx.cwmp.CwmpException;
import vertx.cwmp.CwmpFaultCodes;
import vertx.cwmp.CwmpMessage;
import dslforumOrgCwmp12.FaultDocument;
import dslforumOrgCwmp12.GetParameterAttributesDocument;
import dslforumOrgCwmp12.GetParameterAttributesResponseDocument;
import dslforumOrgCwmp12.ParameterNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  ccng-acs
 *
 * This class defines methods for getting parameter values from a CPE, which is needed
 * during CPE auto discovery and probably when we want to refresh some or all parameters of a CPE.
 *
 * @author: ronyang
 */
public class GetParameterAttributes {
    private static final Logger log = LoggerFactory.getLogger(GetParameterAttributes.class.getName());

    /**
     * A static/final instance of the Default response handler
     */
    public static final GetParameterAttributesResponseHandler defaultHandler =
            new GetParameterAttributesResponseHandler();

    /**
     * Create the request and add it to the queue using a custom response handler
     * @param session
     */
    public static void start(CwmpSession session,
                             ParameterNames parameterNames,
                             CwmpRequest.Handlers responseHandler,
                             String requester) {
        /**
         * Build a new "GetParameterAttributes" Message
         */
        CwmpMessage getParameterAttributesMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        getParameterAttributesMessage.rpcMessageName = "GetParameterAttributes";
        GetParameterAttributesDocument.GetParameterAttributes getParameterAttributes =
                getParameterAttributesMessage.soapEnv.getBody().addNewGetParameterAttributes();
        if (parameterNames == null) {
            // Create default parameter names which is based on the root object name
            ParameterNames paramNames = getParameterAttributes.addNewParameterNames();
            paramNames.addString(session.cpe.rootObjectName + ".");
            paramNames.setArrayType("xsd:string[1]");
        } else {
            // Insert the provided parameter names
            parameterNames.setArrayType("xsd:string[" + parameterNames.sizeOfStringArray() + "]");
            getParameterAttributes.setParameterNames(parameterNames);
        }

        log.info("Enqueuing a GetParameterAttributes message for CPE " + session.cpe.toString() +
                " with " + parameterNames.sizeOfStringArray() + " parameter name(s)");

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                requester,
                getParameterAttributesMessage,
                "GetParameterAttributes",
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
     * Response handler for the "GetParameterAttributes" request (for retrieving the device type)
     */
    public static class GetParameterAttributesResponseHandler implements CwmpRequest.Handlers {
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
            GetParameterAttributesResponseDocument.GetParameterAttributesResponse response =
                    responseMessage.soapEnv.getBody().getGetParameterAttributesResponse();
            if (response == null || response.getParameterList() == null) {
                log.error("Null or Invalid GetParameterAttributesResponse message!\n" +
                        responseMessage.soapEnv.getBody().xmlText());
                throw new CwmpException(
                        "Null or Invalid GetParameterAttributesResponse message",
                        CwmpFaultCodes.ACS_INVALID_ARGS,
                        responseMessage.cwmpVersion
                );
            }

            /**
             * Store the learned parameter name/value pairs to the CPE object
             */
            log.info("Processing the learned parameter name/value pairs from CPE " + session.cpe.toString());
            session.cpe.storeParameterAttributeList(response.getParameterList());
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
            log.error("Received fault response for GetParameterAttributes request!");
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
