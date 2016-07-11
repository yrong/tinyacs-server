package vertx2.cpeserver.deviceop;

import vertx2.cpeserver.session.CwmpRequest;
import vertx2.cpeserver.session.CwmpSession;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import dslforumOrgCwmp12.ParameterValueList;
import dslforumOrgCwmp12.ParameterValueStruct;
import org.apache.xmlbeans.XmlString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project: cwmp
 *
 * Re-configure the ACS Credentials on Zero Touch.
 *
 * @author ronyang
 */
public class ZeroTouchActivation {
    private static final Logger log = LoggerFactory.getLogger(ZeroTouchActivation.class.getName());

    /**
     * Response Handler
     */
    public static final ResponseHandler DEFAULT_RESPONSE_HANDLER = new ResponseHandler();

    /**
     * Enqueue requests.
     *
     * @param session
     */
    public static void start(CwmpSession session, String acsUsername, String acsPassword) {
        ParameterValueList parameterValueList = ParameterValueList.Factory.newInstance();
        ParameterValueStruct valueStruct = parameterValueList.addNewParameterValueStruct();
        valueStruct.setName("InternetGatewayDevice.ManagementServer.Username");
        valueStruct.addNewValue().setStringValue(acsUsername);
        valueStruct.getValue().changeType(XmlString.type);
        valueStruct = parameterValueList.addNewParameterValueStruct();
        valueStruct.setName("InternetGatewayDevice.ManagementServer.Password");
        valueStruct.addNewValue().setStringValue(acsPassword);
        valueStruct.getValue().changeType(XmlString.type);

        /**
         * Build a new "SetParameterValues" Message to kick off the diag process
         */
        SetParameterValues.start(
                session,
                parameterValueList,
                DEFAULT_RESPONSE_HANDLER,
                CwmpRequest.CWMP_REQUESTER_LOCAL
        );
    }

    /**
     * Custom Response Handler that saves the Zero-Touch Event
     */
    public static class ResponseHandler extends SetParameterValues.SetParameterValuesResponseHandler {
        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            super.responseHandler(session, request, responseMessage);

            // Save Event
            /*
            Event.saveEvent(
                    session.eventBus,
                    session.cpe.orgId,
                    session.cpeKey,
                    EventTypeEnum.ZeroTouchActivation,
                    EventSourceEnum.CCFG,
                    null
            );
            */
        }
    }
}
