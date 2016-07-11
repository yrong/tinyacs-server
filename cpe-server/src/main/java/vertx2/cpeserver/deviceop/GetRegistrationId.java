package vertx2.cpeserver.deviceop;

import vertx2.cpeserver.session.CwmpRequest;
import vertx2.cpeserver.session.CwmpSession;
import vertx2.cwmp.CwmpException;
import vertx2.model.Cpe;
import dslforumOrgCwmp12.FaultDocument;
import dslforumOrgCwmp12.ParameterNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  cwmp-parent
 *
 * Get the CPE's Registration Id during discovery process.
 *
 * @author: ronyang
 */
public class GetRegistrationId extends GetParameterValues {
    private static final Logger log = LoggerFactory.getLogger(GetRegistrationId.class.getName());

    /**
     * Enqueue/Start the request.
     *
     * @param session
     */
    public static void start(CwmpSession session) {
        if (session.cpe.deviceId != null && "Calix".equals(session.cpe.deviceId.manufacturer)) {
            log.info(session.cpeKey + ": Getting CPE Registration Id...");

            ParameterNames paramNames = ParameterNames.Factory.newInstance();
            paramNames.addString("InternetGatewayDevice.DeviceInfo.X_000631_RegistrationId");

            start(session,
                    paramNames,
                    new ResponseHandler(session),
                    CwmpRequest.CWMP_REQUESTER_ACS
            );
        } else {
            log.info(session.cpeKey + ": Not a Calix Device.");
        }
    }

    /**
     * Custom Response Handler
     */
    public static class ResponseHandler extends GetParameterValuesResponseHandler {
        public CwmpSession session;

        /**
         * The deviceOps are passed in during the Construction.
         *
         * @param session
         */
        public ResponseHandler(CwmpSession session) {
            this.session = session;
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
             * Update DB record with null regId
             */
            if (session.cpe.deviceId.registrationId != null && session.cpe.deviceId.registrationId.length() > 0) {
                session.cpe.deviceId.registrationId = null;
                session.cpeRegIdChanged = true;
                session.cpe.addUnSet(Cpe.DB_FIELD_NAME_REGISTRATION_ID);
            }
        }
    }
}
