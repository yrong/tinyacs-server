package vertx2.cpeserver.deviceop;

import com.calix.sxa.VertxJsonUtils;
import vertx2.cpeserver.session.CwmpRequest;
import vertx2.cpeserver.session.CwmpSession;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import vertx2.model.CpeDeviceOp;
import vertx2.model.CpeDeviceOpTypeEnum;
import vertx2.util.AcsConstants;
import dslforumOrgCwmp12.GetParameterValuesResponseDocument;
import dslforumOrgCwmp12.ParameterNames;
import dslforumOrgCwmp12.ParameterValueList;
import dslforumOrgCwmp12.ParameterValueStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Project: SXA-CC
 *
 * This class defines methods for Diagnostics Operations.
 *
 * @author ronyang
 */
public class Diagnostics {
    private static final Logger log = LoggerFactory.getLogger(Diagnostics.class.getName());

    /**
     * Constants
     */
    public static final String DIAG_STATE = "DiagnosticsState";
    public static final String DIAG_STATE_REQUESTED = "Requested";
    public static final String DIAG_STATE_COMPLETED = "Complete";
    public static final String DIAG_STATE_NONE = "None";

    /**
     * Create the request and add it to the queue using a custom response handler.
     *
     * @param session
     * @param deviceOp
     *
     * @throws vertx2.cwmp.CwmpException
     */
    public static void start(JsonObject deviceOp, CwmpSession session)
            throws CwmpException {
        String diagType = deviceOp.getString(CpeDeviceOp.FIELD_NAME_DIAG_TYPE);

        // Build the parameter list struct
        JsonObject diagParams = deviceOp.getObject(CpeDeviceOp.FIELD_NAME_DIAG_PARAMS);
        // Set "DiagnosticsState" to "Requested" will tell the CPE to start the diagnostics process
        diagParams.putString(DIAG_STATE, DIAG_STATE_REQUESTED);
        ParameterValueList paramList = SetParameterValuesNbi.jsonObjToParameterValuesList(
                session.cpe,
                diagParams,
                null,
                diagType + "."
        );

        /**
         * Build a new "SetParameterValues" Message to kick off the diag process
         */
        SetParameterValues.start(
                session,
                paramList,
                new StartDiagResponseHandler(deviceOp),
                CwmpRequest.CWMP_REQUESTER_ACS,
                deviceOp.getString(CpeDeviceOp.FIELD_NAME_ID)
        );
    }

    /**
     * Collect Diagnostics Result by sending a "GetParameterValues" message
     * @param deviceOp
     * @param session
     * @throws CwmpException
     */
    public static void collectResult(JsonObject deviceOp, CwmpSession session) {
        // Build the parameter names struct with a partial path
        ParameterNames parameterNames = ParameterNames.Factory.newInstance();
        parameterNames.addString(deviceOp.getString(CpeDeviceOp.FIELD_NAME_DIAG_TYPE) + ".");

        // Start
        GetParameterValues.start(
                session,
                parameterNames,
                new DiagResultResponseHandler(deviceOp),
                CwmpRequest.CWMP_REQUESTER_ACS
        );
    }

    /**
     * Response handler for the "Diagnostics" request
     */
    public static class StartDiagResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {

        /**
         * Constructor
         *
         * @param deviceOp
         */
        public StartDiagResponseHandler(JsonObject deviceOp) {
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
             * Diagnostics Request has been accepted.
             */
            if (deviceOp.getBoolean(CpeDeviceOp.FIELD_NAME_ASYNCHRONOUS, false) == true) {
                // Asynchronous diag. Send response now
                DeviceOpUtils.callback(
                        session,
                        deviceOp,
                        CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                        null
                );
            } else {
                // Synchronous diag. Push back this device op as in-progress
                DeviceOpUtils.saveInProgressDeviceOp(session, deviceOp, CpeDeviceOpTypeEnum.Diagnostics);
            }
        }
    }

    /**
     * Response handler for collecting Diagnostics result
     */
    public static class DiagResultResponseHandler extends GetParameterValuesNbi.CpeGetParameterValuesNbiResponseHandler {

        /**
         * Constructor
         *
         * @param deviceOp
         */
        public DiagResultResponseHandler(JsonObject deviceOp) {
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
             * Traverse all parameter values in the response from the CPE
             */
            paramValues = new JsonObject();
            GetParameterValuesResponseDocument.GetParameterValuesResponse response =
                    responseMessage.soapEnv.getBody().getGetParameterValuesResponse();

            String diagStateParamName = deviceOp.getString(CpeDeviceOp.FIELD_NAME_DIAG_TYPE) + "." + DIAG_STATE;
            String diagState = null;
            for (ParameterValueStruct parameterValue : response.getParameterList().getParameterValueStructArray()) {
                String name = parameterValue.getName();
                if (parameterValue.getValue() != null) {
                    String value = parameterValue.getValue().getStringValue();
                    log.debug(name + " : " + value);
                    //parameterValueArray.add(new JsonObject().putString(name, value));
                    VertxJsonUtils.deepAdd(paramValues, name, value);

                    if (name.equals(diagStateParamName)) {
                        diagState = parameterValue.getValue().getStringValue();
                    }
                }
            }

            if (DIAG_STATE_COMPLETED.equals(diagState)) {
                /**
                 * Send Callback
                 */
                DeviceOpUtils.callback(
                        session,
                        deviceOp,
                        CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                        paramValues
                );
            } else if (diagState != null && diagState.startsWith("Error_")) {
                /**
                 * Send Callback with errors
                 */
                DeviceOpUtils.callback(
                        session,
                        deviceOp,
                        CpeDeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                        new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, diagState.substring(6))
                );
            } else {
                log.debug("Diag is still in progress...");
            }
        }
    }
}
