package vertx.cpeserver.deviceop;

import vertx.cpeserver.session.CwmpRequest;
import vertx.cpeserver.session.CwmpSession;
import vertx.cwmp.CwmpException;
import vertx.cwmp.CwmpMessage;
import vertx.model.Cpe;
import vertx.util.GigaCenter;
import dslforumOrgCwmp12.ParameterNames;
import dslforumOrgCwmp12.SetParameterAttributesList;
import dslforumOrgCwmp12.SetParameterAttributesStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project: cwmp
 *
 * Enable Notifications on the Change Counter.
 *
 * @author ronyang
 */
public class EnableNotifOnChangeCounter {
    private static final Logger log = LoggerFactory.getLogger(EnableNotifOnChangeCounter.class.getName());

    /**
     * Static Parameter Value/Attribute List that contains the default credentials
     */
    public static SetParameterAttributesList PARAM_ATTRIBUTE_LIST = initParamAttributeList();

    /**
     * Default Handler for Get Change Counter Value
     */
    public static GetChangeCounterValueHandler DEFAULT_GetChangeCounterValueHandler = new GetChangeCounterValueHandler();

    /**
     * Static List of Change Counter
     */
    public static ParameterNames CHANGE_COUNTER_PARAM_NAMES = initParamNames();

    /**
     * Initialize the Static Param Name list
     */
    private static ParameterNames initParamNames() {
        ParameterNames paramNames = ParameterNames.Factory.newInstance();
        paramNames.addString(GigaCenter.CHANGE_COUNTER);
        return paramNames;
    }

    /**
     * Initialize the Static Param Attribute list
     */
    private static SetParameterAttributesList initParamAttributeList() {
        SetParameterAttributesList  list = SetParameterAttributesList.Factory.newInstance();
        SetParameterAttributesStruct attributeStruct = list.addNewSetParameterAttributesStruct();
        attributeStruct.setName(GigaCenter.CHANGE_COUNTER);
        attributeStruct.setNotification(2); // "2" means active notification
        attributeStruct.setNotificationChange(true);
        return list;
    }

    /**
     * Enqueue requests.
     *
     * @param session
     */
    public static void start(CwmpSession session) {
        /**
         * Get the value of the change counter first (to see if change counter is supported or not)
         */
        GetParameterValues.start(
                session,
                CHANGE_COUNTER_PARAM_NAMES,
                DEFAULT_GetChangeCounterValueHandler,
                CwmpRequest.CWMP_REQUESTER_ACS
        );
    }

    public static class GetChangeCounterValueHandler extends GetParameterValues.GetParameterValuesResponseHandler {
        /**
         * Overwrite the Response Handler Class
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            super.responseHandler(session, request, responseMessage);

            if (session.cpe.sets.containsKey(Cpe.DB_FIELD_NAME_CHANGE_COUNTER)) {
                /**
                 * Build a new "SetParameterAttributes" Message to kick off the diag process
                 */
                SetParameterAttributes.start(session, PARAM_ATTRIBUTE_LIST);
            } else {
                log.info(session.cpeKey + " (sw version:" + session.cpe.deviceId.swVersion + ") does not support "
                        + GigaCenter.CHANGE_COUNTER + ".");
            }
        }
    }
}
