package com.calix.sxa.cc.cpeserver.deviceop;

import com.calix.sxa.cc.cpeserver.session.CwmpRequest;
import com.calix.sxa.cc.cpeserver.session.CwmpSession;
import com.calix.sxa.cc.cwmp.CwmpException;
import com.calix.sxa.cc.cwmp.CwmpMessage;
import com.calix.sxa.cc.cwmp.CwmpMessageTypeEnum;
import com.calix.sxa.cc.cwmp.CwmpUtils;
import com.calix.sxa.cc.model.Cpe;
import com.calix.sxa.cc.model.SxaCcTr098ModelExtensions;
import com.calix.sxa.cc.util.CpeDataModelMgmt;
import dslforumOrgCwmp12.FaultDocument;
import dslforumOrgCwmp12.ParameterValueList;
import dslforumOrgCwmp12.ParameterValueStruct;
import dslforumOrgCwmp12.SetParameterValuesDocument;
import org.apache.xmlbeans.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  ccng-acs
 *
 * This class defines default methods for setting some or all objects and parameter values for a CPE.
 *
 * @author: jqin
 */
public class SetParameterValues {
    private static final Logger log = LoggerFactory.getLogger(SetParameterValues.class.getName());

    /**
     * A static/final instance of the Default response handler 
     */
    public static final SetParameterValuesResponseHandler defaultHandler = new SetParameterValuesResponseHandler();

    /**
     * Create the request with a custom parameter key and add it to the queue using a custom response handler.
     *
     * @param session
     * @param parameterList
     * @param responseHandler
     * @param requester
     * @param parameterKey
     *
     * @throws com.calix.sxa.cc.cwmp.CwmpException
     */
    public static void start(CwmpSession session,
                             ParameterValueList parameterList,
                             CwmpRequest.Handlers responseHandler,
                             String requester,
                             String parameterKey) {
        if (parameterList == null) {
            log.error("Null Pointer!");
            return;
        }

        log.info("Sending SetParameterValues for CPE " + session.cpe.getCpeKey());

        /**
         * Build a new "SetParameterValues" Message
         */
        CwmpMessage setParameterValuesMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        setParameterValuesMessage.rpcMessageName = "SetParameterValues";
        SetParameterValuesDocument.SetParameterValues setParameterValues =
                setParameterValuesMessage.soapEnv.getBody().addNewSetParameterValues();

        // Insert the provided parameter names
        parameterList.setArrayType("cwmp:ParameterValueStruct[" + parameterList.sizeOfParameterValueStructArray() +"]");
        setParameterValues.setParameterList(parameterList);
        setParameterValues.setParameterKey(parameterKey);

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                        requester,
                        setParameterValuesMessage,
                        "SetParameterValues",
                        responseHandler)
        );
    }

    /**
     * Create the request without a custom parameter keyand add it to the queue using a custom response handler.
     *
     * @param session
     * @param parameterList
     * @param responseHandler
     * @param requester
     * @
     * @throws com.calix.sxa.cc.cwmp.CwmpException
     */
    public static void start(CwmpSession session,
                             ParameterValueList parameterList,
                             CwmpRequest.Handlers responseHandler,
                             String requester) {
        start(session, parameterList, responseHandler, requester, CwmpUtils.getParameterKey());
    }

    /**
     * Create a request with a custom list of parameter names using the default response handler.
     * @param session
     */
    public static void start(CwmpSession session, ParameterValueList paramList)
            throws CwmpException {
        if (paramList == null) {
            throw new CwmpException("Null Pointer!");
        }
        start(session, paramList, defaultHandler, CwmpRequest.CWMP_REQUESTER_LOCAL);
    }

    /**
     * Response handler for the "SetParameterValues" request (for retrieving the device type)
     */
    public static class SetParameterValuesResponseHandler implements CwmpRequest.Handlers {
        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            int responseStatus = responseMessage.soapEnv.getBody().getSetParameterValuesResponse().getStatus();
            log.info("The SetParameterValues request has been processed successfully (status=" + responseStatus +
                    ") by  CPE " + session.cpe.toString());

            /**
             * Update the CPE parameter name/value pairs
             */
            ParameterValueList parameterValueList =
                    request.rpcMessage.soapEnv.getBody().getSetParameterValues().getParameterList();
            for (ParameterValueStruct valueStruct : parameterValueList.getParameterValueStructArray()) {
                session.cpe.addParamValueStructToSets(valueStruct, CwmpMessageTypeEnum.SET_PARAMETER_VALUES);
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
            log.info("Received fault response for SetParameterValues request!");
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
     * Util to convert paramValues Object -> ParameterValuesList.
     *
     * @param paramValues
     * @param paramList
     * @param prefix
     * @return
     */
    public static ParameterValueList jsonObjToParameterValuesList(
            Cpe cpe,
            JsonObject paramValues,
            ParameterValueList paramList,
            String prefix) {
        broadbandForumOrgCwmpDatamodel14.Model cwmpDataModel = cpe.dataModel.cwmpDataModel;

        if (paramList == null) {
            paramList = ParameterValueList.Factory.newInstance();
        }

        for (String fieldName : paramValues.getFieldNames()) {
            if (paramValues.getField(fieldName) instanceof JsonObject) {
                // The field is a JSON Object, dig in
                jsonObjToParameterValuesList(
                        cpe,
                        paramValues.getObject(fieldName),
                        paramList,
                        prefix + fieldName + "."
                );
            } else {
                // The field is a parameter value or attribute
                /**
                 * Add a new ParameterValueStruct
                 */
                ParameterValueStruct valueStruct = paramList.addNewParameterValueStruct();

                // Check for SXA-CC Extensions in parameter names
                String paramName = prefix + fieldName;
                if (SxaCcTr098ModelExtensions.containSxaCcAbstractName(paramName)) {
                    paramName = SxaCcTr098ModelExtensions.convertSxaCcAbstractNameToActualName(cpe, paramName);
                }
                valueStruct.setName(paramName);

                // Sometime the values may also contain an abstracted name
                // for example the WAN-IP Interface used for IP-Ping and Trace-Route Diagnostics
                Object rawValue = paramValues.getField(fieldName);
                if (rawValue != null) {
                    String stringValue = paramValues.getField(fieldName).toString();
                    if (SxaCcTr098ModelExtensions.containSxaCcAbstractName(stringValue)) {
                        stringValue = SxaCcTr098ModelExtensions.convertSxaCcAbstractNameToActualName(cpe, stringValue);
                    }
                    valueStruct.addNewValue().setStringValue(stringValue);
                } else {
                    valueStruct.setValue(null);
                }
                /**
                 * Figure out the parameter's type based on data model
                 */
                if (paramName.contains("{i}")) {
                    paramName = paramName.replace("{i}", "1");
                }
                SchemaType schemaType = CpeDataModelMgmt.getParamSchemaType(
                        cwmpDataModel,
                        paramName
                );
                valueStruct.getValue().changeType(schemaType);
            }
        }

        return paramList;
    }
}
