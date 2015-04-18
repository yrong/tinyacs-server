package com.calix.sxa.cc.cpeserver.deviceop;

import com.calix.sxa.cc.cpeserver.session.CwmpRequest;
import com.calix.sxa.cc.cpeserver.session.CwmpSession;
import com.calix.sxa.cc.cwmp.CwmpException;
import com.calix.sxa.cc.cwmp.CwmpMessage;
import com.calix.sxa.cc.model.Cpe;
import dslforumOrgCwmp12.GetParameterValuesResponseDocument;
import dslforumOrgCwmp12.ParameterNames;
import dslforumOrgCwmp12.ParameterValueStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  sxa-cc-parent
 *
 * Get the CPE's WAN MAC Address.
 *
 * @author: jqin
 */
public class GetWanConnectionParameters extends GetParameterValues {
    private static final Logger log = LoggerFactory.getLogger(GetWanConnectionParameters.class.getName());

    /**
     * Enqueue/Start the request.
     *
     * @param session
     */
    public static void start(CwmpSession session) {
        log.info(session.cpeKey + ": Getting WAN Connection Info...");
        ParameterNames paramNames = ParameterNames.Factory.newInstance();
        paramNames.addString(session.cpe.deviceId.wanIpConnectionPath + ".MACAddress");
        paramNames.addString(session.cpe.deviceId.wanIpConnectionPath + ".ExternalIPAddress");
        if (session.cpe.deviceId.wanIpConnectionPath != null &&
                session.cpe.deviceId.wanIpConnectionPath.contains("PPPConnection")) {
            paramNames.addString(session.cpe.deviceId.wanIpConnectionPath + ".RemoteIPAddress");
        } else {
            paramNames.addString(session.cpe.deviceId.wanIpConnectionPath + ".SubnetMask");
            paramNames.addString(session.cpe.deviceId.wanIpConnectionPath + ".DefaultGateway");
        }

        start(session,
                paramNames,
                new ResponseHandler(session),
                CwmpRequest.CWMP_REQUESTER_ACS
        );
    }

    /**
     * Custom Response Handler
     */
    public static class ResponseHandler extends GetParameterValues.GetParameterValuesResponseHandler {
        public CwmpSession session;
        public JsonObject paramValues;

        /**
         * The deviceOps are passed in during the Construction.
         *
         * @param session
         */
        public ResponseHandler(CwmpSession session) {
            this.session = session;
        }

        public ResponseHandler() {
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
             * Perform the default/standard actions
             */
            GetParameterValues.defaultHandler.responseHandler(session, request, responseMessage);

            /**
             * Extract the parameter values from the response message.
             */
            GetParameterValuesResponseDocument.GetParameterValuesResponse response =
                    responseMessage.soapEnv.getBody().getGetParameterValuesResponse();

            /**
             * Traverse all parameter values in the response from the CPE
             */
            if (response != null) {
                for (ParameterValueStruct parameterValue : response.getParameterList().getParameterValueStructArray()) {
                    String name = parameterValue.getName();
                    if (name.equals(session.cpe.deviceId.wanIpConnectionPath + ".MACAddress") &&
                        parameterValue.getValue() != null) {
                        session.cpe.deviceId.macAddress = parameterValue.getValue().getStringValue();
                        session.cpe.addSet(Cpe.DB_FIELD_NAME_MAC_ADDRESS, session.cpe.deviceId.macAddress);
                    } else if (name.equals(session.cpe.deviceId.wanIpConnectionPath + ".ExternalIPAddress") &&
                            parameterValue.getValue() != null) {
                        session.cpe.deviceId.ipAddress = parameterValue.getValue().getStringValue();
                        session.cpe.addSet(Cpe.DB_FIELD_NAME_IP_ADDRESS, session.cpe.deviceId.ipAddress);
                    }

                    if (session.cpe.deviceId.wanIpConnectionPath.contains("PPPConnection")) {
                        /**
                         * PPP
                         */
                        if (name.equals(session.cpe.deviceId.wanIpConnectionPath + ".RemoteIPAddress") &&
                                parameterValue.getValue() != null) {
                            session.cpe.addSet(Cpe.DB_FIELD_NAME_DEFAULT_GATEWAY, parameterValue.getValue().getStringValue());
                        }
                        session.cpe.addSet(Cpe.DB_FIELD_NAME_IP_SUBNET_MASK, "255.255.255.255");
                    } else {
                        /**
                         * DHCP/IP
                         */
                        if (name.equals(session.cpe.deviceId.wanIpConnectionPath + ".SubnetMask") &&
                            parameterValue.getValue() != null) {
                            session.cpe.addSet(Cpe.DB_FIELD_NAME_IP_SUBNET_MASK, parameterValue.getValue().getStringValue());
                        }
                        if (name.equals(session.cpe.deviceId.wanIpConnectionPath + ".DefaultGateway") &&
                            parameterValue.getValue() != null) {
                            session.cpe.addSet(Cpe.DB_FIELD_NAME_DEFAULT_GATEWAY, parameterValue.getValue().getStringValue());
                        }
                    }
                }
            }
        }
    }
}
