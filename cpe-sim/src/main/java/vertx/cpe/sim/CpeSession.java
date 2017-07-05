package vertx.cpe.sim;

import broadbandForumOrgCwmpDatamodel14.ModelObject;
import broadbandForumOrgCwmpDatamodel14.ModelParameter;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.ext.mongo.MongoClient;
import vertx.VertxJsonUtils;
import vertx.VertxMongoUtils;
import vertx.cwmp.*;
import vertx.model.Cpe;
import vertx.model.CpeDeviceDataModel;
import vertx.util.CpeDataModelMgmt;
import vertx.util.HttpDigestAuthUtils;
import dslforumOrgCwmp12.*;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.internal.StringUtil;
import org.apache.http.auth.AUTH;
import org.apache.xmlbeans.XmlCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Set;

/**
 * Project:  cwmp-parent
 *
 * A CPE CWMP Session
 *
 * @author: ronyang
 */
public class CpeSession {
    private static final Logger log = LoggerFactory.getLogger(CpeSession.class.getName());

    /**
     * Vertx
     */
    Vertx vertx;
    MongoClient mongoClient;


    /**
     * CPE DB Record
     */
    public JsonObject cpeJsonObject;

    /**
     * CPE POJO
     */
    public Cpe cpe;

    /**
     * Request Payload (in JSON Format)
     *
     * Only applicable for "value-change".
     */
    JsonObject newValues;

    /**
     * CPE SN String
     */
    public String sn;

    /**
     * HTTP Client
     */
    public HttpClient httpClient;

    /**
     * ACS URL Path
     */
    public String acsUrlPath;

    /**
     * Message Id
     */
    public int messageId = 1;

    /**
     * Session Cookie
     */
    public String cookie = null;

    /**
     * TR Data Model
     */
    public CpeDeviceDataModel cpeDeviceDataModel = null;

    /**
     * Type of follow-up session
     */
    public String followUpSessionType = null;
    public long followUpSessionDelay;
    public String followUpSessionCmdKey= null;

    /**
     * Does the CPE Meta data need to be persisted?
     */
    public boolean bToBePersisted = false;
    public boolean bIsNew = false;

    public String eventCode;
    public String commandKey;

    /**
     * Constructor.
     *
     * @param vertx
     * @param queryResult
     * @param eventCode
     * @param newValues
     */
    public CpeSession(
            Vertx vertx,
            MongoClient mongoClient,
            JsonObject queryResult,
            String orgId,
            long snLong,
            final String eventCode,
            boolean bIsNew,
            JsonObject newValues,
            String commandKey) {
        this.vertx = vertx;
        this.mongoClient = mongoClient;
        this.bToBePersisted = bIsNew;
        this.bIsNew = bIsNew;
        this.newValues = newValues;
        this.eventCode = eventCode;
        this.commandKey = commandKey;
        this.sn = CpeSimUtils.snToHexString(snLong);
        this.cpeJsonObject = CpeSimUtils.getDefaultCpeDataObjectBySn(snLong, orgId, "000631");
        if (queryResult != null) {
            VertxJsonUtils.merge(cpeJsonObject, queryResult);
        }
        this.cpe = new Cpe(cpeJsonObject);

        // Save session in shared set
        CpeSimUtils.allSessions.add(this.sn);

        /**
         * Initialize Data model by device type/id information
         */
        cpeDeviceDataModel = CpeDataModelMgmt.findDataModelByDeviceType(CpeDataModelMgmt.calix800RsgDeviceType);

        // Extract ACS URL
        acsUrlPath = CpeSimConstants.ACS_URL_SUFFIX;

        // Build HTTP Client
        HttpClientOptions options = new HttpClientOptions().setDefaultHost(CpeSimConstants.ACS_HOST).setDefaultPort(CpeSimConstants.ACS_PORT);
        httpClient = vertx.createHttpClient(options);


        // Store Diag Results if any
        if (eventCode.equals(CwmpInformEventCodes.DIAGNOSTICS_COMPLETE)) {
            if (newValues != null) {
                for (String name : newValues.fieldNames()) {
                    cpe.setValue(name, newValues.getString(name));
                }
                bToBePersisted = true;
            }
        }

        // Build and Send Inform
        sendMessageToAcs(buildInform(eventCode));
    }

    /**
     * ACS Response Handler
     */
    public class AcsResponseHandler implements Handler<HttpClientResponse> {

        /**
         * CPE Session
         */
        public CpeSession session;

        /**
         * Constructor.
         *
         * @param session
         */
        public AcsResponseHandler(CpeSession session) {
            this.session = session;
        }

        /**
         * The Response is a SOAP Message
         *
         * @param resp
         */
        @Override
        public void handle(final HttpClientResponse resp) {
            final Buffer body = Buffer.buffer();

            resp.handler((data)-> {
                body.appendBuffer(data);
            });

            resp.endHandler((Void)->{
                if (resp.cookies() != null && resp.cookies().size() >= 1) {
                    cookie = "";
                    for (int i = 0; i < resp.cookies().size(); i ++) {
                        String aCookie = resp.cookies().get(i);
                        //log.info("Found a cookie: " + aCookie);
                        cookie = i > 0? " " : "" + cookie + aCookie;
                    }
                    //log.info("Final Cookie: " + cookie);
                }
                try {
                    if (body.length() > 0) {
                        stateMachine(new CwmpMessage(body.toString()));
                    } else {
                        if (resp.statusCode() == HttpResponseStatus.UNAUTHORIZED.code()) {
                            String authResp = null;
                            // Get Auth Challenge
                            String challenge = resp.headers().get(AUTH.WWW_AUTH);
                            if (challenge == null) {
                                // No challenge string. Username/Password are invalid
                                log.error("Invalid Username/Password!");

                                // Must Terminate Session
                                stateMachine(null);
                            } else if (challenge.startsWith("Digest ")) {
                                authResp = HttpDigestAuthUtils.getAuthResponse(
                                        challenge,
                                        "/" + CpeSimConstants.ACS_URL_SUFFIX,
                                        "POST",
                                        CpeSimConstants.ACS_USERNAME,
                                        CpeSimConstants.ACS_PASSWORD
                                );
                            } else if (challenge.startsWith("Basic ")) {
                                authResp = CpeSimUtils.getBasicAuthString();
                            }

                            // Build and Send another Inform with Authorization Response
                            if (authResp != null) {
                                log.info("Sending auth response " + authResp);
                                sendMessageToAcs(buildInform(eventCode), authResp);
                            }
                        } else {
                            stateMachine(null);
                        }
                    }
                } catch (CwmpException e) {
                    e.printStackTrace();
                    log.error("Raw SOAP Message:\n" + body.toString());
                }
            });

            // Final Data Buffer Handler
//            resp.endHandler(new Handler<Void>() {
//                public void handle() {
//                    // The entire response body has been received
//
//                    // Parse all Cookie(s)
//                    if (resp.cookies() != null && resp.cookies().size() >= 1) {
//                        cookie = "";
//                        for (int i = 0; i < resp.cookies().size(); i ++) {
//                            String aCookie = resp.cookies().get(i);
//                            //log.info("Found a cookie: " + aCookie);
//                            cookie = i > 0? " " : "" + cookie + aCookie;
//                        }
//                        //log.info("Final Cookie: " + cookie);
//                    }
//
//                    // Drive the state machine with this response
//                    try {
//                        if (body.length() > 0) {
//                            stateMachine(new CwmpMessage(body.toString()));
//                        } else {
//                            if (resp.statusCode() == HttpResponseStatus.UNAUTHORIZED.code()) {
//                                String authResp = null;
//                                // Get Auth Challenge
//                                String challenge = resp.headers().get(AUTH.WWW_AUTH);
//                                if (challenge == null) {
//                                    // No challenge string. Username/Password are invalid
//                                    log.error("Invalid Username/Password!");
//
//                                    // Must Terminate Session
//                                    stateMachine(null);
//                                } else if (challenge.startsWith("Digest ")) {
//                                    authResp = HttpDigestAuthUtils.getAuthResponse(
//                                            challenge,
//                                            "/" + CpeSimConstants.ACS_URL_SUFFIX,
//                                            "POST",
//                                            CpeSimConstants.ACS_USERNAME,
//                                            CpeSimConstants.ACS_PASSWORD
//                                    );
//                                } else if (challenge.startsWith("Basic ")) {
//                                    authResp = CpeSimUtils.getBasicAuthString();
//                                }
//
//                                // Build and Send another Inform with Authorization Response
//                                if (authResp != null) {
//                                    log.info("Sending auth response " + authResp);
//                                    sendMessageToAcs(buildInform(eventCode), authResp);
//                                }
//                            } else {
//                                stateMachine(null);
//                            }
//                        }
//                    } catch (CwmpException e) {
//                        e.printStackTrace();
//                        log.error("Raw SOAP Message:\n" + body.toString());
//                    }
//                }
//            });
        }
    }

    /**
     * CPE Session State Machine
     *
     * @param acsMessage
     */
    public void stateMachine(CwmpMessage acsMessage) {
        if (acsMessage == null || acsMessage.rpcMessageName.equals("Fault")) {
            // Terminate this session
            log.info("CPE Session " + cpe.getCpeKey() + " has been terminated.");
            CpeSimUtils.allSessions.remove(sn);

            // Persist changes if needed
            if (bToBePersisted) {
                log.info("Persisting CPE meta data...");
                CpeSimUtils.persistCpe(
                        mongoClient,
                        cpe.getCpeKey(),
                        VertxMongoUtils.getUpdatesObject(cpe.sets, cpe.unsets, null, null, null),
                        false
                );
            }

            // Close the HTTP Client
            httpClient.close();

            // Follow-up session?
            if (followUpSessionType != null) {
                vertx.setTimer(followUpSessionDelay, new Handler<Long>() {
                    @Override
                    public void handle(Long event) {
                        log.info("Starting a scheduled followup CWMP Session for CPE " + cpe.getCpeKey());
                        JsonObject message = new JsonObject()
                                .put("orgId", cpeJsonObject.getString("orgId"))
                                .put("sn", Long.decode("0x" + sn))
                                .put("eventCode", followUpSessionType)
                                .put("commandKey", followUpSessionCmdKey);

                        // Start a new session by sending an event to one of the session vertices
                        vertx.eventBus().send(CpeSimConstants.VERTX_ADDRESS_NEW_SESSION, message);
                    }
                });
            }
        } else if (acsMessage.rpcMessageName.equals("InformResponse")) {
            if (CwmpInformEventCodes.TRANSFER_COMPLETE.equals(eventCode) ||
                    CwmpInformEventCodes.M_DOWNLOAD.equals(eventCode)) {
                // Send Transfer Complete
                sendMessageToAcs(buildTransferComplete());
            } else {
                // Send Empty POST
                sendMessageToAcs(null);
            }
        } else if (acsMessage.rpcMessageName.equals("TransferCompleteResponse")) {
            // Send Empty POST
            sendMessageToAcs(null);
        } else {
            acsMessage.id = acsMessage.id==null||acsMessage.id==""?"1234":acsMessage.id;
            if (acsMessage.rpcMessageName.equals("GetParameterValues")) {
                sendMessageToAcs(handleGetParameterValues(acsMessage));
            } else if (acsMessage.rpcMessageName.equals("SetParameterValues")) {
                sendMessageToAcs(handleSetParameterValues(acsMessage));
            } else if (acsMessage.rpcMessageName.equals("AddObject")) {
                sendMessageToAcs(handleAddObject(acsMessage));
            } else if (acsMessage.rpcMessageName.equals("DeleteObject")) {
                sendMessageToAcs(handleDeleteObject(acsMessage));
            } else if (acsMessage.rpcMessageName.equals("SetParameterAttributes")) {
                sendMessageToAcs(handleSetParameterAttributes(acsMessage));
            } else if (acsMessage.rpcMessageName.equals("GetParameterAttributes")) {
                sendMessageToAcs(handleGetParameterAttributes(acsMessage));
            } else if (acsMessage.rpcMessageName.equals("Reboot")) {
                sendMessageToAcs(handleReboot(acsMessage));
            } else if (acsMessage.rpcMessageName.equals("Download")) {
                sendMessageToAcs(handleDownload(acsMessage));
            } else if (acsMessage.rpcMessageName.equals("FactoryReset")) {
                sendMessageToAcs(handleFactoryReset(acsMessage));
            } else {
                sendMessageToAcs(
                        CwmpUtils.getFaultMessage(
                                CwmpMessage.DEFAULT_CWMP_VERSION,
                                CwmpFaultCodes.INVALID_PARAMETER_NAME,
                                "Invalid RPC Method! (" + acsMessage.rpcMessageName + ")"
                        )
                );
            }
        }
    }

    /**
     * Send a CWMP Message to ACS Server without auth response
     * @param message
     */
    private void sendMessageToAcs(CwmpMessage message) {
        CpeSimUtils.sendHttpRequest(
                acsUrlPath,
                httpClient,
                HttpMethod.POST,
                cookie,
                null,
                message!=null? message.soapEnv.xmlText(CwmpMessage.SOAP_ENV_PRETTY_PRINT_XML_OPTIONS) : null,
                new AcsResponseHandler(this)
        );
    }

    /**
     * Send a CWMP Message to ACS Server
     * @param message
     */
    private void sendMessageToAcs(CwmpMessage message, String authResp) {
        CpeSimUtils.sendHttpRequest(
                acsUrlPath,
                httpClient,
                HttpMethod.POST,
                cookie,
                authResp,
                message!=null? message.soapEnv.xmlText(CwmpMessage.SOAP_ENV_PRETTY_PRINT_XML_OPTIONS) : null,
                new AcsResponseHandler(this)
        );
    }

    /**
     * Build an Inform Message.
     *
     * @param eventCode
     * @
     * @return
     */
    public CwmpMessage buildInform(String eventCode) {
        // Build a new CWMP Message
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, messageId++);
        cwmpMessage.rpcMessageName = "Inform";

        // Add Inform to the message
        InformDocument.Inform inform = cwmpMessage.soapEnv.getBody().addNewInform();

        // Device ID
        dslforumOrgCwmp12.DeviceIdStruct deviceIdStruct = inform.addNewDeviceId();
        deviceIdStruct.setManufacturer(getStringValueByPath("InternetGatewayDevice.DeviceInfo.Manufacturer"));
        deviceIdStruct.setOUI(getStringValueByPath("InternetGatewayDevice.DeviceInfo.ManufacturerOUI"));
        deviceIdStruct.setProductClass(getStringValueByPath("InternetGatewayDevice.DeviceInfo.ProductClass"));
        String fsan = getStringValueByPath("InternetGatewayDevice.DeviceInfo.SerialNumber");
        fsan = fsan.replace(fsan.substring(0, 4), "CXNK").substring(0, 12);
        deviceIdStruct.setSerialNumber(fsan);

        // Event Codes
        EventList eventList = inform.addNewEvent();
        eventList.addNewEventStruct().setEventCode(eventCode);
        if (eventCode.equals(CwmpInformEventCodes.M_DOWNLOAD)) {
            // Schedule an "M_BOOT" Inform
            scheduleFollowUpSession(CwmpInformEventCodes.BOOT, 10000, null);
        }

        // Max Envelope
        inform.setMaxEnvelopes(1);

        // Current Time
        inform.setCurrentTime(new XmlCalendar(new Date()));

        // Retry Count
        inform.setRetryCount(0);

        // Parameter List
        // Add parameters whose "forcedInform" is set to "true"
        ParameterValueList parameterValueList = inform.addNewParameterList();
        dslforumOrgCwmp12.ParameterValueStruct parameterValueStruct = parameterValueList.addNewParameterValueStruct();
        parameterValueStruct.setName("InternetGatewayDevice.DeviceInfo.HardwareVersion");
        parameterValueStruct.addNewValue().setStringValue(getStringValueByPath("InternetGatewayDevice.DeviceInfo.HardwareVersion"));
        parameterValueStruct = parameterValueList.addNewParameterValueStruct();
        parameterValueStruct.setName("InternetGatewayDevice.DeviceInfo.SoftwareVersion");
        parameterValueStruct.addNewValue().setStringValue(getStringValueByPath("InternetGatewayDevice.DeviceInfo.SoftwareVersion"));
        parameterValueStruct = parameterValueList.addNewParameterValueStruct();
        parameterValueStruct.setName("InternetGatewayDevice.DeviceInfo.X_000631_RegistrationId");
        //parameterValueStruct.addNewValue().setStringValue(getStringValueByPath("InternetGatewayDevice.DeviceInfo.X_000631_RegistrationId"));
        parameterValueStruct = parameterValueList.addNewParameterValueStruct();
        parameterValueStruct.setName("InternetGatewayDevice.ManagementServer.ConnectionRequestURL");
        parameterValueStruct.addNewValue().setStringValue(getStringValueByPath("InternetGatewayDevice.ManagementServer.ConnectionRequestURL"));
        parameterValueStruct = parameterValueList.addNewParameterValueStruct();
        parameterValueStruct.setName("InternetGatewayDevice.WANDevice.5.WANConnectionDevice.1.WANIPConnection.3.ExternalIPAddress");
        parameterValueStruct.addNewValue().setStringValue(CpeSimUtils.snToIpAddress(Long.decode("0x" + sn)));
        parameterValueStruct = parameterValueList.addNewParameterValueStruct();

        if (CwmpInformEventCodes.VALUE_CHANGE.equals(eventCode) && newValues != null) {
            log.info(newValues.encodePrettily());

            /**
             * Add new values (that just changed).
             */
            JsonObject sets = newValues.getJsonObject("$set");
            if (sets != null) {
                for (String fieldName : sets.fieldNames()) {
                    log.info(fieldName + " : " + sets.getValue(fieldName).toString() + ", notif: "
                            + cpe.getNotifAttr(fieldName));
                    if (cpe.getNotifAttr(fieldName) != CwmpNotificationValues.NOTIFICATION_OFF) {
                        parameterValueStruct = parameterValueList.addNewParameterValueStruct();
                        parameterValueStruct.setName(fieldName);
                        parameterValueStruct.addNewValue().setStringValue(getStringValueByPath(fieldName));
                    }
                }
            }
        }

        // Return the message
        return cwmpMessage;
    }

    /**
     * Build Transfer Complete Message
     */
    public CwmpMessage buildTransferComplete() {
        // Build a new CWMP Message
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, messageId++);
        cwmpMessage.rpcMessageName = "TransferComplete";

        TransferCompleteDocument.TransferComplete transferComplete =
                cwmpMessage.soapEnv.getBody().addNewTransferComplete();
        transferComplete.setCommandKey(commandKey);
        GregorianCalendar completeTime = new GregorianCalendar();
        transferComplete.setCompleteTime(completeTime);
        GregorianCalendar startTime = new GregorianCalendar();
        startTime.roll(Calendar.MINUTE, false);
        transferComplete.setStartTime(startTime);

        return cwmpMessage;
    }

    /**
     * Handle GetParameterValues Message.
     *
     * @param request
     */
    public CwmpMessage handleGetParameterValues(CwmpMessage request) {
        // Build a new CWMP Message
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, Integer.valueOf(request.id));
        cwmpMessage.rpcMessageName = "GetParameterValuesResponse";

        // Add GetParameterValuesResponse to the message
        GetParameterValuesResponseDocument.GetParameterValuesResponse response =
                cwmpMessage.soapEnv.getBody().addNewGetParameterValuesResponse();
        dslforumOrgCwmp12.ParameterValueList  parameterValueList = response.addNewParameterList();

        // Analyze the request
        GetParameterValuesDocument.GetParameterValues getParameterValues =
                request.soapEnv.getBody().getGetParameterValues();
        dslforumOrgCwmp12.ParameterNames parameterNames = getParameterValues.getParameterNames();

        // Traverse all parameter names
        for (String paramName : parameterNames.getStringArray()) {
            try {
                if (paramName.endsWith(".")) {
                    /**
                     * Partial Path
                     */
                    getParamValuesByPartialPath(paramName, parameterValueList);
                } else {
                    /**
                     * Complete Path
                     */
                    String value = getStringValueByPath(paramName);
                    if (value == null) {
                        return CwmpUtils.getFaultMessage(
                                CwmpMessage.DEFAULT_CWMP_VERSION,
                                CwmpFaultCodes.INVALID_PARAMETER_NAME,
                                paramName);
                    }
                    ParameterValueStruct parameterValueStruct = parameterValueList.addNewParameterValueStruct();
                    parameterValueStruct.setName(paramName);
                    parameterValueStruct.addNewValue().setStringValue(getStringValueByPath(paramName));
                }
            } catch (Exception ex) {
                // Return a CWMP Fault
                return CwmpUtils.getFaultMessage(
                        CwmpMessage.DEFAULT_CWMP_VERSION,
                        CwmpFaultCodes.INVALID_PARAMETER_NAME,
                        paramName);
            }
        }

        // Return the message
        return cwmpMessage;
    }

    /**
     * Handle SetParameterValues Message.
     *
     * @param request
     */
    public CwmpMessage handleSetParameterValues(CwmpMessage request) {
        // Diag Type
        String diagType = null;
        // Diag Args
        JsonObject diagArgs = new JsonObject();

        // Update local meta data (in memory)
        for (ParameterValueStruct parameterValueStruct :
                request.soapEnv.getBody().getSetParameterValues().getParameterList().getParameterValueStructArray()) {
            if (parameterValueStruct == null || parameterValueStruct.getName() == null)
                continue;

            try {
                String name = parameterValueStruct.getName();
                String value = parameterValueStruct.getValue().getStringValue();
                log.info("Set " + name + " with new value " + value);
                cpe.setValue(name, value);
                cpe.addSet(Cpe.DB_FIELD_NAME_PARAM_VALUES + "." + name, value);

                /**
                 * Is it a Diagnostics request?
                 */
                if (name.endsWith("DiagnosticsState")) {
                    diagType = name.substring(0, name.indexOf("DiagnosticsState") - 1);

                    // Check for un-supported diag types
                    switch (diagType) {
                        case "InternetGatewayDevice.IPPingDiagnostics":
                            break;

                        default:
                            return CwmpUtils.getFaultMessage(
                                    CwmpMessage.DEFAULT_CWMP_VERSION,
                                    CwmpFaultCodes.INVALID_PARAMETER_NAME,
                                    parameterValueStruct.getName());
                    }
                } else if (name.contains("Diagnostics.")) {
                    diagArgs.put(name.substring(name.indexOf("Diagnostics.") + 12), value);
                }
            } catch (Exception ex) {
                log.error("setValue() failed for " + parameterValueStruct.getName() + "! Exception: ");
                //ex.printStackTrace();

                // Return a CWMP Fault
                return CwmpUtils.getFaultMessage(
                        CwmpMessage.DEFAULT_CWMP_VERSION,
                        CwmpFaultCodes.INVALID_PARAMETER_NAME,
                        parameterValueStruct.getName());
            }
        }

        // Build a new CWMP Message
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, Integer.valueOf(request.id));
        cwmpMessage.rpcMessageName = "SetParameterValuesResponse";

        // Add SetParameterValuesResponse to the message
        SetParameterValuesResponseDocument.SetParameterValuesResponse response =
                cwmpMessage.soapEnv.getBody().addNewSetParameterValuesResponse();
        response.setStatus(0);

        // Mark this CPE as to be persisted
        bToBePersisted = true;

        // Kick off a diag process if needed
        if (diagType != null) {
            vertx.eventBus().send(CpeSimConstants.VERTX_ADDRESS_DIAG_REQUEST,
                    new JsonObject()
                            .put("cpe", cpeJsonObject)
                            .put("diagType", diagType)
                            .put("diagArgs", diagArgs)
            );
        }

        // Return the message
        return cwmpMessage;
    }

    /**
     * Handle Reboot Message.
     *
     * @param request
     */
    public CwmpMessage handleReboot(CwmpMessage request) {
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, Integer.valueOf(request.id));
        cwmpMessage.rpcMessageName = "RebootResponse";
        cwmpMessage.soapEnv.getBody().addNewRebootResponse();

        // Schedule an "M REBOOT" Inform
        scheduleFollowUpSession(CwmpInformEventCodes.M_REBOOT, 30000,
                request.soapEnv.getBody().getReboot().getCommandKey());
        return cwmpMessage;
    }

    /**
     * Handle Download Message.
     *
     * @param request
     */
    public CwmpMessage handleDownload(CwmpMessage request) {
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, Integer.valueOf(request.id));
        cwmpMessage.rpcMessageName = "DownloadResponse";
        DownloadResponseDocument.DownloadResponse downloadResponse = cwmpMessage.soapEnv.getBody().addNewDownloadResponse();
        downloadResponse.setStatus(1);
        downloadResponse.setStartTime(new GregorianCalendar());
        downloadResponse.setCompleteTime(new GregorianCalendar());

        // Schedule an "M_DOWNLOAD" Inform
        scheduleFollowUpSession(CwmpInformEventCodes.M_DOWNLOAD, 10000,
                request.soapEnv.getBody().getDownload().getCommandKey());
        return cwmpMessage;
    }

    /**
     * Handle FactoryReset Message.
     *
     * @param request
     */
    public CwmpMessage handleFactoryReset(CwmpMessage request) {
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, Integer.valueOf(request.id));
        cwmpMessage.rpcMessageName = "FactoryResetResponse";
        cwmpMessage.soapEnv.getBody().addNewFactoryResetResponse();

        // Schedule an "BOOT_STRAP" Inform
        scheduleFollowUpSession(CwmpInformEventCodes.BOOT_STRAP, 30000, null);
        return cwmpMessage;
    }

    /**
     * Handle GetParameterAttributes Message.
     *
     * @param request
     */
    public CwmpMessage handleGetParameterAttributes(CwmpMessage request) {
        // Build a new CWMP Message
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, Integer.valueOf(request.id));
        cwmpMessage.rpcMessageName = "GetParameterAttributesResponse";

        // Add GetParameterAttributesResponse to the message
        GetParameterAttributesResponseDocument.GetParameterAttributesResponse response =
                cwmpMessage.soapEnv.getBody().addNewGetParameterAttributesResponse();
        dslforumOrgCwmp12.ParameterAttributeList  parameterAttributeList = response.addNewParameterList();

        // Analyze the request
        GetParameterAttributesDocument.GetParameterAttributes parameterAttributes =
                request.soapEnv.getBody().getGetParameterAttributes();
        dslforumOrgCwmp12.ParameterNames parameterNames = parameterAttributes.getParameterNames();

        // Traverse all parameter names
        for (String paramName : parameterNames.getStringArray()) {
            try {
                if (paramName.endsWith(".")) {
                    /**
                     * Partial Path
                     */
                    getParamAttributesByPartialPath(paramName, parameterAttributeList);
                } else {
                    /**
                     * Complete Path
                     */
                    String value = getStringValueByPath(paramName);
                    if (value == null) {
                        return CwmpUtils.getFaultMessage(
                                CwmpMessage.DEFAULT_CWMP_VERSION,
                                CwmpFaultCodes.INVALID_PARAMETER_NAME,
                                paramName);
                    }
                    ParameterAttributeStruct parameterAttributeStruct =
                            parameterAttributeList.addNewParameterAttributeStruct();
                    parameterAttributeStruct.setName(paramName);
                    parameterAttributeStruct.setNotification(cpe.getNotifAttr(paramName));
                }
            } catch (Exception ex) {
                // Return a CWMP Fault
                return CwmpUtils.getFaultMessage(
                        CwmpMessage.DEFAULT_CWMP_VERSION,
                        CwmpFaultCodes.INVALID_PARAMETER_NAME,
                        paramName);
            }
        }

        // Return the message
        return cwmpMessage;
    }

    /**
     * Handle SetParameterAttribute Message.
     *
     * @param request
     */
    public CwmpMessage handleSetParameterAttributes(CwmpMessage request) {
        // Update local meta data (in memory)
        for (SetParameterAttributesStruct parameterAttrStruct :
                request.soapEnv.getBody().getSetParameterAttributes().getParameterList().getSetParameterAttributesStructArray()) {
            try {
                cpe.setNotifAttr(parameterAttrStruct.getName(), parameterAttrStruct.getNotification());
                cpe.addSet(Cpe.DB_FIELD_NAME_PARAM_ATTRIBUTES + "." + parameterAttrStruct.getName(),
                        parameterAttrStruct.getNotification());
            } catch (Exception ex) {
                log.error("setNotifAttr() failed for " + parameterAttrStruct.getName() + "!");

                // Return a CWMP Fault
                return CwmpUtils.getFaultMessage(
                        CwmpMessage.DEFAULT_CWMP_VERSION,
                        CwmpFaultCodes.INVALID_PARAMETER_NAME,
                        parameterAttrStruct.getName());
            }
        }

        // Build a new CWMP Message
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, Integer.valueOf(request.id));
        cwmpMessage.rpcMessageName = "SetParameterAttributesResponse";

        // Add SetParameterValuesResponse to the message
        SetParameterAttributesResponseDocument.SetParameterAttributesResponse response =
                cwmpMessage.soapEnv.getBody().addNewSetParameterAttributesResponse();

        // Mark this CPE as to be persisted
        bToBePersisted = true;

        // Return the message
        return cwmpMessage;
    }

    /**
     * Handle AddObject Message.
     *
     * @param request
     */
    public CwmpMessage handleAddObject(CwmpMessage request) {
        String path = request.soapEnv.getBody().getAddObject().getObjectName();

        // Find the next available instance #
        int nextInstance = getNextAvailableObjectInstance(path);

        if (nextInstance > 0) {
            // Update local meta data

            // Get Model Object
            String objectPath = path + nextInstance + ".";
            String tmpObjPath = CpeDataModelMgmt.getParentObjNameByParameterName(objectPath + "dummy");
            log.info("Creating Object " + tmpObjPath + "...");
            ModelObject modelObject =
                    CpeDataModelMgmt.getObjectByObjName(cpeDeviceDataModel.cwmpDataModel,tmpObjPath);

            // Create all parameters defined in the CPE device data model
            JsonObject newObject = new JsonObject();
            for (int i = 0; i < modelObject.getParameterArray().length; i ++) {
                ModelParameter modelParameter = modelObject.getParameterArray()[i];

                // Determine default value by syntax/type
                String defaultValue = null;
                broadbandForumOrgCwmpDatamodel14.Syntax syntax = modelParameter.getSyntax();
                if (syntax.isSetBoolean()) {
                    defaultValue = "false";
                } else if (syntax.isSetInt() || syntax.isSetUnsignedInt() ||
                        syntax.isSetUnsignedLong() || syntax.isSetLong()) {
                    defaultValue = "1";
                } else if (syntax.isSetBase64() || syntax.isSetHexBinary()) {
                    // todo
                } else if (syntax.isSetDateTime()) {
                    defaultValue = new Date().toString();
                } else {
                    defaultValue = "(unknown)";
                }

                log.info("Creating Parameter " + objectPath + modelParameter.getName()
                        + " with default value " + defaultValue);
                newObject.put(modelParameter.getName(), defaultValue);
                cpe.addSet(Cpe.DB_FIELD_NAME_PARAM_VALUES + "." + modelParameter.getName(), defaultValue);
            }

            // Add the new object to its parent object
            JsonObject parentObject = cpe.getParamValueObject(path, true);
            parentObject.put(String.valueOf(nextInstance), newObject);

            // Build a new CWMP Message
            CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, Integer.valueOf(request.id));
            cwmpMessage.rpcMessageName = "AddObject";

            // Add AddObjectResponse to the message
            AddObjectResponseDocument.AddObjectResponse response = cwmpMessage.soapEnv.getBody().addNewAddObjectResponse();
            response.setStatus(0);
            response.setInstanceNumber(nextInstance);

            // Mark this CPE as to be persisted
            bToBePersisted = true;

            // Return the message
            return cwmpMessage;
        } else {
            // No more instance available, return fault
            return CwmpUtils.getFaultMessage(
                    CwmpMessage.DEFAULT_CWMP_VERSION,
                    CwmpFaultCodes.RESOURCE_EXCEEDED,
                    "No more instance available"
            );
        }
    }

    /**
     * Handle DeleteObject Message.
     *
     * @param request
     */
    public CwmpMessage handleDeleteObject(CwmpMessage request) {
        // Build a new CWMP Message
        CwmpMessage cwmpMessage = new CwmpMessage(CwmpMessage.DEFAULT_CWMP_VERSION, Integer.valueOf(request.id));
        cwmpMessage.rpcMessageName = "DeleteObject";

        // Split a parameter name into parent object name (end with a '.') and the child parameter name
        // within the parent object.
        String[] paths = cpe.splitParamPath(request.soapEnv.getBody().getDeleteObject().getObjectName());

        // Get the parent object of the target object
        JsonObject parentObj = cpe.getParamValueObject(paths[0], false);
        if (parentObj == null || parentObj.getJsonObject(paths[1]) == null) {
            if (parentObj == null) {
                log.error("Failed to find parent object " + paths[0] + "!");
            } else {
                log.error("Failed to find child object " + paths[0] +"." + paths[1] + "!\n"
                        + parentObj.encodePrettily());
            }

            return CwmpUtils.getFaultMessage(
                    CwmpMessage.DEFAULT_CWMP_VERSION,
                    CwmpFaultCodes.INVALID_PARAMETER_NAME,
                    "Invalid Object Name!"
            );
        }

        // Delete the target object from parent object
        parentObj.remove(paths[1]);
        cpe.addUnSet(Cpe.DB_FIELD_NAME_PARAM_VALUES + "." + request.soapEnv.getBody().getDeleteObject().getObjectName());


        // Add DeleteObjectResponse to the CWMP message
        DeleteObjectResponseDocument.DeleteObjectResponse response = cwmpMessage.soapEnv.getBody().addNewDeleteObjectResponse();
        response.setStatus(0);

        // Mark this CPE as to be persisted
        bToBePersisted = true;

        // Return the message
        return cwmpMessage;
    }

    /**
     * Get String value by deep json path (separated via ".")
     *
     * For example,
     *
     *     "InternetGatewayDevice" : {
     *       "DeviceInfo" : {
     *          "Manufacturer" : "Calix"
     *          }
     *       }
     *    }
     *
     *   Path "InternetGatewayDevice.DeviceInfo.Manufacturer" shall return "Calix".
     *
     * @param path
     * @return
     */
    public String getStringValueByPath(String path) {
        if (path.equals("InternetGatewayDevice.DeviceInfo.UpTime")) {
            return String.valueOf(CpeSimUtils.getUpTime());
        } else if (path.contains("Uptime")) {
            return String.valueOf(CpeSimUtils.getUpTime());
        } else if (path.endsWith(".ExternalIPAddress")) {
            return CpeSimUtils.snToIpAddress(Long.decode("0x" + sn));
        } else if (path.endsWith("SubnetMask")) {
            return CpeSimUtils.snToSubnetMask(Long.decode("0x" + sn));
        } else if (path.equals("InternetGatewayDevice.DeviceInfo.SerialNumber")) {
            return sn;
        } else if (path.equals("InternetGatewayDevice.ManagementServer.ConnectionRequestURL")) {
            return CpeSimUtils.getConnReqUrl(cpe.getCpeKey());
        } else {
            return cpe.getParamValue(path);
        }
    }

    /**
     * Get the instance # of the next Object.
     *
     * @param objectPath
     * @return
     */
    public int getNextAvailableObjectInstance(String objectPath) {
        String[] subPaths = objectPath.split("\\.");

        // Max Instance Index
        int maxIndex = CpeSimUtils.getMaxInstanceIndex(
                getStringValueByPath("InternetGatewayDevice.DeviceInfo.ModelName"), objectPath);
        if (maxIndex <= 0) {
            log.error("Unknown Object " + objectPath);
            return -1;
        }

        // Locate the parent object
        JsonObject parentObj = cpe.getParamValueObject(objectPath, false);
        if (parentObj == null) {
            log.error(objectPath + " : Failed to locate " + objectPath);
            return -1;
        }

        // Traverse all existing instances
        Set<String> allInstances = parentObj.fieldNames();

        /*
        log.info(parentObj.encodePrettily());
        for (String index : allInstances) {
            log.info("Has " + index);
        }
        */

        for (int i = 1; i <= maxIndex; i ++) {
            log.info("Checking instance " + i + "...");
            if (!allInstances.contains(String.valueOf(i))) {
                // Found it
                return i;
            }
        }

        // No more instance available
        log.error("No more instance available for " + objectPath + "!");
        return -1;
    }

    /**
     * Get Parameter values by partial path.
     *
     * @param partialPath
     * @param parameterValueList
     */
    public void getParamValuesByPartialPath(
            String partialPath,
            dslforumOrgCwmp12.ParameterValueList  parameterValueList) {
        JsonObject obj = cpe.getParamValueObject(partialPath, false); 
        getObjectValues(partialPath.substring(0, partialPath.length() - 1), obj, parameterValueList);
    }

    /**
     * Util Method to traverse a JSON Object (which may have child objects) and put its parameter value(s) into the
     * given CWMP parameterValueList.
     *
     * @param path
     * @param obj
     * @param parameterValueList
     */
    public void getObjectValues(
            String path,
            JsonObject obj,
            dslforumOrgCwmp12.ParameterValueList  parameterValueList) {
        // Traverse all sub field
        Set<String> fieldNames = obj.fieldNames();
        for (String fieldName : fieldNames) {
            if (obj.getValue(fieldName) instanceof JsonObject) {
                // Found another level of object
                getObjectValues(path + "." + fieldName, (JsonObject)obj.getValue(fieldName), parameterValueList);
            } else {
                // this is a parameter
                ParameterValueStruct parameterValueStruct = parameterValueList.addNewParameterValueStruct();
                parameterValueStruct.setName(path + "." + fieldName);
                parameterValueStruct.addNewValue().setStringValue(obj.getValue(fieldName).toString());
            }
        }
    }

    /**
     * Get Parameter Attributes by partial path.
     *
     * @param partialPath
     * @param parameterAttributeList
     */
    public void getParamAttributesByPartialPath(
            String partialPath,
            dslforumOrgCwmp12.ParameterAttributeList  parameterAttributeList) {
        JsonObject obj = cpe.getParamValueObject(partialPath, false);
        getObjectAttributes(partialPath.substring(0, partialPath.length() - 1), obj, parameterAttributeList);
    }

    /**
     * Util Method to traverse a JSON Object (which may have child objects) and put its parameter attribute(s) into the
     * given CWMP ParameterAttributeList.
     *
     * @param path
     * @param obj
     * @param parameterAttributeList
     */
    public void getObjectAttributes(
            String path,
            JsonObject obj,
            dslforumOrgCwmp12.ParameterAttributeList  parameterAttributeList) {
        // Traverse all sub field
        Set<String> fieldNames = obj.fieldNames();
        for (String fieldName : fieldNames) {
            if (obj.getValue(fieldName) instanceof JsonObject) {
                // Found another level of object
                getObjectAttributes(path + "." + fieldName, (JsonObject) obj.getValue(fieldName), parameterAttributeList);
            } else {
                // this is a parameter
                ParameterAttributeStruct attributeStruct = parameterAttributeList.addNewParameterAttributeStruct();
                attributeStruct.setName(path + "." + fieldName);
                attributeStruct.setNotification(obj.getInteger(fieldName).intValue());
            }
        }
    }

    /**
     * Schedule a follow-up session.
     *
     * @param eventCode
     * @param delay
     * @param commandKey
     */
    public void scheduleFollowUpSession(final String eventCode, long delay, final String commandKey) {
        followUpSessionType = eventCode;
        followUpSessionDelay = delay;
        followUpSessionCmdKey = commandKey;
    }
}
