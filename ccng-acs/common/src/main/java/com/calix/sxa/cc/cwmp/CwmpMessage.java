/*
 *
 * Copyright 2007-2012 Audrius Valunas
 *
 * This file is part of OpenACS.

 * OpenACS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * OpenACS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with OpenACS.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.calix.sxa.cc.cwmp;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.VertxMongoUtils;
import com.calix.sxa.cc.model.Cpe;
import com.calix.sxa.cc.util.AcsConfigProperties;
import com.calix.sxa.cc.util.AcsConstants;
import dslforumOrgCwmp12.*;
import dslforumOrgCwmp12.FaultDocument;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlObject;
import org.apache.xmlbeans.XmlOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.xmlsoap.schemas.soap.envelope.*;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract class to represent a CWMP Message
 */

public class CwmpMessage {
    private static final Logger log = LoggerFactory.getLogger(CwmpMessage.class.getName());

    /**
     * Name of the MongoDB Collection that stores all CWMP messages
     */
    public static final String DB_COLLECTION_NAME = "sxacc-cwmp-messages";

    /**
     * Message TTL by (in # of milliseconds)
     */
    public static final long TTL_ONE_DAY = 1000 * 3600 * 24;
    public static final long DEFAULT_TTL = AcsConfigProperties.CWMP_MESSAGE_TTL * TTL_ONE_DAY;

    /**
     * DB Field Names
     */
    public static final String DB_FIELD_NAME_TYPE = "type";
    public static final String DB_FIELD_NAME_TIMESTAMP = "timestamp";
    public static final String DB_FIELD_NAME_EXPIRE_AT = "expireAt";
    public static final String DB_FIELD_NAME_XML_TEXT = "xmlText";
    public static final String DB_FIELD_NAME_SUMMARY = "summary";
    public static final String DB_FIELD_NAME_SN = "sn";

    /**
     * Static Counter for sn
     */
    private static AtomicInteger nextSn = new AtomicInteger( 0 );

    /**
     * CWMP Version (1.0, 1.1, 1.2)
     */
    public CwmpVersionEnum cwmpVersion;

    /**
     * Default CWMP Version is 1.2 (we used ver 1.2 to generate Java classes)
     */
    public static final CwmpVersionEnum DEFAULT_CWMP_VERSION = CwmpVersionEnum.CWMP_VERSION_ENUM_1_2;
    public static final String DEFAULT_CWMP_VERSION_STRING = "urn:dslforum-org:cwmp-1-2";
    public static final String CWMP_VERSION_1_1_STRING = "urn:dslforum-org:cwmp-1-1";
    public static final String CWMP_VERSION_1_0_STRING = "urn:dslforum-org:cwmp-1-0";
    public static final String CWMP_VERSION_STRING_PREFIX = "urn:dslforum-org:cwmp-1-";
    public static final int CWMP_VERSION_STRING_PREFIX_LENGTH = CWMP_VERSION_STRING_PREFIX.length();
    /**
     * Static XmlOptions for parsing/building SOAP Envelope
     */
    public static final XmlOptions SOAP_ENV_XML_OPTIONS = InitSoapEnvXmlOptions();

    /**
     * Static XmlOptions for pretty printing SOAP Envelope
     */
    public static final XmlOptions SOAP_ENV_PRETTY_PRINT_XML_OPTIONS = InitSoapEnvPrettyPrintXmlOptions();

    /**
     * Static XmlOptions for regular printing SOAP Envelope
     */
    public static final XmlOptions SOAP_ENV_REGULAR_PRINT_XML_OPTIONS = InitSoapEnvPrintXmlOptions();

    /**
     * Message ID String
     */
    public String id = null;

    /**
     * SOAP Envelope
     */
    public Envelope soapEnv;

    /**
     * CWMP RPC Message Name
     */
    public String rpcMessageName = null;

    /**
     * XML Text String
     */
    public String rawXmlStringFromCpe = null;

    /**
     * Boolean Indicator to prevent same message being persisted multiple times
     */
    public boolean bPersisted = false;

    /**
     * Creates a new instance of Message by providing the raw XML String (received from CPE).
     */
    public CwmpMessage(String rawXmlString) throws CwmpException {
        // Save the Raw String
        rawXmlStringFromCpe = rawXmlString;

        /**
         * Check for CWMP Namespace/Version String
         */
        int versionStrIndex =  rawXmlStringFromCpe.indexOf(CWMP_VERSION_STRING_PREFIX);
        if (versionStrIndex < 0) {
            throw new CwmpException("No Valid CWMP Namespace String Found!", CwmpFaultCodes.ACS_REQUEST_DENIED);
        }
        switch (rawXmlStringFromCpe.charAt(versionStrIndex + CWMP_VERSION_STRING_PREFIX_LENGTH)) {
            case '0':
                cwmpVersion = CwmpVersionEnum.CWMP_VERSION_ENUM_1_0;
                //log.debug("CWMP Version: 1.0");
                break;

            case '1':
                cwmpVersion = CwmpVersionEnum.CWMP_VERSION_ENUM_1_1;
                //log.debug("CWMP Version: 1.1");
                break;

            case '2':
                cwmpVersion = CwmpVersionEnum.CWMP_VERSION_ENUM_1_2;
                //log.debug("CWMP Version: 1.2");
                break;

            default:
                throw new CwmpException("Invalid CWMP Namespace String Found!", CwmpFaultCodes.ACS_REQUEST_DENIED);
        }

        /**
         * Parse the SOAP Envelope
         */
        try {
            soapEnv = EnvelopeDocument.Factory.parse(rawXmlStringFromCpe, SOAP_ENV_XML_OPTIONS).getEnvelope();
        } catch (XmlException e) {
            log.error("Caught XmlException " + e.getError() +
                    " while processing CPE message. Raw XML Message String:\n" + rawXmlString);
            throw new CwmpException("Malformed Message!", CwmpFaultCodes.ACS_REQUEST_DENIED, cwmpVersion);
        }

        /**
         * Parse the SOAP Envelope Header and get ID
         */
        Header header = soapEnv.getHeader();
        if (header!= null) {
            IDDocument.ID idElement = header.getID();
            if (idElement != null) {
                id = idElement.getStringValue();
                //log.debug("CWMP Message ID: " + id);
            }
        }

        /**
         * Extract CWMP RPC Message Name
         */
        try {
            rpcMessageName = soapEnv.getBody().getDomNode().getFirstChild().getLocalName();
        } catch (Exception ex) {
            log.error("Malformed SOAP Envelope! (empty body)");
            throw new CwmpException("Malformed Message!", CwmpFaultCodes.ACS_REQUEST_DENIED, cwmpVersion);
        }

        /**
         * Debug print
         */
        if (log.isDebugEnabled()) {
            if (rawXmlString.length() < 4096 || soapEnv.getBody().isSetFault() || soapEnv.getBody().isSetInform()) {
                log.debug("Received Raw SOAP Envelope from CPE:\n" + toPrettyXmlText());
            } else {
                log.debug("Received a very long (> 4096 bytes) " + rpcMessageName + " message thus skip printing.");
            }
        }
    }

    /**
     * Creates a new instance of Message by providing CWMP Version string.
     *
     * This is likely to be called from ACS Executor Threads when sending a message to CPE.
     *
     * @param version
     */
    public CwmpMessage(CwmpVersionEnum version, int id) {
        /**
         * Save the version string
         */
        cwmpVersion = version;

        /**
         * Save the ID string for easier access
         */
        this.id = String.valueOf(id);

        /**
         * Save the SOAP Envelop
         */
        soapEnv = CwmpUtils.buildEnvelope(this.id);
    }

    /**
     * Creates a new instance of Message by providing CWMP Version string and a SOAP Envelop Object
     *
     * This is likely to be called from ACS Executor Threads when sending a message to CPE.
     *
     * @param version
     * @param envelope
     */
    public CwmpMessage(CwmpVersionEnum version, Envelope envelope) {
        cwmpVersion = version;

        /**
         * Save the SOAP Envelop
         */
        soapEnv = envelope;
    }

    /**
     * Initialize the common XML Options for SOAP Envelope Parsing/Building.
     */
    public static XmlOptions InitSoapEnvXmlOptions() {
        XmlOptions options = new XmlOptions();

        /**
         * Map for substitute namespace when parsing
         */
        HashMap<String, String> nsSubstituteIn = new HashMap<String, String>();
        nsSubstituteIn.put(CWMP_VERSION_1_0_STRING, DEFAULT_CWMP_VERSION_STRING);
        nsSubstituteIn.put(CWMP_VERSION_1_1_STRING, DEFAULT_CWMP_VERSION_STRING);
        options.setLoadSubstituteNamespaces(nsSubstituteIn);

        /**
         * If this option is set, all insignificant whitespace is stripped
         * when parsing a document.  Can be used to save memory on large
         * documents when you know there is no mixed content.
         */
        options.setLoadStripWhitespace();

        /**
         * Options for converting to XML
         */
        HashMap<String, String> nsPrefix = new HashMap<String, String>();
        nsPrefix.put(DEFAULT_CWMP_VERSION_STRING, "cwmp");
        //nsPrefix.put("http://www.w3.org/2001/XMLSchema", "xs");
        options.setSaveSuggestedPrefixes(nsPrefix);
        options.setUseDefaultNamespace();
        options.setSaveOuter();
        options.setSaveNamespacesFirst();
        options.setSaveAggressiveNamespaces();

        return options;
    }

    /**
     * Initialize the pretty print XML Options for SOAP Envelope debug prints.
     */
    public static XmlOptions InitSoapEnvPrettyPrintXmlOptions() {
        XmlOptions options = new XmlOptions();

        options.setLoadStripWhitespace();
        options.setSaveNamespacesFirst();

        /**
         * Options for converting to XML
         */
        HashMap<String, String> nsPrefix = new HashMap<String, String>();
        nsPrefix.put(DEFAULT_CWMP_VERSION_STRING, "cwmp");
        options.setSaveSuggestedPrefixes(nsPrefix);
        options.setSaveOuter();
        options.setSaveNamespacesFirst();
        options.setSaveAggressiveNamespaces();
        options.setSavePrettyPrint();

        return options;
    }

    /**
     * Initialize the regular print XML Options.
     */
    public static XmlOptions InitSoapEnvPrintXmlOptions() {
        XmlOptions options = new XmlOptions();

        options.setLoadStripWhitespace();
        options.setSaveNamespacesFirst();

        /**
         * Options for converting to XML
         */
        HashMap<String, String> nsPrefix = new HashMap<String, String>();
        nsPrefix.put(DEFAULT_CWMP_VERSION_STRING, "cwmp");
        options.setSaveSuggestedPrefixes(nsPrefix);
        options.setSaveOuter();
        options.setSaveNamespacesFirst();
        options.setSaveAggressiveNamespaces();

        return options;
    }

    /**
     * Convert the SOAP Envelope to a nice looking XML string.
     */
    public String toPrettyXmlText() {
        return toXmlText(SOAP_ENV_PRETTY_PRINT_XML_OPTIONS);
    }

    /**
     * Convert the SOAP Envelope to XML string.
     */
    public String toXmlText() {
        return toXmlText(SOAP_ENV_REGULAR_PRINT_XML_OPTIONS);
    }

    /**
     * Convert the SOAP Envelope to XML string.
     */
    public String toXmlText(XmlOptions options) {
        String xmlText = soapEnv.xmlText(options);

        // Match the CPE's CWMP version
        if (!cwmpVersion.equals(CwmpVersionEnum.CWMP_VERSION_ENUM_1_2)) {
            switch (cwmpVersion) {
                case CWMP_VERSION_ENUM_1_0:
                    log.debug("CWMP Version: " + CWMP_VERSION_1_0_STRING);
                    xmlText = xmlText.replaceFirst(DEFAULT_CWMP_VERSION_STRING, CWMP_VERSION_1_0_STRING);
                    break;

                case CWMP_VERSION_ENUM_1_1:
                    log.debug("CWMP Version: " + CWMP_VERSION_1_1_STRING);
                    xmlText = xmlText.replaceFirst(DEFAULT_CWMP_VERSION_STRING, CWMP_VERSION_1_1_STRING);
                    break;
            }
        }

        if (soapEnv.getBody().isSetSetParameterValues()) {
            return xmlText
                    .replace(
                            "<Value xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xsi:type=\"xs:",
                            "<Value xsi:type=\"xsd:")
                    .replace(
                            "<soapenv:Envelope",
                            "<soapenv:Envelope xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"");
        } else {
            return xmlText;
        }
    }

    /**
     * Persist this message into MongoDB.
     *
     * @param eventBus
     * @param cpe
     * @param informEventCodes
     */
    public void persist(EventBus eventBus, Cpe cpe, String[] informEventCodes) {
        if (bPersisted == true) {
            return;
        }

        if (cpe == null) {
            log.error("cpe is null!");
            return;
        }

        JsonObject jsonObject = new JsonObject()
                .putString(AcsConstants.FIELD_NAME_ORG_ID, cpe.orgId)
                .putObject(AcsConstants.FIELD_NAME_CPE_ID, cpe.getCpeIdentifier())
                .putObject(DB_FIELD_NAME_TIMESTAMP, VertxMongoUtils.getDateObject());

        // Continue the process based on message type
        Body body = soapEnv.getBody();
        JsonObject summary = null;
        String type = null;
        XmlObject xmlObject = null;
        if (body.isSetInform() || body.isSetInformResponse()) {
            /**
             * "Inform" or "InformResponse"
             */
            if (body.isSetInform()) {
                type = CwmpMessageTypeEnum.INFORM.typeString;
                xmlObject = body.getInform();
            } else {
                type = CwmpMessageTypeEnum.INFORM_RESPONSE.typeString;
                xmlObject = body.getInformResponse();
            }

            if (informEventCodes != null && informEventCodes.length > 0) {
                if (body.isSetInform()) {
                    summary = new JsonObject();
                }

                JsonArray eventCodes = new JsonArray();
                for (String eventCode : informEventCodes) {
                    if (body.isSetInform()) {
                        eventCodes.addString(eventCode);
                    }
                }

                if (body.isSetInform()) {
                    summary.putArray("eventCodes", eventCodes);
                }
            }
        } else if (body.isSetAddObject()) {
            /**
             * "AddObject"
             */
            type = AddObjectDocument.AddObject.class.getSimpleName();
            xmlObject = body.getAddObject();
            summary = new JsonObject().putString("ObjectName", body.getAddObject().getObjectName());
        } else if (body.isSetAddObjectResponse()) {
            /**
             * "AddObjectResponse"
             */
            type = AddObjectResponseDocument.AddObjectResponse.class.getSimpleName();
            xmlObject = body.getAddObjectResponse();
            summary = new JsonObject()
                    .putNumber("InstanceNumber", body.getAddObjectResponse().getInstanceNumber())
                    .putNumber("Status", body.getAddObjectResponse().getStatus());
        } else if (body.isSetDeleteObject()) {
            /**
             * "DeleteObject"
             */
            type = DeleteObjectDocument.DeleteObject.class.getSimpleName();
            xmlObject = body.getDeleteObject();
            summary = new JsonObject().putString("ObjectName", body.getDeleteObject().getObjectName());
        } else if (body.isSetDeleteObjectResponse()) {
            /**
             * "DeleteObjectResponse"
             */
            type = DeleteObjectResponseDocument.DeleteObjectResponse.class.getSimpleName();
            xmlObject = body.getDeleteObjectResponse();
            summary = new JsonObject().putNumber("Status", body.getDeleteObjectResponse().getStatus());
        } else if (body.isSetSetParameterValues()) {
            /**
             * "SetParameterValues"
             */
            type = SetParameterValuesDocument.SetParameterValues.class.getSimpleName();
            xmlObject = body.getSetParameterValues();

            /**
             * Save the list of parameter name/value pairs in summary
             */
            summary = new JsonObject();
            int nbrOfParams = body.getSetParameterValues().getParameterList().getParameterValueStructArray().length;
            ParameterValueStruct firstParameterValueStruct =
                    body.getSetParameterValues().getParameterList().getParameterValueStructArray(0);
            if (firstParameterValueStruct != null) {
                String value = firstParameterValueStruct.getValue().getStringValue();
                if (nbrOfParams > 1) {
                    value = value + " ( and " + (nbrOfParams - 1) + " more parameters)";
                    summary.putString(
                            firstParameterValueStruct.getName(),
                            value
                    );
                }
            }
            VertxJsonUtils.convertDotInFieldNames(summary, true);
        } else if (body.isSetSetParameterValuesResponse()) {
            /**
             * "SetParameterValuesResponse"
             */
            type = SetParameterValuesResponseDocument.SetParameterValuesResponse.class.getSimpleName();
            xmlObject = body.getSetParameterValuesResponse();
            summary = new JsonObject()
                    .putNumber("Status", body.getSetParameterValuesResponse().getStatus());
        } else if (body.isSetGetParameterValues()) {
            /**
             * "GetParameterValues"
             */
            type = GetParameterValuesDocument.GetParameterValues.class.getSimpleName();
            xmlObject = body.getGetParameterValues();

            /**
             * Save the list of parameter names in summary
             */
            String paramNames = "\"" + body.getGetParameterValues().getParameterNames().getStringArray(0) + "\"";
            if (body.getGetParameterValues().getParameterNames().sizeOfStringArray() > 1) {
                paramNames += " and " + (body.getGetParameterValues().getParameterNames().sizeOfStringArray() - 1)
                        + " more";
            }
            summary = new JsonObject().putString("Parameter Name(s)", paramNames);
        } else if (body.isSetGetParameterValuesResponse()) {
            /**
             * "GetParameterValuesResponse"
             */
            type = GetParameterValuesResponseDocument.GetParameterValuesResponse.class.getSimpleName();

            /**
             * Save the first Parameter Name/Value Pairs
             */
            ParameterValueList  parameterValueList = body.getGetParameterValuesResponse().getParameterList();
            if (parameterValueList != null) {
                ParameterValueStruct[]  parameterValueStructs = parameterValueList.getParameterValueStructArray();
                if (parameterValueStructs != null) {
                    int nbrOfParams = parameterValueStructs.length;
                    if (nbrOfParams > 0) {
                        ParameterValueStruct firstParameterValueStruct = parameterValueStructs[0];
                        if (firstParameterValueStruct != null) {
                            summary = new JsonObject();
                            String value = firstParameterValueStruct.getValue().getStringValue();
                            if (nbrOfParams > 1) {
                                value = value + " ( and " + (nbrOfParams - 1) + " more parameters)";
                                summary.putString(
                                        firstParameterValueStruct.getName(),
                                        value
                                );
                            }
                            VertxJsonUtils.convertDotInFieldNames(summary, true);
                        }
                    }
                }
            }

            xmlObject = body.getGetParameterValuesResponse();
        } else if (body.isSetSetParameterAttributes()) {
            /**
             * "SetParameterAttributes"
             */
            type = SetParameterAttributesDocument.SetParameterAttributes.class.getSimpleName();
            xmlObject = body.getSetParameterAttributes();
        } else if (body.isSetSetParameterAttributesResponse()) {
            /**
             * "SetParameterAttributesResponse"
             */
            type = SetParameterAttributesResponseDocument.SetParameterAttributesResponse.class.getSimpleName();
            xmlObject = body.getSetParameterAttributesResponse();
        } else if (body.isSetGetParameterAttributes()) {
            /**
             * "GetParameterAttributes"
             */
            type = GetParameterAttributesDocument.GetParameterAttributes.class.getSimpleName();
            xmlObject = body.getGetParameterAttributes();
        } else if (body.isSetGetParameterAttributesResponse()) {
            /**
             * "GetParameterAttributesResponse"
             */
            type = GetParameterAttributesResponseDocument.GetParameterAttributesResponse.class.getSimpleName();
            xmlObject = body.getGetParameterAttributesResponse();
        } else if (body.isSetDownload()) {
            /**
             * "Download"
             */
            type = DownloadDocument.Download.class.getSimpleName();
            xmlObject = body.getDownload();
            summary = new JsonObject()
                    .putString("FileType", body.getDownload().getFileType())
                    .putString("URL", body.getDownload().getURL());

            // Add Username?Password if any
            if (body.getDownload().getUsername() != null) {
                summary.putString("Username", body.getDownload().getUsername());
                if (body.getDownload().getPassword() != null) {
                    summary.putString("Password", body.getDownload().getPassword());
                }
            }
        } else if (body.isSetDownloadResponse()) {
            /**
             * "DownloadResponse"
             */
            type = DownloadResponseDocument.DownloadResponse.class.getSimpleName();
            xmlObject = body.getDownloadResponse();
            if (xmlObject != null) {
                summary = new JsonObject().putNumber("Status", body.getDownloadResponse().getStatus());
                if (body.getDownloadResponse().getStartTime() != null) {
                    summary.putString("StartTime", body.getDownloadResponse().getStartTime().toString());
                }
                if (body.getDownloadResponse().getCompleteTime() != null) {
                    summary.putString("CompleteTime", body.getDownloadResponse().getCompleteTime().toString());
                }
            }
        } else if (body.isSetUpload()) {
            /**
             * "Upload"
             */
            type = UploadDocument.Upload.class.getSimpleName();
            xmlObject = body.getUpload();
            summary = new JsonObject()
                    .putString("FileType", body.getUpload().getFileType())
                    .putString("URL", body.getUpload().getURL());
        } else if (body.isSetUploadResponse()) {
            /**
             * "UploadResponse"
             */
            type = UploadResponseDocument.UploadResponse.class.getSimpleName();
            xmlObject = body.getUploadResponse();
            summary = new JsonObject()
                    .putNumber("Status", body.getUploadResponse().getStatus())
                    .putString("StartTime", body.getUploadResponse().getStartTime().toString())
                    .putString("CompleteTime", body.getUploadResponse().getCompleteTime().toString());
        } else if (body.isSetReboot()) {
            /**
             * "Reboot"
             */
            type = RebootDocument.Reboot.class.getSimpleName();
            xmlObject = body.getReboot();
        } else if (body.isSetRebootResponse()) {
            /**
             * "RebootResponse"
             */
            type = RebootResponseDocument.RebootResponse.class.getSimpleName();
            xmlObject = body.getReboot();
        } else if (body.isSetFactoryReset()) {
            /**
             * "FactoryReset"
             */
            type = FactoryResetDocument.FactoryReset.class.getSimpleName();
            xmlObject = body.getFactoryReset();
        } else if (body.isSetFactoryResetResponse()) {
            /**
             * "FactoryResetResponse"
             */
            type = FactoryResetResponseDocument.FactoryResetResponse.class.getSimpleName();
            xmlObject = body.getFactoryResetResponse();
        } else if (body.isSetTransferComplete()) {
            /**
             * "TransferComplete"
             */
            type = TransferCompleteDocument.TransferComplete.class.getSimpleName();
            xmlObject = body.getTransferComplete();

            /**
             * Save FaultStruct/StartTime/CompleteTime
             */
            summary = new JsonObject();
            dslforumOrgCwmp12.TransferCompleteFaultStruct fault = body.getTransferComplete().getFaultStruct();
            if (fault != null && fault.getFaultCode() != 0) {
                // Save fault code/string
                summary.putNumber("FaultCode", fault.getFaultCode());
                summary.putString("FaultString", fault.getFaultString());
            }
            if (body.getTransferComplete().getStartTime() != null) {
                summary.putString("StartTime", body.getTransferComplete().getStartTime().toString());
            }
            if (body.getTransferComplete().getCompleteTime() != null) {
                summary.putString("CompleteTime", body.getTransferComplete().getCompleteTime().toString());
            }
        } else if (body.isSetTransferCompleteResponse()) {
            /**
             * "TransferCompleteResponse"
             */
            type = TransferCompleteResponseDocument.TransferCompleteResponse.class.getSimpleName();
            xmlObject = body.getTransferCompleteResponse();
        } else if (body.isSetAutonomousTransferComplete()) {
            /**
             * "AutonomousTransferComplete"
             */
            type = AutonomousTransferCompleteDocument.AutonomousTransferComplete.class.getSimpleName();
            xmlObject = body.getAutonomousTransferComplete();
        } else if (body.isSetAutonomousTransferCompleteResponse()) {
            /**
             * "AutonomousTransferCompleteResponse"
             */
            type = AutonomousTransferCompleteResponseDocument.AutonomousTransferCompleteResponse.class.getSimpleName();
            xmlObject = body.getAutonomousTransferCompleteResponse();
        } else if (body.isSetFault()) {
            /**
             * "Fault"
             */
            type = FaultDocument.Fault.class.getSimpleName();
            xmlObject = body.getFault();
            Detail detail = body.getFault().getDetail();
            if (detail != null) {
                FaultDocument.Fault fault = detail.getFault();
                summary = new JsonObject()
                        .putNumber("FaultCode", fault.getFaultCode())
                        .putString("FaultString", fault.getFaultString());

                if (fault.getSetParameterValuesFaultArray() != null) {
                    JsonArray setParameterValuesFaultArray = new JsonArray();
                    for (FaultDocument.Fault.SetParameterValuesFault setParameterValuesFault :
                            fault.getSetParameterValuesFaultArray()) {
                        // Add a new string value
                        setParameterValuesFaultArray.add(
                                setParameterValuesFault.getFaultString()
                                + " (code " + setParameterValuesFault.getFaultCode() + "), "
                                + "parameter name: " + setParameterValuesFault.getParameterName()
                        );
                    }
                    summary.putArray("setParameterValuesFaults", setParameterValuesFaultArray);
                }
            } else {
                summary = new JsonObject()
                        .putString("FaultCode", body.getFault().getFaultcode().toString())
                        .putString("FaultString", body.getFault().getFaultstring());
            }
        }

        // Finalize the JSON Object
        jsonObject.putString(DB_FIELD_NAME_TYPE, type);
        if (xmlObject != null) {
            jsonObject.putString(DB_FIELD_NAME_XML_TEXT, xmlObject.xmlText(SOAP_ENV_REGULAR_PRINT_XML_OPTIONS));
        }
        if (summary != null) {
            jsonObject.putObject(DB_FIELD_NAME_SUMMARY, summary);
        }
        jsonObject.putObject(
                DB_FIELD_NAME_EXPIRE_AT,
                VertxMongoUtils.getDateObject(System.currentTimeMillis() + DEFAULT_TTL)
        );
        jsonObject.putNumber(DB_FIELD_NAME_SN, nextSn.getAndIncrement());

        // Persist it
        //log.debug("Persisting a " + type + " ...");
        try {
            VertxMongoUtils.save(
                    eventBus,
                    DB_COLLECTION_NAME,
                    jsonObject,
                    null
            );
            bPersisted = true;
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }
    }
}