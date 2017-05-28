package vertx.cwmp;

import vertx.VertxUtils;
import vertx.model.CpeDeviceOp;
import dslforumOrgCwmp12.FaultDocument;
import dslforumOrgCwmp12.IDDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import org.xmlsoap.schemas.soap.envelope.Envelope;
import org.xmlsoap.schemas.soap.envelope.EnvelopeDocument;
import org.xmlsoap.schemas.soap.envelope.Header;

import javax.xml.namespace.QName;
import java.io.OutputStream;

/**
 * Project:  ccng-acs
 *
 * CWMP Servlet Utils.
 *
 * @author: ronyang
 */
public class CwmpUtils {
    private static final Logger log = LoggerFactory.getLogger(CwmpUtils.class.getName());

    /**
     * Send a SOAP Fault response to the CPE
     * @param faultCode
     * @param faultString
     * @param outputStream
     */
    public static void sendSoapFault(String faultCode, String faultString, OutputStream outputStream) {
        // Build a new SOAP Fault
    }

    /**
     * Build an empty Envelope with an ID String.
     *
     * @param idString
     * @return
     */
    public static Envelope buildEnvelope(String idString) {
        // Build the SOAP Envelope
        Envelope  soapEnv = EnvelopeDocument.Factory.newInstance().addNewEnvelope();

        // Create header
        Header header = soapEnv.addNewHeader();
        if (idString != null) {
            IDDocument.ID id = header.addNewID();
            id.setStringValue(idString);
            id.setMustUnderstand(true);
        }

        // Create an empty Body
        soapEnv.addNewBody();

        return soapEnv;
    }

    /**
     * Build an empty Envelope with an integer ID.
     *
     * @param id
     * @return
     */
    public static Envelope buildEnvelope(int id) {
        return buildEnvelope(String.valueOf(id));
    }

    /**
     * Build an empty Envelope with default ID.
     *
     * @return
     */
    public static Envelope buildEnvelope() {
        return buildEnvelope(String.valueOf(String.valueOf(System.currentTimeMillis())));
    }

    /**
     * Create a new SOAP Fault.
     *
     * @param cwmpVersion
     * @param faultCode
     * @param faultString
     * @return
     */
    public static CwmpMessage getFaultMessage(CwmpVersionEnum cwmpVersion, long faultCode, String faultString) {
        // Build the SOAP Envelope with ID string being a timestamp
        Envelope  soapEnv = buildEnvelope();

        // Generate the SOAP Fault
        org.xmlsoap.schemas.soap.envelope.Fault soapFault = soapEnv.getBody().addNewFault();
        soapFault.setFaultcode(new QName("", "faultCode"));
        soapFault.setFaultstring("CWMP fault");

        // Generate the CWMP Fault within the SOAP Fault
        FaultDocument.Fault cwmpFault = soapFault.addNewDetail().addNewFault();
        cwmpFault.setFaultCode(faultCode);
        cwmpFault.setFaultString(faultString);

        // Create/Return a new CWMP Message Object
        return new CwmpMessage(cwmpVersion, soapEnv);
    }

    /**
     * Generate a ParameterKey for AddObject/DeleteObject/SetParameterValues methods.
     */
    public static String getParameterKey() {
        /**
         * Let us use the system time plus hostname for now
         */
        return VertxUtils.getHostnameAndPid() + "-" + System.currentTimeMillis();
    }

    /**
     * Convert CWMP Fault to JsonObject.
     * @param cwmpFault
     */
    public static JsonObject cwmpFaultToJsonObject(FaultDocument.Fault cwmpFault) {
        String faultString = "Received TR-069/CWMP Fault from Device: "
                + cwmpFault.getFaultCode() + " " + cwmpFault.getFaultString();
        return new JsonObject().putString(CpeDeviceOp.FIELD_NAME_ERROR, faultString);
        /*
        JsonObject newFault = new JsonObject()
                .putNumber(CpeDeviceOp.FIELD_NAME_FAULT_CODE, cwmpFault.getFaultCode())
                .putString(CpeDeviceOp.FIELD_NAME_FAULT_MESSAGE, cwmpFault.getFaultString());
        FaultDocument.Fault.SetParameterValuesFault[] setParameterValuesFaults =
                cwmpFault.getSetParameterValuesFaultArray();
        if (setParameterValuesFaults != null && setParameterValuesFaults.length > 0) {
            JsonArray invalidParamNames = new JsonArray();
            for (FaultDocument.Fault.SetParameterValuesFault parameterValuesFault : setParameterValuesFaults) {
                invalidParamNames.add(parameterValuesFault.getParameterName());
            }
            newFault.putArray(CpeDeviceOp.FIELD_NAME_INVALID_PARAM_NAMES, invalidParamNames);
        }

        return new JsonObject()
                .putObject(
                        CpeDeviceOp.FIELD_NAME_FAULT,
                        new JsonObject()
                                .putNumber(CpeDeviceOp.FIELD_NAME_FAULT_CODE, cwmpFault.getFaultCode())
                                .putString(CpeDeviceOp.FIELD_NAME_FAULT_MESSAGE, cwmpFault.getFaultString())
                );
        */
    }

    /**
     * Get Object Index from a parameter name
     *
     * @param paramName
     * @return
     */
    public static int getIndexFromParamName(String paramName, String prefix) {
        String indexString = paramName.substring(
                prefix.length(),
                paramName.indexOf('.', prefix.length())
        );
        log.info(prefix + " indexString is " + indexString + " (learned from " + paramName + ")");
        return Integer.valueOf(indexString);
    }
}

