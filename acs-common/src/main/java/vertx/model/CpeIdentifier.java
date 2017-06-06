package vertx.model;

import vertx.VertxException;
import vertx.VertxJsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp ACS API
 *
 * CPE Identifier Class
 *
 * CPEs are identified via one of two of the following attributes:
 * Attribute        Type        Description
 * serialNumber	    String	    The Unique Serial Number assigned to the CPE Device by manufacturer
 * manufacturerOUI	String	    The OUI of the CPE’s manufacturer.
 * macAddress	    String	    MAC Address of the CPE Device
 *
 * Either "serialNumber" or "macAddress" is required but it is OK to provide both.
 *
 * The "manufacturerOUI" attribute is optional.
 * For example:
 * {
 *  "manufacturerOUI": "001122",
 *  "serialNumber": "08A027C000E2"
 * }
 *
 * Or:
 * {
 *  "macAddress": "08:A0:27:C0:00:E2"
 * }
 *
 * @author: ronyang
 */
public class CpeIdentifier {
    private static final Logger log = LoggerFactory.getLogger(CpeIdentifier.class.getName());

    /**
     * Field Name Constants
     */
    public static final String FIELD_NAME_OUI = CpeDeviceType.FIELD_NAME_OUI;
    public static final String FIELD_NAME_SN = Cpe.DB_FIELD_NAME_SN;
    public static final String FIELD_NAME_MAC_ADDRESS = Cpe.DB_FIELD_NAME_MAC_ADDRESS;

    /**
     * Define static JSON Field Validators with all 3 fields optional
     */
    public static final VertxJsonUtils.JsonFieldValidator mandatoryFields =
            new VertxJsonUtils.JsonFieldValidator();
    public static final VertxJsonUtils.JsonFieldValidator optionalFields =
            new VertxJsonUtils.JsonFieldValidator(FIELD_NAME_SN, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_MAC_ADDRESS, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_OUI, VertxJsonUtils.JsonFieldType.String);


    /**
     * Static Exception
     */
    public static final VertxException MISSING_SN_OR_MAC_ADDR =
            new VertxException("Missing CPE SN or MAC Address!");

    /**
     * Validate a given CPE Identifier JSON Object.
     *
     * @param cpeIdentifier
     */
    public static void validate(JsonObject cpeIdentifier) throws VertxException{
        try {
            /**
             * Call the common validation method first
             */
            VertxJsonUtils.validateFields(cpeIdentifier, mandatoryFields, optionalFields);
        } catch (VertxException ex) {
            log.error("Invalidate CPE Identifier!\n" + cpeIdentifier.encodePrettily());
            throw ex;
        }

        /**
         * Although all fields are optional, either SN or MAC should be present
         */
        if (cpeIdentifier.getString(FIELD_NAME_MAC_ADDRESS) == null &&
                cpeIdentifier.getString(FIELD_NAME_SN) == null) {
            // Neither SN nor MAC is present
            log.error("Invalidate CPE Identifier!\n" + cpeIdentifier.encodePrettily());
            throw MISSING_SN_OR_MAC_ADDR;
        }
    }

    /**
     * Get the MongoDB Matcher to find a single CPE.
     *
     * The CPE Identifier must have already been validated.
     *
     * @param orgId             Organization Id
     * @param cpeIdentifier     CPE Identifier JSON Object
     *
     * @return  A CPE Matcher as a JSON Object.
     */
    public static JsonObject getCpeMatcher(
            String orgId,
            JsonObject cpeIdentifier
    ) throws VertxException{
        // Validate CPE Identifier
        validate(cpeIdentifier);

        // Always Have the OrgId
        JsonObject matcher = new JsonObject().put(Cpe.DB_FIELD_NAME_ORG_ID, orgId);

        // Check for other fields
        if (cpeIdentifier.getString(FIELD_NAME_MAC_ADDRESS) != null) {
            matcher.put(Cpe.DB_FIELD_NAME_MAC_ADDRESS,
                    cpeIdentifier.getString(FIELD_NAME_MAC_ADDRESS));
        } else {
            matcher.put(Cpe.DB_FIELD_NAME_SN,
                    cpeIdentifier.getString(FIELD_NAME_SN));
            if (cpeIdentifier.getString(FIELD_NAME_OUI) != null) {
                matcher.put(CpeDeviceType.FIELD_NAME_OUI,
                        cpeIdentifier.getString(FIELD_NAME_OUI));
            }
        }

        return matcher;
    }

    /**
     * Build a CpeId struct by the CPE's "deviceId" struct (retrieved from MongoDB)
     *
     * @param cpeDeviceId
     */
    public static JsonObject getCpeIdentifierByCpeDeviceId(JsonObject cpeDeviceId) {
        return new JsonObject()
                .put(FIELD_NAME_OUI, cpeDeviceId.getString(CpeDeviceType.FIELD_NAME_OUI))
                .put(FIELD_NAME_SN, cpeDeviceId.getString(Cpe.DB_FIELD_NAME_SN));
    }
}
