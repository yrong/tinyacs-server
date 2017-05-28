package vertx.acs.nbi.cwmplog;

import vertx.VertxException;
import vertx.VertxJsonUtils;
import vertx.acs.nbi.AbstractAcNbiCrudService;
import vertx.acs.nbi.model.AcsNbiRequest;
import vertx.cwmp.CwmpMessage;
import vertx.model.AcsApiCrudTypeEnum;
import vertx.model.CpeIdentifier;
import vertx.util.AcsConstants;
import dslforumOrgCwmp12.GetParameterValuesResponseDocument;
import dslforumOrgCwmp12.SetParameterValuesDocument;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * Project:  cwmp
 *
 * ACS API CWMP Log Services.
 *
 * @author: ronyang
 */
public class CwmpLogService extends AbstractAcNbiCrudService{

    /**
     * Static Exceptions
     */
    public static final VertxException CANNOT_CREATE = new VertxException("Cannot create new CWMP Log Entries!");
    public static final VertxException CANNOT_UPDATE = new VertxException("Cannot modify CWMP Log Entries!");

    public static final String FIELD_NAME_INCLUDE_XML_TEXT = "includeXmlText";

    /**
     * Index Fields
     */
    private String[] indexFields = {};

    /**
     * Define static JSON Field Validator
     */
    public static final VertxJsonUtils.JsonFieldValidator MANDATORY_FIELDS =
            new VertxJsonUtils.JsonFieldValidator();

    public static final VertxJsonUtils.JsonFieldValidator OPTIONAL_FIELDS = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
            .append(CwmpMessage.DB_FIELD_NAME_TYPE, VertxJsonUtils.JsonFieldType.Any)
            .append(FIELD_NAME_INCLUDE_XML_TEXT, VertxJsonUtils.JsonFieldType.Boolean)
            .append(AcsConstants.FIELD_NAME_CPE_ID, VertxJsonUtils.JsonFieldType.JsonObject);

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     * <p/>
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_CWMP_LOG;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return CwmpMessage.DB_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     */
    @Override
    public String[] getIndexFieldName() {
        return indexFields;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return null;
    }

    /**
     * Validate an NBI Request.
     *
     * Must be implemented by actual services.
     *
     * If validation is completed, returns true.
     * If validation is not completed (for example pending a further DB query callback), returns false.
     *
     * @param nbiRequest
     * @param crudType      Type of the CRUD operation.
     *
     * @return boolean
     * @throws vertx.VertxException
     */
    public boolean validate(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
        /**
         * Perform basic Validate Mandatory/Optional Field Types
         */
        switch (crudType) {
            case Create:
                throw CANNOT_CREATE;
            case Update:
                throw CANNOT_UPDATE;
        }

        // Validate field names/types
        VertxJsonUtils.validateFields(nbiRequest.body, MANDATORY_FIELDS, OPTIONAL_FIELDS);

        // Either cpeId or "_id" is required
        if (!nbiRequest.body.containsField(AcsConstants.FIELD_NAME_CPE_ID) &&
                !nbiRequest.body.containsField(AcsConstants.FIELD_NAME_ID)) {
            throw MISSING_REQUIRED_FIELD_EXCEPTION;
        }
        return true;
    }

    /**
     * Pre-Process the request.
     *
     * @param nbiRequest
     * @param crudType      Type of the CRUD operation.
     *
     * @return None
     * @throws vertx.VertxException
     */
    @Override
    public void preProcess(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
        if (crudType.equals(AcsApiCrudTypeEnum.Delete)) {
            nbiRequest.body = convertRequestBodyToMatcher(nbiRequest);
        }
    }

    /**
     * For bulk query, get the "sort" JSON Object on how to sort the results.
     *
     * @param nbiRequest
     * @return  Default to null (let MongoDB to sort it)
     */
    private static final JsonObject SORT_BY_TIME =
            new JsonObject()
                    // Descending Order for Timestamp, so newest first
                    .putNumber(CwmpMessage.DB_FIELD_NAME_TIMESTAMP, -1)
                    // If multiple entries have the same timestamp, further sort with sn in descending order
                    .putNumber(CwmpMessage.DB_FIELD_NAME_SN, -1);
    @Override
    public JsonObject getDefaultQuerySort(AcsNbiRequest nbiRequest) {
        return SORT_BY_TIME;
    }

    /**
     * Build MongoDB Matcher for Retrieve
     */
    @Override
    public JsonObject buildRetrieveMatcher(AcsNbiRequest nbiRequest) throws VertxException {
        return convertRequestBodyToMatcher(nbiRequest);
    }

    /**
     * Build MongoDB Query Keys for Retrieve.
     *
     * Default to null (return everything)
     */
    private static final JsonObject INCLUDE_XML_TEXT = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ORG_ID, 0)
            .putNumber(AcsConstants.FIELD_NAME_CPE_ID, 0)
            .putNumber(CwmpMessage.DB_FIELD_NAME_EXPIRE_AT, 0)
            .putNumber(CwmpMessage.DB_FIELD_NAME_SN, 0);
    private static final JsonObject DEFAULT_QUERY_KEY = INCLUDE_XML_TEXT.copy()
            .putNumber(CwmpMessage.DB_FIELD_NAME_XML_TEXT, 0);
    @Override
    public JsonObject buildRetrieveQueryKeys(AcsNbiRequest nbiRequest) {
        boolean includeXmlText = nbiRequest.body.getBoolean(FIELD_NAME_INCLUDE_XML_TEXT, false);
        if (includeXmlText || nbiRequest.body.containsField(AcsConstants.FIELD_NAME_ID)) {
            return INCLUDE_XML_TEXT;
        } else {
            return DEFAULT_QUERY_KEY;
        }
    }

    /**
     * Convert the request body to matcher.
     *
     * @param nbiRequest
     */
    private JsonObject convertRequestBodyToMatcher(AcsNbiRequest nbiRequest) {
        JsonObject matcher = nbiRequest.body.copy();
        matcher.removeField(AcsConstants.FIELD_NAME_CPE_ID);
        matcher.removeField(FIELD_NAME_INCLUDE_XML_TEXT);

        // Convert CPE Identifier Struct to Mongo Matcher
        String id = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
        if (id == null) {
            JsonObject cpeId = nbiRequest.body.getObject(AcsConstants.FIELD_NAME_CPE_ID);
            String sn = cpeId.getString(CpeIdentifier.FIELD_NAME_SN);
            String mac = cpeId.getString(CpeIdentifier.FIELD_NAME_MAC_ADDRESS);
            String oui = cpeId.getString(CpeIdentifier.FIELD_NAME_OUI);
            if (sn != null) {
                matcher.putString(AcsConstants.FIELD_NAME_CPE_ID + "." + CpeIdentifier.FIELD_NAME_SN, sn);
            }
            if (mac != null) {
                matcher.putString(AcsConstants.FIELD_NAME_CPE_ID + "." + CpeIdentifier.FIELD_NAME_MAC_ADDRESS, mac);
            }
            if (oui != null) {
                matcher.putString(AcsConstants.FIELD_NAME_CPE_ID + "." + CpeIdentifier.FIELD_NAME_OUI, oui);
            }
        }

        return matcher;
    }

    /**
     * Get the list of field names that contain MongoDB "$date" timestamp.
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    private static final String[] TIMESTAMP_FIELDS = {
            CwmpMessage.DB_FIELD_NAME_TIMESTAMP
    };
    @Override
    public String[] getDateTimeFieldName() {
        return TIMESTAMP_FIELDS;
    }

    /**
     * Before returning the query results to the client, call this method to perform additional actions on a per-record
     * basis.
     *
     * Default to no action.
     *
     * @param nbiRequest
     * @param aRecord
     */
    @Override
    public JsonObject additionalPostRetrievePerRecordHandler(AcsNbiRequest nbiRequest, JsonObject aRecord) {
        if (SetParameterValuesDocument.SetParameterValues.class.getSimpleName()
                .equals(aRecord.getString(CwmpMessage.DB_FIELD_NAME_TYPE))
                ||
            GetParameterValuesResponseDocument.GetParameterValuesResponse.class.getSimpleName()
                .equals(aRecord.getString(CwmpMessage.DB_FIELD_NAME_TYPE))) {
            /**
             * "SetParameterValues"
             *
             * Convert the "$DOT" in parameter names to "." in summary
             */

            JsonObject summary = aRecord.getObject(CwmpMessage.DB_FIELD_NAME_SUMMARY);
            if (summary != null) {
                VertxJsonUtils.convertDotInFieldNames(summary, false);
            }
        }

        return aRecord;
    }
}
