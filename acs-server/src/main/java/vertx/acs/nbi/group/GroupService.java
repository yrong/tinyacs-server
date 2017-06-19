package vertx.acs.nbi.group;

import vertx.VertxException;
import vertx.VertxJsonUtils;
import vertx.acs.nbi.AbstractAcNbiCrudService;
import vertx.acs.nbi.model.AcsNbiRequest;
import vertx.model.AcsApiCrudTypeEnum;
import vertx.model.Cpe;
import vertx.model.CpeGroup;
import vertx.model.Workflow;
import vertx.util.AcsConstants;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  cwmp ACS API
 *
 * Device Group Web Service Implementation.
 *
 * @author: ronyang
 */
public class GroupService extends AbstractAcNbiCrudService {
    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_GROUP;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return CpeGroup.DB_COLLECTION_NAME;
    }

    /**
     * A group is uniquely identified by "orgId" + "group name".
     */
    @Override
    public String[] getIndexFieldName() {
        return CpeGroup.INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return CpeGroup.EDITABLE_FIELDS;
    }

    /**
     * JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator groupMandatoryFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(CpeGroup.FIELD_NAME_GROUP_TYPE, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator groupOptionalFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsConstants.FIELD_NAME_CPE_FILTER, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator cpeFilterOptionalFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(Cpe.DeviceId.FIELD_NAME_MANUFACTURER, VertxJsonUtils.JsonFieldType.String)
                    .append(Cpe.DeviceId.FIELD_NAME_MODEL_NAME, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(Cpe.DeviceId.FIELD_NAME_HW_VER, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(Cpe.DeviceId.FIELD_NAME_SW_VER, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(Cpe.DeviceId.FIELD_NAME_OUI, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(Cpe.DB_FIELD_NAME_SN, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(CpeGroup.OPERATOR_OR,VertxJsonUtils.JsonFieldType.JsonArray);

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
        switch (crudType) {
            case Create:
            case Update:
                JsonObject group = nbiRequest.body;
                try {
                    VertxJsonUtils.validateFields(group, groupMandatoryFields, groupOptionalFields);
                } catch (VertxException ex) {
                    throw new VertxException("Invalid group! Caught exception " + ex.getMessage());
                }

                /**
                 * Validate group type
                 */
                String groupType = group.getString(CpeGroup.FIELD_NAME_GROUP_TYPE);
                if (!CpeGroup.GROUP_TYPE_DYNAMIC.equals(groupType) &&
                        !CpeGroup.GROUP_TYPE_STATIC.equals(groupType)) {
                    throw new VertxException("Invalid  Group Type!");
                }

                /**
                 * Validate the Filter Content.
                 */
                JsonObject cpeFilters = group.getJsonObject(AcsConstants.FIELD_NAME_CPE_FILTER);
                //VertxJsonUtils.validateFields(cpeFilter, null, cpeFilterOptionalFields);
                if (cpeFilters != null) {
                    for (String fieldName : cpeFilters.fieldNames()) {
                        Object aFilter = cpeFilters.getValue(fieldName);
                        if (aFilter instanceof JsonObject) {
                            JsonObject jsonFilter = (JsonObject)aFilter;
                            String regex = jsonFilter.getString(CpeGroup.OPERATOR_REGEX);
                            /**
                             * Right now regex is only applicable to FSAN which can only
                             * contain 'C'/'X'/'N'/'K' or 0-9 or 'A'-'F'.
                             */
                            if (regex != null) {
                                /**
                                 * Special Case for CWMP-1296
                                 */
                                if (regex.charAt(0) == '^' && regex.charAt(regex.length() - 1) == '$') {
                                    // Remove the leading '^' and trailing '$'
                                    regex = regex.substring(1, regex.length() - 1);
                                }

                                for (int i = 0; i < regex.length(); i ++) {
                                    char aChar = regex.charAt(i);
                                    if (aChar >= '0' && aChar <= '9') {
                                        continue;
                                    }
                                    if (aChar >= 'A' && aChar <= 'Z') {
                                        continue;
                                    }
                                    throw new VertxException(
                                            "Found illegal character '" + aChar + "' in the \"Contains\" rule!"
                                    );
                                }
                            }
                        }
                    }
                }
                break;
        }

        return true;
    }


    /**
     * PreProcess an NBI Request.
     *
     * @param nbiRequest
     * @param crudType   Type of the CRUD operation.
     * @return None
     * @throws vertx.VertxException
     */
    @Override
    public void preProcess(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
        /**
         * For Crete/Update: Convert CPE Filter from JSON Object into raw JSON String
         */
        switch (crudType) {
            case Create:
            case Update:
                String filterString = nbiRequest.body.getJsonObject(AcsConstants.FIELD_NAME_CPE_FILTER).encode();
                nbiRequest.body.remove(AcsConstants.FIELD_NAME_CPE_FILTER);
                nbiRequest.body.put(AcsConstants.FIELD_NAME_CPE_FILTER, filterString);
                break;
        }
    }

    /**
     * Override Default Post Retrieve Handler.
     *
     * @param nbiRequest
     * @param queryResults
     * @param moreExist
     *
     * @return  The processed query results as a JSON Array, or null if there is more work to be done.
     */
    @Override
    public JsonArray postRetrieve(
            AcsNbiRequest nbiRequest,
            JsonArray queryResults,
            boolean moreExist) {
        if (queryResults != null && queryResults.size() > 0) {
            /**
             * Convert CPE Filter from raw JSON String back to JSON Object
             */
            JsonArray convertedResults = new JsonArray();
            for (int i = 0; i < queryResults.size(); i++) {
                JsonObject aGroup = queryResults.getJsonObject(i);
                String filterString = aGroup.getString(AcsConstants.FIELD_NAME_CPE_FILTER);
                aGroup.remove(AcsConstants.FIELD_NAME_CPE_FILTER);
                aGroup.put(AcsConstants.FIELD_NAME_CPE_FILTER, new JsonObject(filterString));
                convertedResults.add(aGroup);
            }

            return convertedResults;
        } else {
            return super.postRetrieve(nbiRequest, queryResults, moreExist);
        }
    }

    /**
     * Whether Cross Reference Check(s) is needed on Update.
     *
     * Default to false (i.e. no cross-reference checks are needed).
     */
    @Override
    public boolean doCrossReferenceCheckOnUpdate() {
        return true;
    }

    /**
     * Whether Cross Reference Check(s) is needed on Delete.
     *
     * Default to false (i.e. no cross-reference checks are needed).
     */
    @Override
    public boolean doCrossReferenceCheckOnDelete() {
        return true;
    }

    /**
     * Return all the cross-reference checks needed when deleting a profile instance
     *
     * Default to return null (i.e. no cross-reference checks are needed).
     *
     * @param id    Internal id for this profile instance
     * @return      A Sorted Set that contains one or more CrossReferenceCheck instance(s), or null.
     */
    @Override
    public List<CrossReferenceCheck> getAllCrossReferenceChecks(String id) {
        JsonObject matcher = new JsonObject().put(Workflow.FIELD_NAME_GROUPS, id);

        final CrossReferenceCheck crossReferenceCheck = new CrossReferenceCheck(matcher, Workflow.DB_COLLECTION_NAME);

        return new ArrayList<CrossReferenceCheck>() {{
            add(crossReferenceCheck);
        }};
    }

    /**
     * Get the Vert.x Event Bus Address for publishing the CRUD Events to.
     *
     * Default to null (do not publish CRUD events.
     *
     * @return
     */
    @Override
    public String getPublishCrudEventsAddress() {
        return AcsConstants.VERTX_ADDRESS_ACS_GROUP_CRUD;
    }
}
