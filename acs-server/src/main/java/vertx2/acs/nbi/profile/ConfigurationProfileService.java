package vertx2.acs.nbi.profile;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.VertxMongoUtils;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.cache.ConfigurationCategoryCache;
import vertx2.model.*;
import vertx2.util.AcsConstants;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  SXA-CC
 *
 * ACS API Configuration Profile Services.
 *
 * @author: ronyang
 */
public class ConfigurationProfileService extends AbstractProfileService{
    // Configuration Category Cache
    public ConfigurationCategoryCache configurationCategoryCache;

    /**
     * Static Constants
     */
    public static final SxaVertxException EDIT_PROFILE_PROHIBITED =
            new SxaVertxException("Editing in-use Profile is Prohibited!");

    /**
     * Start the service
     */
    @Override
    public void start(Vertx vertx) {
        super.start(vertx);

        /**
         * Initialize Configuration Category Cache
         */
        configurationCategoryCache = new ConfigurationCategoryCache(
                vertx,
                AcsConstants.VERTX_ADDRESS_ACS_CONFIG_CATEGORY_CRUD,
                ConfigurationCategory.DB_COLLECTION_NAME,
                ConfigurationCategory.class.getSimpleName()
        );
    }

    /**
     * Get the name (type) of the profile such as "configuration" or "notification"
     *
     * @return
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_CONFIGURATION_PROFILE;
    }

    /**
     * Validate Profile Field Types.
     *
     * @param profile
     */
    @Override
    public void validateFieldTypes(JsonObject profile) throws SxaVertxException {
        ConfigurationProfile.validate(profile);
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
     * @throws com.calix.sxa.SxaVertxException
     */
    @Override
    public boolean validate(final AcsNbiRequest nbiRequest, final AcsApiCrudTypeEnum crudType) throws SxaVertxException {
        /**
         * Perform basic Validate Mandatory/Optional Field Types
         */
        switch (crudType) {
            case Create:
                JsonObject profile = nbiRequest.body;
                validateFieldTypes(profile);

                // Convert profile to parameter values.
                ConfigurationProfile.processParameterValues(profile, configurationCategoryCache);
                break;

            case Update:
                /**
                 * Editing Profile is not allowed
                 */
                if (!nbiRequest.body.containsField(AcsConstants.FIELD_NAME_ID)) {
                    throw MISSING_ID_EXCEPTION;
                }
                break;

            default:
                break;
        }

        return true;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return ConfigurationProfile.DB_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     */
    @Override
    public String[] getIndexFieldName() {
        return ConfigurationProfile.INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return ConfigurationProfile.EDITABLE_FIELDS;
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
        return AcsConstants.VERTX_ADDRESS_ACS_CONFIG_PROFILE_CRUD;
    }

    /**
     * Build MongoDB Query Keys for Retrieve.
     *
     * Default to null (return everything)
     */
    private static final JsonObject QUERY_KEY = new JsonObject()
            .putNumber(ConfigurationProfile.FIELD_NAME_SERVICES, 0)
            .putNumber(ConfigurationProfile.FIELD_NAME_DYNAMIC_OBJECTS, 0)
            .putNumber(ConfigurationProfile.FIELD_NAME_PARAMETER_VALUES, 0);
    private static final JsonObject QUERY_KEY_BRIEF = QUERY_KEY.copy()
            .putNumber(ConfigurationProfile.FIELD_NAME_CONFIGURATIONS, 0)
            .putNumber(AcsConstants.FIELD_NAME_ORG_ID, 0);
    public JsonObject buildRetrieveQueryKeys(AcsNbiRequest nbiRequest) {
        if (nbiRequest.getQueryBrief()) {
            return QUERY_KEY_BRIEF;
        } else {
            return QUERY_KEY;
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
        JsonObject matcher = new JsonObject().putObject(
                Workflow.FIELD_NAME_ACTIONS,
                new JsonObject().putObject(
                        VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_ELEM_MATCH,
                        new JsonObject().putObject(
                                WorkflowAction.FIELD_NAME_PROFILE_ID,
                                new JsonObject().putArray(
                                        VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_ALL,
                                        new JsonArray().add(id)
                                )
                        )
                )
        );

        final CrossReferenceCheck crossReferenceCheck = new CrossReferenceCheck(matcher, Workflow.DB_COLLECTION_NAME);

        return new ArrayList<CrossReferenceCheck>() {{
            add(crossReferenceCheck);
        }};
    }

    /**
     * Before returning the query results to the client, call this method to perform additional actions on a per-record
     * basis.
     *
     * Default to no action.
     *
     * @param nbiRequest
     * @param aRecord
     * @return
     */
    @Override
    public JsonObject additionalPostRetrievePerRecordHandler(AcsNbiRequest nbiRequest, JsonObject aRecord) {
        VertxJsonUtils.convertDotInFieldNames(aRecord, false);
        return aRecord;
    }
}
