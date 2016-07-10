package vertx2.acs.nbi.configurationcategory;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxMongoUtils;
import vertx2.acs.nbi.AbstractAcNbiCrudService;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.model.AcsApiCrudTypeEnum;
import vertx2.model.ConfigurationCategory;
import vertx2.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.List;

/**
 * Project:  SXA-CC
 *
 * @author: ronyang
 */
public class ConfigurationCategoryService extends AbstractAcNbiCrudService {
    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return ConfigurationCategory.DB_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     */
    @Override
    public String[] getIndexFieldName() {
        return ConfigurationCategory.INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return ConfigurationCategory.EDITABLE_FIELDS;
    }

    /**
     * Validate an NBI Request.
     * <p/>
     * Must be implemented by actual services.
     * <p/>
     * If authentication is needed, this is the right place to implement.
     * <p/>
     * If validation is completed, returns true.
     * If validation is not completed (for example pending a further DB query callback), returns false.
     *
     * @param nbiRequest
     * @param crudType   Type of the CRUD operation.
     * @return boolean
     * @throws com.calix.sxa.SxaVertxException
     */
    @Override
    public boolean validate(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws SxaVertxException {
        switch (crudType) {
            case Create:
            case Update:
                ConfigurationCategory.validate(nbiRequest.body);
                break;

            default:
                break;
        }

        return true;
    }

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     * <p/>
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_CONFIGURATION_CATEGORY;
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
        return AcsConstants.VERTX_ADDRESS_ACS_CONFIG_CATEGORY_CRUD;
    }

    /**
     * Build MongoDB Query Keys for Retrieve.
     *
     * Default to null (return everything)
     */
    private static final JsonObject QUERY_KEY_BRIEF = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ID, 1)
            .putNumber(AcsConstants.FIELD_NAME_NAME, 1)
            .putNumber(AcsConstants.FIELD_NAME_DESCRIPTION, 1);
    public JsonObject buildRetrieveQueryKeys(AcsNbiRequest nbiRequest) {
        if (nbiRequest.getQueryBrief()) {
            return QUERY_KEY_BRIEF;
        }

        return null;
    }

    /**
     * For bulk query, get the "sort" JSON Object on how to sort the results.
     *
     * @param nbiRequest
     * @return  Default to null (let MongoDB to sort it)
     */
    private static final JsonObject SORT_BY_NAME =
            new JsonObject().putNumber(AcsConstants.FIELD_NAME_NAME, 1);
    @Override
    public JsonObject getDefaultQuerySort(AcsNbiRequest nbiRequest) {
        return SORT_BY_NAME;
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
        return ConfigurationCategory.convertRawDbObject(aRecord);
    }

    /**
     * Get a FindBeforeCreateResultHandler instance.
     * @param nbiRequest
     * @return
     */
    @Override
    public VertxMongoUtils.FindOneHandler getFindBeforeCreateResultHandler(AcsNbiRequest nbiRequest) {
        return new CustomFindBeforeCreateResultHandler(nbiRequest);
    }

    /**
     * Custom Find Result Handler (used by Create)
     */
    public class CustomFindBeforeCreateResultHandler extends VertxMongoUtils.FindOneHandler{
        AcsNbiRequest nbiRequest;

        /**
         * Constructor that requires an AcsNbiRequest POJO
         */
        public CustomFindBeforeCreateResultHandler(AcsNbiRequest nbiRequest) {
            this.nbiRequest = nbiRequest;
        }

        /**
         * The handler method body.
         * @param jsonObjectMessage
         */
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            // Call super
            super.handle(jsonObjectMessage);

            // Any match found?
            if (VertxMongoUtils.FIND_ONE_TIMED_OUT.equals(queryResult)) {
                // MongoDB Timed Out
                nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, MONGODB_TIMED_OUT);
                return;
            } else if (queryResult != null && queryResult.containsField(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID)) {
                // Found existing document, overwrite
                nbiRequest.body.putString(
                        AcsConstants.FIELD_NAME_ID,
                        queryResult.getString(AcsConstants.FIELD_NAME_ID)
                );
                log.info("Found an existing configuration category with the same name.");

                // treat it as an Update request from now on
                saveUpdate(nbiRequest);
            } else {
                // Ok to create new record
                log.info("Creating new document with\n" + nbiRequest.body.encodePrettily());
                try {
                    VertxMongoUtils.save(
                            vertx.eventBus(),
                            getDbCollectionName(),
                            nbiRequest.body,
                            getMongoSaveHandler(nbiRequest)
                    );
                } catch (SxaVertxException e) {
                    e.printStackTrace();
                    nbiRequest.sendResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            getServerInternalErrorWithDetails());
                }
            }
        }
    }
}
