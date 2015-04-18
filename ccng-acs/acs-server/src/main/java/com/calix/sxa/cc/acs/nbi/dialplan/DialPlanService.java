package com.calix.sxa.cc.acs.nbi.dialplan;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import com.calix.sxa.cc.acs.nbi.AbstractAcNbiCrudService;
import com.calix.sxa.cc.acs.nbi.model.AcsNbiRequest;
import com.calix.sxa.cc.model.AcsApiCrudTypeEnum;
import com.calix.sxa.cc.model.DialPlan;
import com.calix.sxa.cc.model.ServicePlan;
import com.calix.sxa.cc.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  844e_mvp
 *
 * @author: jqin
 */
public class DialPlanService extends AbstractAcNbiCrudService {
    /**
     * Static Errors
     */
    public static final JsonObject CANNOT_DELETE_SYSTEM_DEFAULT = new JsonObject()
            .putString(AcsConstants.FIELD_NAME_ERROR, "The system-default dial plan cannot be deleted!");

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     * <p/>
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_DIAL_PLAN;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return DialPlan.DB_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     * <p/>
     * If more than one index fields are defined, they are considered as "AND" relation, i.e. all index fields
     * combined together to represent the unique index.
     */
    @Override
    public String[] getIndexFieldName() {
        return DialPlan.INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return DialPlan.EDITABLE_FIELDS;
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
    public boolean validate(final AcsNbiRequest nbiRequest, final AcsApiCrudTypeEnum crudType) throws SxaVertxException {
        switch (crudType) {
            case Create:
            case Update:
                VertxJsonUtils.validateFields(nbiRequest.body, DialPlan.MANDATORY_FIELDS, DialPlan.OPTIONAL_FIELDS);
                break;

            case Retrieve:
                String id = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
                if (DialPlan.SYSTEM_DEFAULT_DIAL_PLAN_ID.equals(id)) {
                    nbiRequest.sendResponse(HttpResponseStatus.OK, DialPlan.SYSTEM_DEFAULT_DIAL_PLAN);
                    return VALIDATION_PENDING_OR_FAILED;
                }
                break;

            case Delete:
                id = nbiRequest.body.getString(AcsConstants.FIELD_NAME_ID);
                if (DialPlan.SYSTEM_DEFAULT_DIAL_PLAN_ID.equals(id)) {
                    nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST, CANNOT_DELETE_SYSTEM_DEFAULT);
                    return VALIDATION_PENDING_OR_FAILED;
                }
                break;

            default:
                break;
        }
        return VALIDATION_SUCCEEDED;
    }

    /**
     * Should the retrieve results be sent to clients in chunk mode?
     */
    @Override
    public boolean bReturnRetrieveResultInChunkMode(AcsNbiRequest nbiRequest) {
        if (nbiRequest.body.containsField(AcsConstants.FIELD_NAME_ID) ||
                nbiRequest.body.containsField(AcsConstants.FIELD_NAME_NAME)) {
            // Return a single record if querying with ID
            return false;
        } else {
            // Return JSON Array if no id is present in the query filter
            return true;
        }
    }

    /**
     * Post Retrieve Handler.
     *
     * Default to only convert MongoDB "$date" to Strings, but can be override by actual services.
     *
     * @param nbiRequest
     * @param queryResults
     * @param moreExists
     *
     * @return  The processed query results as a JSON Array, or null if there is more work to be done.
     */
    @Override
    public JsonArray postRetrieve(AcsNbiRequest nbiRequest, JsonArray queryResults, boolean moreExists) {
        if (moreExists == false && bReturnRetrieveResultInChunkMode(nbiRequest)) {
            /**
             * Add System-Default to the end of query results
             */
            queryResults.add(DialPlan.SYSTEM_DEFAULT_DIAL_PLAN);
        }
        return super.postRetrieve(nbiRequest, queryResults, moreExists);
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
        JsonObject matcher = new JsonObject().putString(
                ServicePlan.FIELD_NAME_VOICE + "." + ServicePlan.FIELD_NAME_DIAL_PLAN,
                id
        );

        final CrossReferenceCheck crossReferenceCheck = new CrossReferenceCheck(matcher, ServicePlan.DB_COLLECTION_NAME);

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
        return AcsConstants.VERTX_ADDRESS_ACS_DIAL_PLAN_CRUD;
    }
}

