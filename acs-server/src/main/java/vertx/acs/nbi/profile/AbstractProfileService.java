package vertx.acs.nbi.profile;

import vertx.VertxException;
import vertx.VertxMongoUtils;
import vertx.acs.nbi.AbstractAcNbiCrudService;
import vertx.acs.nbi.model.AcsNbiRequest;
import vertx.model.AcsApiCrudTypeEnum;
import vertx.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  cwmp
 *
 * Abstract ACS Profile Services.
 *
 * @author: ronyang
 */
public abstract class AbstractProfileService extends AbstractAcNbiCrudService{

    /**
     * Validate Profile Field Types.
     * @param profile
     */
    public abstract void validateFieldTypes(JsonObject profile) throws VertxException;

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
    @Override
    public boolean validate(final AcsNbiRequest nbiRequest, final AcsApiCrudTypeEnum crudType) throws VertxException {
        boolean bPendingDbQuery = false;

        /**
         * Perform basic Validate Mandatory/Optional Field Types
         */
        switch (crudType) {
            case Create:
            case Update:
                validateFieldTypes(nbiRequest.body);

                // Validate Child Profile ID Strings if any
                final JsonArray childProfiles = nbiRequest.body.getArray(AcsConstants.FIELD_NAME_INCLUDES);
                if (childProfiles != null && childProfiles.size() > 0) {
                    // Build a query matcher with this array
                    JsonObject matcher = new JsonObject().putObject(
                            VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID,
                            new JsonObject().putArray("$in", childProfiles)
                    );

                    // Query the DB and make sure all child profiles exist
                    log.info("Querying DB with matcher:\n" + matcher.encodePrettily());
                    bPendingDbQuery = true;
                    VertxMongoUtils.count(
                            vertx.eventBus(),
                            getDbCollectionName(),
                            matcher,
                            new Handler<Long>() {
                                @Override
                                public void handle(Long count) {
                                    if (childProfiles.size() != count) {
                                        String error = "One or more child profile(s) do not exist!";
                                        log.error(error + " (expecting " + childProfiles.size()
                                                + ", got " + count + ")");
                                        nbiRequest.sendResponse(
                                                HttpResponseStatus.BAD_REQUEST,
                                                new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, error)
                                        );
                                    } else {
                                        // Continue the handling process
                                        postValidation(nbiRequest, crudType);
                                    }
                                }
                            }
                    );
                }
                break;
        }

        if (bPendingDbQuery == true) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Cross Reference Check(s) is needed on Delete.
     */
    @Override
    public boolean doCrossReferenceCheckOnDelete() {
        return true;
    }

    /**
     * Return all the cross-reference checks needed when deleting a profile instance
     *
     * @param id    Internal id for this profile instance
     * @return      A Sorted Set that contains one or more CrossReferenceCheck instance(s).
     */
    @Override
    public List<CrossReferenceCheck> getAllCrossReferenceChecks(String id) {
        List<CrossReferenceCheck> allChecks = new ArrayList<>();

        // By default only checks this DB collection
        JsonObject matcher = new JsonObject().putObject(
                AcsConstants.FIELD_NAME_INCLUDES,
                new JsonObject().putArray("$all", new JsonArray().add(id))
        );
        allChecks.add(new CrossReferenceCheck(matcher, getDbCollectionName()));

        return allChecks;
    }
}
