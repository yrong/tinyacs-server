package vertx2.acs.nbi.maintenanceschedule;

import vertx2.VertxException;
import vertx2.acs.nbi.AbstractAcNbiCrudService;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.model.AcsApiCrudTypeEnum;
import vertx2.model.ExecPolicy;
import vertx2.model.MaintenanceSchedule;
import vertx2.model.Workflow;
import vertx2.util.AcsConstants;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  cwmp
 *
 * @author: ronyang
 */
public class MaintenanceScheduleService extends AbstractAcNbiCrudService{

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return MaintenanceSchedule.DB_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     */
    @Override
    public String[] getIndexFieldName() {
        return MaintenanceSchedule.INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return MaintenanceSchedule.EDITABLE_FIELDS;
    }

    /**
     * Validate an NBI Request.
     *
     * Must be implemented by actual services.
     *
     * If authentication is needed, this is the right place to implement.
     *
     * If validation is completed, returns true.
     * If validation is not completed (for example pending a further DB query callback), returns false.
     *
     * @param nbiRequest
     * @param crudType   Type of the CRUD operation.
     * @return boolean
     * @throws vertx2.VertxException
     */
    @Override
    public boolean validate(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
        /**
         * Perform basic Validate Mandatory/Optional Field Types
         */
        switch (crudType) {
            case Create:
            case Update:
                new MaintenanceSchedule(nbiRequest.body);
                break;
        }

        return true;
    }

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_MAINTENANCE_SCHEDULE;
    }

    /**
     * Whether Cross Reference Check(s) is needed on Delete.
     *
     * Default to false (i.e. no cross-reference checks are needed).
     */
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
    public List<CrossReferenceCheck> getAllCrossReferenceChecks(String id) {
        List<CrossReferenceCheck> crossReferenceChecks = new ArrayList<CrossReferenceCheck>();

        crossReferenceChecks.add(
                new CrossReferenceCheck(
                        new JsonObject().putString(
                                Workflow.FIELD_NAME_EXEC_POLICY + "." + ExecPolicy.FIELD_NAME_MAINTENANCE_SCHEDULE,
                                id
                        ),
                        Workflow.DB_COLLECTION_NAME
                    )
            );
        return crossReferenceChecks;
    }

    /**
     * Get the Vert.x Event Bus Address for publishing the CRUD Events to.
     *
     * Default to null (do not publish CRUD events.
     *
     * @return
     */
    public String getPublishCrudEventsAddress() {
        return AcsConstants.VERTX_ADDRESS_MAINTENANCE_SCHEDULE;
    }
}
