package vertx.acs.cache;

import vertx.VertxMongoUtils;
import vertx.cache.AbstractMultiOrgLocalCache;
import vertx.model.ExecPolicy;
import vertx.model.Workflow;
import vertx.model.WorkflowTrigger;
import vertx.util.AcsConstants;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.Comparator;

/**
 * Project:  cwmp
 *
 * Local cache of all passive workflows.
 *
 * TODO: Organize the workflows by organizations
 *
 * @author: ronyang
 */
public class PassiveWorkflowCache extends AbstractMultiOrgLocalCache {
    /**
     *
     */

    /**
     * Constructor.
     *
     * @param vertx
     * @param crudEventAddress
     * @param dbCollectionName
     * @param cachedObjectType
     */
    public PassiveWorkflowCache(
            Vertx vertx,
            String crudEventAddress,
            String dbCollectionName,
            String cachedObjectType) {
        super(vertx, crudEventAddress, dbCollectionName, cachedObjectType);
    }

    /**
     * Build actual index String by the raw JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public String getIndexString(JsonObject jsonObject) {
        return jsonObject.getString(AcsConstants.FIELD_NAME_ID);
    }

    /**
     * Get DB Query Matcher
     */
    public static final JsonObject DEFAULT_MATCHER = new JsonObject()
            .putString(
                    Workflow.FIELD_NAME_EXEC_POLICY + "." + ExecPolicy.FIELD_NAME_INITIAL_TRIGGER + "."
                            + WorkflowTrigger.FIELD_NAME_TRIGGER_TYPE,
                    WorkflowTrigger.TriggerTypeEnum.CPE_EVENT.typeString
            );
    public JsonObject getDbQueryMatcher() {
        return DEFAULT_MATCHER;
    }

    /**
     * Get a POJO by JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public Object getPojoByJsonObject(JsonObject jsonObject) throws Exception {
        // Counts are not interesting to us
        jsonObject = convertRawJsonObject(jsonObject);

        // Check for changes
        String id = jsonObject.getString(AcsConstants.FIELD_NAME_ID);
        String orgId = jsonObject.getString(AcsConstants.FIELD_NAME_ORG_ID);
        Workflow workflow = get(orgId, id);
        if (workflow != null) {
            if (workflow.isActive()) {
                // Ignore all active workflows
                return null;
            }

            if(workflow.rawJsonObject.equals(jsonObject)) {
                return workflow;
            }
        }

        // Create new POJO
        workflow = Workflow.validateJsonObject(jsonObject);
        if (workflow.isActive()) {
            return null;
        }

        return workflow;
    }

    /**
     * Convert raw JSON Objects before saving into rawJsonObjectHashMap.
     *
     * Default to no action.
     *
     * @param rawJsonObject
     */
    @Override
    public JsonObject convertRawJsonObject(JsonObject rawJsonObject) {
        rawJsonObject.removeField(Workflow.FIELD_NAME_TOTAL_COUNT);
        rawJsonObject.removeField(Workflow.FIELD_NAME_SUCCESS_COUNT);
        rawJsonObject.removeField(Workflow.FIELD_NAME_FAILURE_COUNT);

        return rawJsonObject;
    }

    /**
     * Custom comparator that sorts by creation time
     */
    @Override
    public Comparator getComparator(){
        return new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                JsonObject workflow1 = rawJsonObjectHashMap.get(o1);
                JsonObject workflow2 = rawJsonObjectHashMap.get(o2);

                JsonObject dateObject1 = workflow1 == null? null : workflow1.getObject(AcsConstants.FIELD_NAME_CREATE_TIME);
                JsonObject dateObject2 = workflow2 == null? null : workflow2.getObject(AcsConstants.FIELD_NAME_CREATE_TIME);

                if (dateObject1 == null) {
                    if (dateObject2 == null) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else {
                    if (dateObject2 == null) {
                        return 1;
                    } else {
                        String time1 = dateObject1.getField(VertxMongoUtils.MOD_MONGO_DATE).toString();
                        String time2 = dateObject2.getField(VertxMongoUtils.MOD_MONGO_DATE).toString();
                        return time1.compareTo(time2);
                    }
                }
            }
        };
    }
}
