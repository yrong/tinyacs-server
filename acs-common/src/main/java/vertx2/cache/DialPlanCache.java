package vertx2.cache;

import vertx2.model.DialPlan;
import vertx2.util.AcsConstants;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * @author: ronyang
 */
public class DialPlanCache extends AbstractLocalCache{
    /**
     * Constructor.
     *
     * @param vertx
     * @param crudEventAddress
     * @param dbCollectionName
     * @param cachedObjectType
     */
    public DialPlanCache(Vertx vertx, String crudEventAddress, String dbCollectionName, String cachedObjectType) {
        super(vertx, crudEventAddress, dbCollectionName, cachedObjectType);
    }

    /**
     * Simpler Constructor.
     *
     * @param vertx
     */
    public DialPlanCache(Vertx vertx) {
        super(
                vertx,
                AcsConstants.VERTX_ADDRESS_ACS_DIAL_PLAN_CRUD,
                DialPlan.DB_COLLECTION_NAME,
                "DialPlan JsonObject");
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
     * Get a POJO by JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public Object getPojoByJsonObject(JsonObject jsonObject) throws Exception {
        return convertRawJsonObject(jsonObject);
    }

    /**
     * Get a Dial Plan JSON Object by id string
     */
    public JsonObject getDialPlanById(String id) {
        if (DialPlan.SYSTEM_DEFAULT_DIAL_PLAN_ID.equals(id)) {
            return DialPlan.SYSTEM_DEFAULT_DIAL_PLAN;
        }

        return (JsonObject)hashMap.get(id);
    }
}
