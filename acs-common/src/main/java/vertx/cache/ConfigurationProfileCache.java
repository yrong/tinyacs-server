package vertx.cache;

import vertx.util.AcsConstants;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * @author: ronyang
 */
public class ConfigurationProfileCache extends AbstractLocalCache{
    /**
     * Constructor.
     *
     * @param vertx
     * @param crudEventAddress
     * @param dbCollectionName
     * @param cachedObjectType
     */
    public ConfigurationProfileCache(Vertx vertx, String crudEventAddress, String dbCollectionName, String cachedObjectType) {
        super(vertx, crudEventAddress, dbCollectionName, cachedObjectType);
    }

    /**
     * Build actual index String by the raw JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public String getIndexString(JsonObject jsonObject) {
        // Index the cache by "_id"
        return jsonObject.getString(AcsConstants.FIELD_NAME_ID);
    }

    /**
     * Get a POJO by JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public Object getPojoByJsonObject(JsonObject jsonObject) throws Exception {
        return jsonObject;
    }
}
