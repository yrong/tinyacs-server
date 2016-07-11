package vertx2.cache;

import vertx2.model.ConfigurationCategory;
import vertx2.util.AcsConstants;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * @author: ronyang
 */
public class ConfigurationCategoryCache extends AbstractLocalCache{
    /**
     * Constructor.
     *
     * @param vertx
     * @param crudEventAddress
     * @param dbCollectionName
     * @param cachedObjectType
     */
    public ConfigurationCategoryCache(Vertx vertx, String crudEventAddress, String dbCollectionName, String cachedObjectType) {
        super(vertx, crudEventAddress, dbCollectionName, cachedObjectType);
    }

    /**
     * Build actual index String by the raw JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public String getIndexString(JsonObject jsonObject) {
        // Index the cache by "name"
        return jsonObject.getString(AcsConstants.FIELD_NAME_NAME);
    }

    /**
     * Convert raw JSON Objects before saving into rawJsonObjectHashMap.
     *
     * Default to no action.
     *
     * @param rawJsonObject
     */
    public JsonObject convertRawJsonObject(JsonObject rawJsonObject) {
        return ConfigurationCategory.convertRawDbObject(rawJsonObject);
    }

    /**
     * Get a POJO by JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public Object getPojoByJsonObject(JsonObject jsonObject) throws Exception {
        return ConfigurationCategory.convertRawDbObject(jsonObject);
    }
}
