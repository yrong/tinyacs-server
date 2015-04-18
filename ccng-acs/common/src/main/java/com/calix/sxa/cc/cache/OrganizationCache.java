package com.calix.sxa.cc.cache;

import com.calix.sxa.cc.model.Organization;
import com.calix.sxa.cc.util.AcsConstants;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA-CC
 *
 * An in-memory cache that contains all the organizations
 *
 * @author: jqin
 */
public class OrganizationCache extends AbstractLocalCache {
    /**
     * Constructor.
     *
     * @param vertx
     * @param crudEventAddress
     * @param dbCollectionName
     * @param cachedObjectType
     */
    public OrganizationCache(Vertx vertx, String crudEventAddress, String dbCollectionName, String cachedObjectType) {
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
     * Get a POJO by JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public Object getPojoByJsonObject(JsonObject jsonObject) throws Exception {
        return new Organization(jsonObject);
    }

    /**
     * Get Organization POJO by id.
     *
     * @param orgId
     * @return
     */
    public Organization getOrgById(String orgId) {
        return (Organization)hashMap.get(orgId);
    }
}
