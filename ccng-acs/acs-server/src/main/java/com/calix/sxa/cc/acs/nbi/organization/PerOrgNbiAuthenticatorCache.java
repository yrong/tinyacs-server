package com.calix.sxa.cc.acs.nbi.organization;

import com.calix.sxa.cc.cache.AbstractLocalCache;
import com.calix.sxa.cc.model.Organization;
import com.calix.sxa.cc.util.AcsConstants;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA-CC
 * 
 * Maintain a local cache of all the organizations.
 *
 * @author: jqin
 */
public class PerOrgNbiAuthenticatorCache extends AbstractLocalCache {
    /**
     * Constructor
     */
    public PerOrgNbiAuthenticatorCache(Vertx vertx) {
        super(
            vertx,
            AcsConstants.VERTX_ADDRESS_ACS_ORGANIZATION_CRUD,
            Organization.DB_COLLECTION_NAME,
            "per-org-nbi-authenticator"
        );
    }

    /**
     * Build actual index String by the raw JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public String getIndexString(JsonObject jsonObject) {
        return PerOrgNbiAuthenticator.getBasicAuthString(jsonObject);
    }

    /**
     * Get a Per-oRg Authenticator POJO by JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public Object getPojoByJsonObject(JsonObject jsonObject) {
        return new PerOrgNbiAuthenticator(jsonObject);
    }

    /**
     * Get the Authenticator by Basic Auth Header.
     * @param authString
     */
    public PerOrgNbiAuthenticator getAuthenticatorByAuthString(String authString) {
        return (PerOrgNbiAuthenticator)hashMap.get(authString);
    }
}
