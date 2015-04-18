package com.calix.sxa.cc.cpeserver;

import com.calix.sxa.cc.cache.AbstractLocalCache;
import com.calix.sxa.cc.cpeserver.httpauth.BasicAuthenticator;
import com.calix.sxa.cc.cpeserver.httpauth.DigestAuthenticator;
import com.calix.sxa.cc.cpeserver.httpauth.PerOrgAuthenticator;
import com.calix.sxa.cc.model.Organization;
import com.calix.sxa.cc.util.AcsConfigProperties;
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
public class PerOrgAuthenticatorCache extends AbstractLocalCache {
    /**
     * Constructor
     */
    public PerOrgAuthenticatorCache(Vertx vertx) {
        super(
            vertx,
            AcsConstants.VERTX_ADDRESS_ACS_ORGANIZATION_CRUD,
            Organization.DB_COLLECTION_NAME,
            "per-org-authenticator"
        );
    }

    /**
     * Build actual index String by the raw JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public String getIndexString(JsonObject jsonObject) {
        return jsonObject.getString(Organization.FIELD_NAME_URL);
    }

    /**
     * Get a Per-oRg Authenticator POJO by JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public Object getPojoByJsonObject(JsonObject jsonObject) {
        if (AcsConfigProperties.CPE_SERVER_LB_HTTPS_ENABLED) {
            // Use Basic Auth if HTTPs is enabled
            return new BasicAuthenticator(jsonObject);
        } else {
            // Use Digest Auth if HTTPs is disabled
            return new DigestAuthenticator(jsonObject);
        }
    }

    /**
     * Get the Authenticator by ACS URL Path.
     * @param url
     * @return
     */
    public PerOrgAuthenticator getAuthenticatorByAcsUrl(String url) {
        return (PerOrgAuthenticator)hashMap.get(url);
    }
}
