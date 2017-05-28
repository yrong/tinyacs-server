package vertx.acs.nbi.organization;

import vertx.cache.AbstractLocalCache;
import vertx.model.Organization;
import vertx.util.AcsConstants;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 * 
 * Maintain a local cache of all the organizations.
 *
 * @author: ronyang
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
