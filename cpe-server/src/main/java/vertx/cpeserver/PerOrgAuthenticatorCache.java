package vertx.cpeserver;

import vertx.cache.AbstractLocalCache;
import vertx.cpeserver.httpauth.BasicAuthenticator;
import vertx.cpeserver.httpauth.DigestAuthenticator;
import vertx.cpeserver.httpauth.PerOrgAuthenticator;
import vertx.model.Organization;
import vertx.util.AcsConfigProperties;
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
