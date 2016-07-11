package vertx2.cpeserver.httpauth;

import vertx2.util.GigaCenter;
import vertx2.util.HttpDigestAuthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;


/**
 * Project:  cwmp ACS
 *
 * This class defines HTTP Digest Authentication Function (as a server)
 *
 * @author: ronyang
 */
public class DigestAuthenticator extends PerOrgAuthenticator{
    private static final Logger loggerInstance = LoggerFactory.getLogger(DigestAuthenticator.class.getName());

    /**
     * Constructor by a JSON Object.
     *
     * @param jsonObject
     */
    public DigestAuthenticator(JsonObject jsonObject) {
        super(jsonObject);
        initLogger(loggerInstance);
    }

    /**
     * Initialize Logger Instance.
     *
     * @param logger
     */
    @Override
    public void initLogger(Logger logger) {
        log = loggerInstance;
    }

    /**
     * Get an Auth Challenge String.
     */
    @Override
    public String getChallengeString() {
        return HttpDigestAuthUtils.getChallengeString(HttpDigestAuthUtils.getNonceByOrgId(id), acsUrl);
    }

    /**
     * Authenticate the auth header received from CPE.
     *
     * @return true if successfully verified; or false
     *
     * Upon failure, an HTTP "NOT AUTHORIZED" response will be sent to CPE.
     */
    @Override
    public boolean verifyAuthHeader(String authHeader) {
        return HttpDigestAuthUtils.verifyAuthHeader(
                authHeader,
                HttpDigestAuthUtils.getNonceByOrgId(id),
                acsUsername,
                acsPassword
        );
    }

    /**
     * Check to see if the auth header contains the Zero-Touch Credentials.
     *
     * @param authHeader
     * @return true if yes; or false
     */
    @Override
    public boolean hasZeroTouchCredentials(String authHeader) {
        String username = HttpDigestAuthUtils.getUsernameFromAuthHeader(authHeader);
        if (username == null || !GigaCenter.isZeroTouchUsername(username)) {
            log.error(username + " is not a valid zero touch username.");
            return false;
        }

        return HttpDigestAuthUtils.verifyAuthHeader(
                authHeader,
                HttpDigestAuthUtils.getNonceByOrgId(id),
                username,
                GigaCenter.ZERO_TOUCH_ACTIVATION_PASSWORD
        );
    }
}
