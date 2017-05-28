package vertx.cpeserver.httpauth;

import vertx.util.AcsMiscUtils;
import vertx.util.GigaCenter;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.impl.Base64;

/**
 * Project:  cwmp
 *
 * HTTP Basic Authenticator
 *
 * @author: ronyang
 */
public class BasicAuthenticator extends PerOrgAuthenticator {
    private static final Logger loggerInstance = LoggerFactory.getLogger(BasicAuthenticator.class.getName());

    /**
     * Per-org auth string (BASE64 encoded)
     */
    public String encodedAuthString;

    /**
     * Constructor by a JSON Object.
     *
     * @param jsonObject
     */
    public BasicAuthenticator(JsonObject jsonObject) {
        super(jsonObject);
        initLogger(loggerInstance);

        // Build the auth String
        encodedAuthString = AcsMiscUtils.getHttpBasicAuthString(acsUsername, acsPassword);
        log.info("Org " + this.id + " Encoded Basic Auth String " + encodedAuthString + ")");
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
        return encodedAuthString;
    }

    /**
     * Authenticate the auth header received from CPE.
     *
     * @param authHeader
     * @return true if successfully verified; or false
     *
     *         Upon failure, an HTTP "NOT AUTHORIZED" response will be sent to CPE.
     */
    @Override
    public boolean verifyAuthHeader(String authHeader) {
        return encodedAuthString.equals(authHeader);
    }

    /**
     * Check to see if the auth header contains the Zero-Touch Credentials.
     *
     * @param authHeader
     * @return true if yes; or false
     */
    @Override
    public boolean hasZeroTouchCredentials(String authHeader) {
        String decodedHeader = new String(Base64.decode(authHeader));
        if (!decodedHeader.startsWith("Basic ")) {
            return false;
        }

        String[] fields = StringUtil.split(decodedHeader.substring(6), ':');

        if (fields == null || fields.length != 2) {
            return false;
        }
        if (!GigaCenter.isZeroTouchUsername(fields[0])) {
            return false;
        }
        if (!GigaCenter.ZERO_TOUCH_ACTIVATION_PASSWORD.equals(fields[1])) {
            return false;

        }
        return true;
    }
}
