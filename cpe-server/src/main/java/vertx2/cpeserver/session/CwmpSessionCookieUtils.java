package vertx2.cpeserver.session;

import com.calix.sxa.VertxUtils;
import vertx2.cpeserver.CpeServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  SXA CC
 *
 * CWMP Session Cookie Utils.
 *
 * @author: ronyang
 */
public class CwmpSessionCookieUtils {
    private static final Logger log = LoggerFactory.getLogger(CwmpSessionCookieUtils.class.getName());

    // Cookie Prefix
    public static final String COOKIE_PREFIX = "CCSESSIONID=";

    /**
     * Build Cookie for a new Session
     *
     * Cookie shall be in the following format:
     *
     * "CCSESSIONID=[CPE KEY]~[# of ms]~[CPE Server's hostname]~[vertice index]"

     * @param cpeKey
     * @param verticeIndex
     */
    public static String getNewCookie(String cpeKey, int verticeIndex) {
        return "CCSESSIONID=" + cpeKey
                + "~" + System.currentTimeMillis()
                + "~" + VertxUtils.getLocalHostname()
                + "~" + verticeIndex;
    }

    /**
     * Get CPE Key from cookie string.
     *
     * @param cookie
     */
    public static String getCpeKeyFromCookie(String cookie)  {
        return cookie.substring(COOKIE_PREFIX.length(), cookie.indexOf('~'));
    }

    /**
     * Get CWMP Session Vertice Index from cookie string.
     *
     * @param cookie
     */
    public static Integer getSessionVerticeIndexFromCookie(String cookie) {
        String indexString = cookie.substring(cookie.lastIndexOf("~") + 1);
        try {
            int index = Integer.valueOf(indexString);
            if (index < CpeServerConstants.NUMBER_OF_SESSION_VERTICES) {
                return index;
            } else {
                log.error("Illegal Cookie String " + cookie + "!");
                return null;
            }

        } catch (Exception ex) {
            log.error("Illegal Cookie String " + cookie + "!");
            return null;
        }
    }

    /**
     * Extract value from a "[name]=[value]" string.
     *
     * @param nameValueString
     * @param name
     * @return
     */
    private static String extractValue(String nameValueString, String name) {
        if (!nameValueString.startsWith(name + "=")
                || nameValueString.length() <= (name.length() + 1)
                ) {
            return null;
        }

        return nameValueString.substring(name.length(), nameValueString.length());
    }
}
