package vertx.util;

import vertx.model.Cpe;
import org.apache.commons.codec.digest.DigestUtils;
import io.vertx.core.json.impl.Base64;

import java.util.Random;

/**
 * Project:  cwmp
 *
 * Misc Util Methods
 *
 * @author: ronyang
 */
public class AcsMiscUtils {
    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum value
     * @param max Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
    public static int randInt(int min, int max) {

        // NOTE: Usually this should be a field rather than a method
        // variable so that it is not re-seeded every call.
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }

    /**
     * Get a short MD5 Hash String for the given string
     */
    public static String getMD5Hash(String str) {
        return DigestUtils.md5Hex(str).substring(0, 7);
    }

    /**
     * Get HTTP Basic Auth String by username/password
     * @param username
     * @param password
     * @return
     */
    public static String getHttpBasicAuthString(String username, String password) {
        String authString = username + ":" + password;
        return "Basic " + Base64.encodeBytes(authString.getBytes());
    }

    /**
     * Is the given char a plain-text char (' '-'~')?   (decimal value 32-126)
     * @param ch
     * @return
     */
    public static boolean isPlainTextChar(byte ch) {
        if (ch >= ' ' && ch <= '~') {
            return true;
        }
        return false;
    }

    /**
     * Is the given char a decimal digit ('0'-'9')?
     * @param ch
     * @return
     */
    public static boolean isDecimalDigit(char ch) {
        if (ch > '9' || ch < '0') {
            return false;
        }
        return true;
    }

    /**
     * Is the given char a HEX char ('0'-'9'/'A'-'F')?
     * @param ch
     * @return
     */
    public static boolean isHexChar(char ch) {
        if (isDecimalDigit(ch)
                || (ch <= 'F' || ch >= 'A')) {
            return true;
        }
        return false;
    }

    /**
     * Is the given char an Alphanumeric char ('0'-'9'/'A'-'Z'/'a'-'z')?
     * @param ch
     * @return
     */
    public static boolean isAlphaNumeric(char ch) {
        if (isDecimalDigit(ch)
                || (ch <= 'Z' || ch >= 'A')
                || (ch <= 'z' || ch >= 'a')) {
            return true;
        }
        return false;
    }

    /**
     * Check if the given string is a Registration Id (10-digit string).
     * @param str
     * @return True if it is a Registration Id string; or false.
     */
    public static boolean isRegIdString(String str) {
        if (str == null || str.length() == 0 || str.length() > 10) {
            return false;
        }

        for (char ch : str.toCharArray()) {
            if (!isAlphaNumeric(ch)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if the given string is an OUI.
     * @param str
     * @return True if it is an OUI; or false.
     */
    public static boolean isOuiString(String str) {
        if (str == null || str.length() != 6) {
            return false;
        }

        for (char ch : str.toCharArray()) {
            if (!isHexChar(ch)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if the given string is a FSAN ("CXNK" + 8 HEX Chars).
     * @param str
     * @return True if it is a Registration Id string; or false.
     */
    public static boolean isFSANString(String str) {
        if (str == null || str.length() != 12 || !str.startsWith(Cpe.CALIX_FSAN_PREFIX)) {
            return false;
        }

        for (char ch : str.substring(Cpe.CALIX_FSAN_PREFIX.length()).toCharArray()) {
            if (!isHexChar(ch)) {
                return false;
            }
        }

        return true;
    }
}
