package vertx2.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  cwmp
 *
 * cwmp Extensions to the TR-098 Data Model.
 *
 * @author: ronyang
 */
public class CWMPTr098ModelExtensions {
    private static final Logger log = LoggerFactory.getLogger(CWMPTr098ModelExtensions.class.getName());

    // All extension names starts with this prefix
    public static final String EXT_PREFIX = "X_CALIX_CWMP_";

    /**
     * Abstract Name for the current WAN Data (IP or PPP) Connection Path.
     *
     * This is reported in every Inform (at least from Calix RGs).
     */
    public static final String WAN_IP_CONNECTION_PATH = EXT_PREFIX + "DefaultWanConnectionPath";

    /**
     * Abstract Name for the current WAN Connection Device, for example
     *  "InternetGatewayDevice.WANDevice.3.WANConnectionDevice.1"
     *
     * This is the physical WAN device is the parent object of all IP/PPP Connections.
     */
    public static final String WAN_CONNECTION_DEVICE = EXT_PREFIX + "WANConnectionDevice";

    /**
     * Abstract Names for the Voice RTP Codecs by priority/order
     */
    public static final String RTP_CODEC_PREFIX = EXT_PREFIX + "RTP_CODEC_";
    public static final String FIRST_ORDER_RTP_CODEC = RTP_CODEC_PREFIX + "1ST_ORDER";
    public static final String SECOND_ORDER_RTP_CODEC = RTP_CODEC_PREFIX + "2ND_ORDER";
    public static final String THIRD_ORDER_RTP_CODEC = RTP_CODEC_PREFIX + "3RD_ORDER";

    /**
     * Check if a given parameter name contains an cwmp Abstract Parameter Name.
     *
     * @param name
     */
    public static boolean containCWMPAbstractName(String name) {
        return name.contains(EXT_PREFIX);
    }

    /**
     * Convert all cwmp Abstract Names to actual names
     * @param cpe
     * @param paramName
     */
    public static String convertCWMPAbstractNameToActualName(Cpe cpe, String paramName) {
        // Extract the abstract name if any
        int begin = paramName.indexOf(EXT_PREFIX);
        if (begin < 0) {
            // done
            return paramName;
        }
        int end = paramName.indexOf(".", begin);
        if (end < 0) {
            end = paramName.length();
        }
        String abstractName = paramName.substring(begin, end);
        String actualName = null;

        switch (abstractName) {
            case WAN_IP_CONNECTION_PATH:
                actualName = cpe.deviceId.wanIpConnectionPath;
                break;

            case WAN_CONNECTION_DEVICE:
                String wanIpPath = cpe.deviceId.wanIpConnectionPath;
                if (wanIpPath.contains(".WANIPConnection.")) {
                    actualName = wanIpPath.substring(0, wanIpPath.indexOf(".WANIPConnection."));
                } else {
                    actualName = wanIpPath.substring(0, wanIpPath.indexOf(".WANPPPConnection."));
                }
                break;
        }

        if (actualName != null) {
            String result = paramName.replaceFirst(abstractName, actualName);
            if (containCWMPAbstractName(result)) {
                // continue the conversion
                return convertCWMPAbstractNameToActualName(cpe, result);
            } else {
                log.info(cpe.getCpeKey() + ": Converted " + abstractName + " -> " + actualName);
                return result;
            }
        } else {
            log.error("Unexpected Abstract Name " + abstractName + "!");;
            return paramName;
        }
    }
}
