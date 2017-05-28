package vertx.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Project:  cwmp
 *
 * NBI Provisioning (for external API clients) Data Model and Utils.
 *
 * The idea is to shorten/hide/abstract the long TR-098 paths for some of the provisioning features that we are
 * exposing to the external NBI API clients.
 *
 * The conversion from the NBI Provisioning to raw TR-098 provisioning shall be done by simply replacing the short path
 * to the long TR-098 path.
 *
 * And vice versa.
 *
 * @author: ronyang
 */
public class NbiDeviceProvisioning {
    private static final Logger log = LoggerFactory.getLogger(NbiDeviceProvisioning.class.getName());

    /**
     * Static Map from NBI Path to TR-098 Path
     */
    public static final HashMap<String, String> nbiToTr098PathMap = initNbiToTr098PathMap();

    /**
     * Initialize the NBI Paths to TR098 Paths Map.
     */
    public static HashMap<String, String> initNbiToTr098PathMap (){
        HashMap<String, String> map = new HashMap<>();

        map.put("WANPPPConnection", "X_CALIX_CWMP_DefaultWanConnectionPath");
        map.put("WLANConfiguration", "InternetGatewayDevice.LANDevice.1.WLANConfiguration");

        return map;
    }

    /**
     * Get the TR-098 Path for a given NBI Provisioning path.
     * @param nbiPath
     */
    public static String nbiPathToTr098Path(String nbiPath) {
        return nbiToTr098PathMap.get(nbiPath);
    }
}
