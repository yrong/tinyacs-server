package com.calix.sxa.cc.cpe.sim;

import com.calix.sxa.VertxConstants;
import com.calix.sxa.VertxMongoUtils;
import com.calix.sxa.VertxUtils;
import com.calix.sxa.cc.model.Cpe;
import com.calix.sxa.cc.util.AcsConstants;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.http.auth.AUTH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.json.impl.Base64;

import java.util.Set;

/**
 * Project:  sxa-cc-parent
 *
 * Profile Management Util Methods.
 *
 * @author: jqin
 */
public class CpeSimUtils {
    private static final Logger log = LoggerFactory.getLogger(CpeSimUtils.class.getName());

    /**
     * A JSON that contains the default CPE meta data
     */
    public static final String DEFAULT_CPE_DATA_FILE = "cpeDefault.json";

    /**
     * Default CPE Meta Data
     */
    public static JsonObject DEFAULT_CPE_DATA = null;

    /**
     * Shared Session Map
     */
    public static Set<String> allSessions;

    /**
     * Get Default CPE Meta Data from resource file.
     */
    public static void initDefaultCpeData(Vertx vertx) {
        vertx.fileSystem().readFile(
                DEFAULT_CPE_DATA_FILE,
                new AsyncResultHandler<Buffer>() {
                    public void handle(AsyncResult<Buffer> ar) {
                        if (ar.succeeded()) {
                            try {
                                DEFAULT_CPE_DATA = new JsonObject(ar.result().toString());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            log.error(VertxUtils.highlightWithHashes(
                                    "File " + DEFAULT_CPE_DATA_FILE + " does not exist!" + " (" + ar.cause() + ")"));
                        }
                    }
                });

        // Init shared session set
        allSessions = vertx.sharedData().getSet(CpeSimConstants.SHARED_SESSION_SET);
    }

    /**
     * Get the connection request URL for a CPE.
     *
     * @param cpeKey
     * @return
     */
    public static String getConnReqUrl(String cpeKey) {
        return "http://" + VertxUtils.getLocalHostname() + ":" + CpeSimConstants.HTTP_SERVICE_REQ_PORT + "/connreq/"
                + cpeKey.replace("-", "/");
    }

    /**
     * get HTTP Basic Auth String
     */
    private static String basicAuthString = null;
    public static String getBasicAuthString() {
        if (basicAuthString == null) {
            String authString = CpeSimConstants.ACS_USERNAME + ":" + CpeSimConstants.ACS_PASSWORD;
            basicAuthString = "Basic " + Base64.encodeBytes(authString.getBytes());
        }

        return basicAuthString;
    }

    /**
     * Build a JSON Object for a given SN (and CPE key).
     *
     * @param sn
     * @param orgId
     * @param oui
     * @return
     */
    public static JsonObject getDefaultCpeDataObjectBySn(long sn, String orgId, String oui) {
        /**
         * Query Failed! Use default CPE Meta Data
         */
        JsonObject cpe = CpeSimUtils.DEFAULT_CPE_DATA.copy();

        /**
         * Overwrite IP and MAC Addresses
         */
        cpe.putString(Cpe.DB_FIELD_NAME_IP_ADDRESS, CpeSimUtils.snToIpAddress(sn));
        cpe.putString(Cpe.DB_FIELD_NAME_MAC_ADDRESS, CpeSimUtils.snToMacAddress(sn));

        /**
         * Overwrite the CPE Key/SN/Connection Request URL
         */
        String cpeKey = Cpe.getCpeKey(orgId, oui, CpeSimUtils.snToHexString(sn));
        cpe.putString("_id", cpeKey);
        cpe.putString(Cpe.DB_FIELD_NAME_SN, CpeSimUtils.snToHexString(sn));
        cpe.putString(Cpe.DB_FIELD_NAME_CONNREQ_URL, CpeSimUtils.getConnReqUrl(cpeKey));

        JsonObject igd = cpe.getObject(Cpe.DB_FIELD_NAME_PARAM_VALUES)
                .getObject(Cpe.INTERNET_GATEWAY_DEVICE_ROOT);

        igd.getObject("ManagementServer")
                .putString("URL", CpeSimConstants.ACS_URL)
                .putString("ConnectionRequestURL", CpeSimUtils.getConnReqUrl(cpeKey));

        igd.getObject("DeviceInfo")
                .putString("SerialNumber", CpeSimUtils.snToHexString(sn));

        return cpe;
    }

    /**
     * Build a Simple JSON Object for a given SN (and CPE key).
     *
     * Used when populating the DB.
     *
     * @param sn
     * @param orgId
     * @param oui
     * @return
     */
    public static JsonObject getSimpleCpeDataObjectBySn(long sn, String orgId, String oui) {
        /**
         * Query Failed! Use default CPE Meta Data
         */
        JsonObject cpe = CpeSimUtils.DEFAULT_CPE_DATA.copy();

        /**
         * Overwrite IP and MAC Addresses
         */
        cpe.putString(Cpe.DB_FIELD_NAME_IP_ADDRESS, CpeSimUtils.snToIpAddress(sn));
        cpe.putString(Cpe.DB_FIELD_NAME_MAC_ADDRESS, CpeSimUtils.snToMacAddress(sn));

        /**
         * Overwrite the CPE Key/SN/Connection Request URL
         */
        String cpeKey = Cpe.getCpeKey(orgId, oui, CpeSimUtils.snToHexString(sn));
        cpe.putString("_id", cpeKey);
        cpe.putString(Cpe.DB_FIELD_NAME_SN, CpeSimUtils.snToHexString(sn));
        cpe.putString(Cpe.DB_FIELD_NAME_CONNREQ_URL, CpeSimUtils.getConnReqUrl(cpeKey));

        return cpe;
    }

    /**
     * A friendly wrapper to send async HTTP request.
     *
     * @param url           The URL String without the hostname and port #
     * @param httpClient
     * @param httpMethod    HTTP Method (GET/PUT/POST/DELETE)
     * @param cookie
     * @param authResponse
     * @param payload
     */
    public static void sendHttpRequest(
            String url,
            HttpClient  httpClient,
            HttpMethod httpMethod,
            String cookie,
            String authResponse,
            String payload,
            Handler<HttpClientResponse> handler) {
        log.info("Sending payload to " + getUrlFromHttpClient(httpClient) + url + ":\n" + payload);

        // Build the request
        HttpClientRequest request = httpClient.request(httpMethod.name(), url, handler);

        // Content Type
        request.headers().set("Content-Type", "text/xml");

        // Auth Response
        if (authResponse != null) {
            request.headers().set(AUTH.WWW_AUTH_RESP, authResponse);
        }

        // Cookie(s)
        if (cookie != null) {
            log.info("Cookie:" + cookie);
            request.headers().set("Cookie", cookie);
        } else {
            // Add HTTP Basic Auth Header if no cookie
            //request.headers().set("Authorization", getBasicAuthString());
        }

        // Payload
        if (payload != null) {
            request.headers().set("Content-Length", String.valueOf(payload.length()));
            request.write(payload);
        } else {
            request.headers().set("Content-Length", "0");
        }

        request.end();
    }

    /**
     * Get the [host:port] from HTTP Client.
     *
     * @param client
     * @return
     */
    public static String getUrlFromHttpClient(HttpClient client) {
        return "http://" + client.getHost() + ":" + client.getPort();
    }

    /**
     * Find a single CPE by CPE Key/Id
     */
    public static void findCpeById(EventBus eventBus, String cpeKey, Handler<Message<JsonObject>> handler) {
        /**
         * Build the query message
         */
        JsonObject queryMessage = new JsonObject();
        queryMessage.putString("action", "findone");
        queryMessage.putString("collection", CpeSimConstants.MONGO_CPE_SIM__COLLECTION);
        queryMessage.putObject("matcher", new JsonObject().putString("_id", cpeKey));
        //log.info(queryMessage.encodePrettily());

        /**
         * Send it
         */
        eventBus.send(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                queryMessage,
                handler
        );
    }

    /**
     * Update a single CPE by CPE Key/Id
     */
    public static void updateCpeById(
            EventBus eventBus,
            String cpeKey,
            JsonObject updates,
            Handler<Message<JsonObject>> handler) {
        /**
         * Build the query message
         */
        JsonObject updateMessage = new JsonObject();
        updateMessage.putString("action", "update");
        updateMessage.putString("collection", CpeSimConstants.MONGO_CPE_SIM__COLLECTION);
        updateMessage.putObject("criteria", new JsonObject().putString("_id", cpeKey));

        /**
         * Convert update object
         */
        JsonObject newUpdates = new JsonObject();
        JsonObject newSets = new JsonObject();
        JsonObject sets = updates.getObject("$set");
        if (sets != null) {
            for (String fieldName : sets.getFieldNames()) {
                String value = sets.getField(fieldName).toString();
                newSets.putString(fieldName + "._value", value);
            }

            newUpdates.putObject("$set", newSets);
        }
        if (updates.containsField("$unset")) {
            newUpdates.putObject("$unset", updates.getObject("$unset"));
        }
        if (updates.containsField("$currentDate")) {
            newUpdates.putObject("$currentDate", updates.getObject("$currentDate"));
        }
        updateMessage.putObject("objNew", newUpdates);

        log.info(updateMessage.encodePrettily());

        /**
         * Send it
         */
        eventBus.send(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                updateMessage,
                handler
        );
    }

    /**
     * Persist CPE Meta Data.
     *
     * @param eventBus
     * @param cpe
     */
    public static void persistCpe(EventBus eventBus, String id, JsonObject cpe, boolean bIsNew) {
        log.info("Saving CPE...\n" + cpe.encodePrettily());
        try {
            if (bIsNew) {
                VertxMongoUtils.save(
                        eventBus,
                        CpeSimConstants.MONGO_CPE_SIM__COLLECTION,
                        cpe,
                        null
                );
            } else {
                VertxMongoUtils.updateWithMatcher(
                        eventBus,
                        CpeSimConstants.MONGO_CPE_SIM__COLLECTION,
                        new JsonObject().putString(AcsConstants.FIELD_NAME_ID, id),
                        cpe,
                        null
                );
            }
        } catch (Exception ex) {

        }
    }

    /**
     * Get the max object number.
     *
     * @param deviceModelName
     * @param objectPath
     * @return
     */
    public static int getMaxInstanceIndex(String deviceModelName, String objectPath) {
        if (deviceModelName.equals("844RG")) {
            if (objectPath.endsWith("WLANConfiguration.")) {
                // Up to 16 WIFI instances
                return 16;
            }
            if (objectPath.endsWith("LANDevice.")) {
                // Up to 16 LANDevice instances
                return 16;
            }

            return 16;
        }

        //log.error("Unsupported device model " + deviceModelName + "!");
        return 16;
    }

    /**
     * Convert CPE Sn (long value) to HEX String
     * @param sn
     * @return
     */
    public static String snToHexString(long sn) {
        return String.format("%012X", sn);
    }

    /**
     * Convert CPE Sn (long value) to IP Address String
     * @param sn
     * @return
     */
    public static String snToIpAddress(long sn) {
        int ip = (int)(sn & 0xFFFFFFFF);
        int byte3 = (ip >> 24) & 0xFF;
        int byte2 = (ip >> 16) & 0xFF;
        int byte1 = (ip >> 8) & 0xFF;
        int byte0 = ip & 0xFF;

        return byte3 + "." + byte2 + "." + byte1 + "." + byte0;
    }

    /**
     * Convert CPE Sn (long value) to Subnet Mask String
     * @param sn
     * @return
     */
    public static String snToSubnetMask(long sn) {
        int ip = (int)(sn & 0xFFFFFFFF);
        int byte3 = (ip >> 24) & 0xFF;
        int byte2 = (ip >> 16) & 0xFF;
        int byte1 = (ip >> 8) & 0xFF;

        return byte3 + "." + byte2 + "." + byte1 + ".0";
    }

    /**
     * Convert CPE Sn (long value) to MAC Address String
     * @param sn
     * @return
     */
    public static String snToMacAddress(long sn) {
        long byte5 = (sn >> 40) & 0xFF;
        long byte4 = (sn >> 32) & 0xFF;
        long byte3 = (sn >> 24) & 0xFF;
        long byte2 = (sn >> 16) & 0xFF;
        long byte1 = (sn >> 8) & 0xFF;
        long byte0 = sn & 0xFF;

        return byte5 + ":" + byte4 + ":" + byte3 + ":" + byte2 + ":" + byte1 + ":" + byte0;
    }
    /**
     * Get CPE Up Time (in seconds).
     *
     * @return
     */
    public static long getUpTime() {
        return (System.currentTimeMillis() - CpeSimMainVertice.startTime) / 1000;
    }
}
