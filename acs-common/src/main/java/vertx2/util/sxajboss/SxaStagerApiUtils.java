package vertx2.util.sxajboss;

import com.calix.sxa.VertxMongoUtils;
import vertx2.model.Cpe;
import vertx2.model.CpeDeviceType;
import vertx2.model.Subscriber;
import vertx2.util.AcsConstants;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA-CC
 *
 * SXA JBoss SXA-Stager API Utils
 *
 * @author: ronyang
 */
public class SxaStagerApiUtils extends SxaJBossApiUtils{
    private static final Logger log = LoggerFactory.getLogger(SxaStagerApiUtils.class);

    /**
     * Constants
     */
    public static final String SXA_STAGER_MODULE_NAME = "sxa-stager";
    public static final String SXA_SUBSCRIBER_URL_PATH = "/sxa-subscriber/";
    public static final String SXA_DEVICE_URL_PATH = "/sxa-device/";
    public static final String USERNAME = "admin@calix.com";
    public static final String PASSWORD = "sxa-cc";
    public static final JsonObject SUBSCRIBER_QUERY_TIMED_OUT = new JsonObject().putString(
            AcsConstants.FIELD_NAME_ERROR,
            "DB Timed Out when Querying Subscriber Data"
    );

    /**
     * Static Error Objects
     */
    public static final JsonObject FAILED_TO_UPDATED_SEARCH_ENGINE = new JsonObject()
            .putString(AcsConstants.FIELD_NAME_ERROR, "Internal Error ! (Failed to updated search engine)");

    /**
     * Notify SXA Stager that a new subscriber has been discovered or updating an existing subscriber.
     * @param subscriber
     */
    public static void createOrUpdateSubscriber(
            final JsonObject subscriber,
            final Handler<Boolean> handler) {
        // Send it
        sendRequest(
            SXA_STAGER_MODULE_NAME,
            HttpMethod.PUT,
            SXA_SUBSCRIBER_URL_PATH + subscriber.getString(AcsConstants.FIELD_NAME_ID),
            USERNAME,
            PASSWORD,
            subscriber.encode(),
            new Handler<HttpClientResponse>() {
                @Override
                public void handle(HttpClientResponse response) {
                    /**
                     * Check HTTP Status Code
                     */
                    if (response.statusCode() == HttpResponseStatus.OK.code() ||
                            response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
                        log.info(subscriber.getString(AcsConstants.FIELD_NAME_NAME)
                                + ": Successfully sent Subscriber notification to SXA Stager.");
                        handler.handle(true);
                    } else {
                        log.info(subscriber.getString(AcsConstants.FIELD_NAME_NAME)
                                + ": Failed to send Subscriber notification to SXA Stager!"
                                + " HTTP Status " + response.statusCode() + " " + response.statusMessage());
                        handler.handle(false);
                    }
                }
            },
            new Handler<Throwable>() {
                @Override
                public void handle(Throwable exception) {
                    log.info(subscriber.getString(AcsConstants.FIELD_NAME_NAME)
                            + ": Failed to send Subscriber notification to SXA Stager due to"
                            + " exception " + exception.getMessage() + "!");
                    handler.handle(false);
                }
            });
    }

    /**
     * Delete Subscriber.
     *
     * @param id
     * @param handler   Custom Handler provided by caller.
     *                  On success, the string result will be null;
     *                  On failures, the string result will contain the error details.
     */
    public static void deleteSubscriber(
            final String id,
            final Handler<Boolean> handler) {
        sendRequest(
                SXA_STAGER_MODULE_NAME,
                HttpMethod.DELETE,
                SXA_SUBSCRIBER_URL_PATH + id,
                USERNAME,
                PASSWORD,
                null,
                new Handler<HttpClientResponse>() {
                    @Override
                    public void handle(HttpClientResponse response) {
                        /**
                         * Simply log the response
                         */
                        if (response.statusCode() == HttpResponseStatus.OK.code() ||
                                response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
                            log.info(id + ": Successfully sent Subscriber Delete notification to SXA Stager.");
                            handler.handle(true);
                        } else {
                            handler.handle(false);
                        }
                    }
                },
                new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable exception) {
                        log.error(id + ": Failed to send Subscriber Delete notification to SXA Stager due to"
                                + " exception " + exception.getMessage() + "!");
                        handler.handle(false);
                    }
                });
    }

    /**
     * Notify SXA Stager that a new CPE has been discovered or an existing device has been updated.
     * @param cpe
     */
    public static void deviceDiscoveryAndUpdate(EventBus eventBus, final Cpe cpe) {
        /**
         * Build request payload
         */
        JsonObject deviceData = new JsonObject()
                .putString(AcsConstants.FIELD_NAME_ID, cpe.key)
                .putString(AcsConstants.FIELD_NAME_ORG_ID, cpe.deviceId.orgId)
                .putString(Cpe.DB_FIELD_NAME_SN, cpe.deviceId.sn)
                .putString(Cpe.DB_FIELD_NAME_REGISTRATION_ID, cpe.deviceId.registrationId)
                .putString(CpeDeviceType.FIELD_NAME_HW_VER, cpe.deviceId.hwVersion)
                .putString(CpeDeviceType.FIELD_NAME_SW_VER, cpe.deviceId.swVersion)
                .putString(CpeDeviceType.FIELD_NAME_OUI, cpe.deviceId.oui)
                .putString(CpeDeviceType.FIELD_NAME_MODEL_NAME, cpe.deviceId.modelName)
                .putString(Cpe.DB_FIELD_NAME_MAC_ADDRESS, cpe.deviceId.macAddress)
                .putString(Cpe.DB_FIELD_NAME_IP_ADDRESS, cpe.deviceId.ipAddress);
        final JsonObject payload = new JsonObject().putObject("device", deviceData);

        // Query Subscriber
        Subscriber.querySubscriberData(
                eventBus,
                cpe.cpeJsonObj,
                new Handler<JsonObject>() {
                    @Override
                    public void handle(JsonObject subscriberData) {
                        if (VertxMongoUtils.FIND_ONE_TIMED_OUT.equals(subscriberData)) {
                            log.error(cpe.getCpeKey() + ": Unable to find subscriber data due to DB timeout!");
                            subscriberData = null;
                        }

                        if (subscriberData != null) {
                            // Add subscriber data if not null
                            subscriberData.removeField(AcsConstants.FIELD_NAME_CREATE_TIME);
                            payload.putObject("subscriber", subscriberData);
                        }

                        // Send API request to JBoss
                        sendRequest(
                                SXA_STAGER_MODULE_NAME,
                                HttpMethod.POST,
                                SXA_DEVICE_URL_PATH,
                                USERNAME,
                                PASSWORD,
                                payload.encode(),
                                new Handler<HttpClientResponse>() {
                                    @Override
                                    public void handle(HttpClientResponse response) {
                                        /**
                                         * Simply log the response
                                         */
                                        if (response.statusCode() == HttpResponseStatus.OK.code() ||
                                                response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
                                            log.info(cpe.getCpeKey() + ": Successfully sent CPE discovery notification to SXA Stager.");
                                        } else {
                                            log.error(cpe.getCpeKey() + ": Failed to send CPE discovery notification to SXA Stager!"
                                                    + " HTTP Status " + response.statusCode() + " " + response.statusMessage());
                                        }
                                    }
                                },
                                new Handler<Throwable>() {
                                    @Override
                                    public void handle(Throwable exception) {
                                        log.error(cpe.getCpeKey() + ": Failed to send CPE discovery notification to SXA Stager due to"
                                                + " exception " + exception.getMessage() + "!");
                                    }
                                });
                    }
                }
        );
    }

    /**
     * Notify JBoss that a Device has been deleted.
     *
     * @param deviceData
     * @param subscriberData
     * @param handler   Custom Handler provided by caller.
     *                  On success, the string result will null;
     *                  On failures, the string result will contain the error details.
     */
    public static void deleteDevice(
            final JsonObject deviceData,
            final JsonObject subscriberData,
            final Handler<JsonObject> handler) {
        final String cpeKey = deviceData.getString(AcsConstants.FIELD_NAME_ID);
        // Build payload
        final JsonObject payload = new JsonObject().putString(
                "orgId",
                deviceData.getString(AcsConstants.FIELD_NAME_ORG_ID)
        );

        if (subscriberData != null) {
            /**
             * Found the subscriber that this device is associated with
             */
            subscriberData.removeField(AcsConstants.FIELD_NAME_CREATE_TIME);
            payload.putObject("subscriber", subscriberData);
        }

        // Send API request to JBoss
        sendRequest(
                SXA_STAGER_MODULE_NAME,
                HttpMethod.DELETE,
                SXA_DEVICE_URL_PATH + cpeKey,
                USERNAME,
                PASSWORD,
                payload.encode(),
                new Handler<HttpClientResponse>() {
                    @Override
                    public void handle(HttpClientResponse response) {
                        /**
                         * Simply log the response
                         */
                        if (response.statusCode() == HttpResponseStatus.OK.code() ||
                                response.statusCode() == HttpResponseStatus.NO_CONTENT.code()) {
                            log.info(cpeKey + ": Successfully sent CPE Delete request to SXA Stager.");
                            handler.handle(payload);
                        } else {
                            log.error(cpeKey + ": Failed to send CPE Delete request to "
                                    + "SXA Stager! HTTP Status " + response.statusCode() + " "
                                    + response.statusMessage());
                            handler.handle(
                                    payload.putString(
                                            AcsConstants.FIELD_NAME_ERROR,
                                            "Internal Error! (Failed to Update Search Engine)"
                                    )
                            );
                        }
                    }
                },
                new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable exception) {
                        log.error(cpeKey + ": Failed to send CPE Delete request to " +
                                "SXA Stager due to " + exception
                                + exception.getMessage() + "!");
                        handler.handle(
                                payload.putString(
                                        AcsConstants.FIELD_NAME_ERROR,
                                        "Internal Error! (Failed to Update Search Engine)"
                                )
                        );
                    }
                }
        );
    }
}
