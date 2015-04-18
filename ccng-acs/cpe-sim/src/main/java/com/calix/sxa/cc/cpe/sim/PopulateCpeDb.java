package com.calix.sxa.cc.cpe.sim;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxMongoUtils;
import com.calix.sxa.VertxUtils;
import com.calix.sxa.cc.model.Cpe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA-CC
 *
 * Populate MongoDB directly with many CPE records into.
 *
 * @author: jqin
 */
public class PopulateCpeDb {
    private static final Logger log = LoggerFactory.getLogger(PopulateCpeDb.class.getName());

    /**
     * Static Variables
     */
    public static Vertx vertx;
    public static HttpServerRequest httpRequest;
    public static long start;
    public static long end;
    public static String orgId;
    public static String oui;


    /**
     * Counters
     */
    static long consecutiveFailures;
    static long successCount;
    static long failCount;
    static long sn;

    /**
     * Perform the actual population with a range of CPE SNs.
     *
     * @param vertxInstance
     * @param httpServerRequest
     * @param orgIdArg
     * @param ouiArg
     * @param startSn
     * @param lastSn
     */
    public static void doPopulate(
            Vertx vertxInstance,
            final HttpServerRequest httpServerRequest,
            String orgIdArg,
            String ouiArg,
            String startSn,
            final String lastSn
    ) {
        vertx = vertxInstance;
        oui = ouiArg;
        orgId = orgIdArg;

        // Set to chunk mode
        httpRequest = httpServerRequest;
        httpRequest.response().setChunked(true);
        httpRequest.response().putHeader("Content-Type", "text/plain");

        /**
         * Convert SN to hex value
         */
        start = Long.decode("0x" + startSn);
        end = Long.decode("0x" + lastSn);
        sn = start;

        consecutiveFailures = 0;
        successCount = 0;
        failCount = 0;

        // Start the loop
        try {
            VertxMongoUtils.save(
                    vertx.eventBus(),
                    Cpe.CPE_COLLECTION_NAME,
                    CpeSimUtils.getDefaultCpeDataObjectBySn(sn, orgId, oui),
                    saveResultHandler
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Save Result Handler
     */
    public static Handler<Message<JsonObject>> saveResultHandler = new Handler<Message<JsonObject>> () {
        @Override
        public void handle(Message<JsonObject> result) {
            if (result == null || result.body() == null
                    || !VertxMongoUtils.MOD_MONGO_FIELD_NAME_STATUS_VALUE_OK.equals(
                    result.body().getString(VertxMongoUtils.MOD_MONGO_FIELD_NAME_STATUS))
                    ) {
                // Failed
                String error = "Failed to insert CPE record with sn " + CpeSimUtils.snToHexString(sn) + "!";
                log.error(error);
                httpRequest.response().write(error + "\n");

                consecutiveFailures ++;
                failCount ++;
                if (consecutiveFailures > 3) {
                    // Quit
                    error = "Quitting due to 3 consecutive failures.";
                    log.error(error);
                    httpRequest.response().write(error + "\n");
                    httpRequest.response().end();
                }
            } else {
                // Succeeded
                String response = "Inserted CPE record for sn " + CpeSimUtils.snToHexString(sn) + ".";
                log.info(response);
                httpRequest.response().write(response + "\n");

                sn ++;
                successCount ++;
                consecutiveFailures = 0;
                if (sn > end) {
                    // Done
                    response = "\n"
                            + VertxUtils.LINE_OF_HASHES
                            + VertxUtils.LINE_OF_HASHES
                            + VertxUtils.encloseStrWithHashes("Summary:")
                            + VertxUtils.encloseStrWithHashes("# of CPE records inserted: " + successCount)
                            + VertxUtils.encloseStrWithHashes("# of failures:             " + failCount)
                            + VertxUtils.LINE_OF_HASHES
                            + VertxUtils.LINE_OF_HASHES
                    ;
                    log.error(response);
                    httpRequest.response().write(response + "\n");
                    httpRequest.response().end();
                } else {
                    // Insert the next one
                    try {
                        VertxMongoUtils.save(
                                vertx.eventBus(),
                                Cpe.CPE_COLLECTION_NAME,
                                CpeSimUtils.getDefaultCpeDataObjectBySn(sn, orgId, oui),
                                saveResultHandler
                        );
                    } catch (SxaVertxException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    };
}
