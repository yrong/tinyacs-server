package vertx.cpe.sim;

import io.vertx.ext.mongo.MongoClient;
import vertx.VertxException;
import vertx.VertxMongoUtils;
import vertx.VertxUtils;
import vertx.model.Cpe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * Populate MongoDB directly with many CPE records into.
 *
 * @author: ronyang
 */
public class PopulateCpeDb {
    private static final Logger log = LoggerFactory.getLogger(PopulateCpeDb.class.getName());

    /**
     * Static Variables
     */
//    public static Vertx vertx;
    public MongoClient mongoClient;
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

    public PopulateCpeDb(MongoClient mongoClient){
        this.mongoClient = mongoClient;
    }

    /**
     * Perform the actual population with a range of CPE SNs.
     *
     * @param mongoClient
     * @param httpServerRequest
     * @param orgIdArg
     * @param ouiArg
     * @param startSn
     * @param lastSn
     */
    public  void doPopulate(
            MongoClient mongoClient,
            final HttpServerRequest httpServerRequest,
            String orgIdArg,
            String ouiArg,
            String startSn,
            final String lastSn
    ) {
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
                    mongoClient,
                    Cpe.CPE_COLLECTION_NAME,
                    CpeSimUtils.getDefaultCpeDataObjectBySn(sn, orgId, oui),
                    saveResultHandler
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Save Result Handler
     */
    public Handler saveResultHandler = new Handler<String> () {
        @Override
        public void handle(String result) {
            {
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
                                PopulateCpeDb.this.mongoClient,
                                Cpe.CPE_COLLECTION_NAME,
                                CpeSimUtils.getDefaultCpeDataObjectBySn(sn, orgId, oui),
                                saveResultHandler
                        );
                    } catch (VertxException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    };
}
