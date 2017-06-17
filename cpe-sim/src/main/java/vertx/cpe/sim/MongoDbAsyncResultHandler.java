package vertx.cpe.sim;

import io.vertx.ext.mongo.MongoClient;
import vertx.cwmp.CwmpInformEventCodes;
import vertx.model.Cpe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp-parent
 *
 * @author: ronyang
 */
public class MongoDbAsyncResultHandler implements Handler<Message<JsonObject>> {
    private static final Logger log = LoggerFactory.getLogger(MongoDbAsyncResultHandler.class.getName());


    Vertx vertx;

    MongoClient mongoClient;

    /**
     * HTTP Request
     */
    HttpServerRequest request;

    /**
     * Request Payload (in JSON Format)
     *
     * Only applicable for "value-change".
     */
    JsonObject newValues;

    /**
     * CWMP Event Code
     */
    String eventCode;

    /**
     * Base SN
     */
    long baseSn;

    /**
     * CPE OrgId/OUI/SN
     */
    String orgId;
    String oui;
    long sn;

    /**
     * CPE Key (i.e. "_id")
     */
    String cpeKey;

    /**
     * Last SN
     */
    Long lastSn = null;

    /**
     * Constructor.
     *
     * @param mongoClient
     * @param request
     * @param newValues
     * @param eventCode
     * @param baseSn
     * @param orgId
     * @param oui
     * @param sn
     */
    public MongoDbAsyncResultHandler(
            Vertx vertx,
            MongoClient mongoClient,
            HttpServerRequest request,
            JsonObject newValues,
            String eventCode,
            long baseSn,
            String orgId,
            String oui,
            long sn) {
        this.vertx = vertx;
        this.mongoClient = mongoClient;
        this.request = request;
        this.eventCode = eventCode;
        this.baseSn = baseSn;
        this.orgId = orgId;
        this.oui = oui;
        this.sn = sn;
        this.newValues = newValues;
        this.cpeKey = Cpe.getCpeKey(orgId, oui, CpeSimUtils.snToHexString(sn));

        // Extract last SN from query parameters if any
        if (request.params() != null && request.params().size() > 0) {
            String lastSnHexStr = request.params().get("lastSn");
            if (lastSnHexStr != null) {
                lastSn = Long.decode("0x" + lastSnHexStr);
            }
        }

        // Check new values for value change events
        if (CwmpInformEventCodes.VALUE_CHANGE.equals(eventCode)) {
        }
    }

    /**
     * Actual Handler Body.
     *
     * @param result
     */
    @Override
    public void handle(Message<JsonObject> result) {
        /**
         * Extract Status ("ok" vs. "error")
         */
        String status = result.body().getString("status");

        /**
         * Update result has a field called "number".
         */
        boolean isUpdate = result.body().containsKey("number");
        if (isUpdate) {
            /**
             * Handle update result
             */
            CpeSimUtils.findCpeById(
                    mongoClient,
                    cpeKey,
                    this
            );
        } else {
            /**
             * Handle query result
             */
            JsonObject queryResult = result.body().getJsonObject("result");
            if (queryResult == null) {
                // Create a new empty CPE in DB
                String cpeKey = Cpe.getCpeKey(orgId, "000631", CpeSimUtils.snToHexString(sn));
                CpeSimUtils.persistCpe(
                        mongoClient,
                        cpeKey,
                        new JsonObject().put("_id", cpeKey),
                        true
                );
            }

            log.info("Starting CWMP Session for CPE " + CpeSimUtils.snToHexString(sn) + "...");
            JsonObject message = new JsonObject()
                    .put("orgId", orgId)
                    .put("sn", sn)
                    .put("eventCode", eventCode);
            if (CwmpInformEventCodes.VALUE_CHANGE.equals(eventCode)) {
                message.put("newValues", newValues);
            }
            if (queryResult != null) {
                message.put("queryResult", queryResult);
            }

            // Start a new session by sending an event to one of the session verticles
            vertx.eventBus().send(CpeSimConstants.VERTX_ADDRESS_NEW_SESSION, message);

            // Perform another query or update (thus triggers another session) if needed
            if (lastSn != null && sn < lastSn) {
                MongoDbAsyncResultHandler mongoHandler = new MongoDbAsyncResultHandler(
                        vertx,
                        mongoClient,
                        request,
                        newValues,
                        eventCode,
                        baseSn,
                        orgId,
                        oui,
                        sn + 1  //  increase the SN
                );

                if (CwmpInformEventCodes.VALUE_CHANGE.equals(eventCode)) {
                    CpeSimUtils.updateCpeById(mongoClient, cpeKey, newValues, mongoHandler);
                } else {
                    CpeSimUtils.findCpeById(mongoClient, cpeKey, mongoHandler);
                }
            } else {
                // Send HTTP response
                long numberOfSessions = 1;
                if (lastSn != null) {
                    numberOfSessions = lastSn - baseSn + 1;
                }
                request.response().end("Started " + numberOfSessions + " session(s).\n");
            }
        }
    }
}
