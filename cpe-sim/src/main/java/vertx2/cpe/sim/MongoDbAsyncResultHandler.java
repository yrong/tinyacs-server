package vertx2.cpe.sim;

import vertx2.cwmp.CwmpInformEventCodes;
import vertx2.model.Cpe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  sxa-cc-parent
 *
 * @author: ronyang
 */
public class MongoDbAsyncResultHandler implements Handler<Message<JsonObject>> {
    private static final Logger log = LoggerFactory.getLogger(MongoDbAsyncResultHandler.class.getName());

    /**
     * Vertx
     */
    Vertx vertx;

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
     * @param vertx
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
            HttpServerRequest request,
            JsonObject newValues,
            String eventCode,
            long baseSn,
            String orgId,
            String oui,
            long sn) {
        this.vertx = vertx;
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
        boolean isUpdate = result.body().containsField("number");
        if (isUpdate) {
            /**
             * Handle update result
             */
            CpeSimUtils.findCpeById(
                    vertx.eventBus(),
                    cpeKey,
                    this
            );
        } else {
            /**
             * Handle query result
             */
            JsonObject queryResult = result.body().getObject("result");
            if (queryResult == null) {
                // Create a new empty CPE in DB
                String cpeKey = Cpe.getCpeKey(orgId, "000631", CpeSimUtils.snToHexString(sn));
                CpeSimUtils.persistCpe(
                        vertx.eventBus(),
                        cpeKey,
                        new JsonObject().putString("_id", cpeKey),
                        true
                );
            }

            log.info("Starting CWMP Session for CPE " + CpeSimUtils.snToHexString(sn) + "...");
            JsonObject message = new JsonObject()
                    .putString("orgId", orgId)
                    .putNumber("sn", sn)
                    .putString("eventCode", eventCode);
            if (CwmpInformEventCodes.VALUE_CHANGE.equals(eventCode)) {
                message.putObject("newValues", newValues);
            }
            if (queryResult != null) {
                message.putObject("queryResult", queryResult);
            }

            // Start a new session by sending an event to one of the session verticles
            vertx.eventBus().send(CpeSimConstants.VERTX_ADDRESS_NEW_SESSION, message);

            // Perform another query or update (thus triggers another session) if needed
            if (lastSn != null && sn < lastSn) {
                MongoDbAsyncResultHandler mongoHandler = new MongoDbAsyncResultHandler(
                        vertx,
                        request,
                        newValues,
                        eventCode,
                        baseSn,
                        orgId,
                        oui,
                        sn + 1  //  increase the SN
                );

                if (CwmpInformEventCodes.VALUE_CHANGE.equals(eventCode)) {
                    CpeSimUtils.updateCpeById(vertx.eventBus(), cpeKey, newValues, mongoHandler);
                } else {
                    CpeSimUtils.findCpeById(vertx.eventBus(), cpeKey, mongoHandler);
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
