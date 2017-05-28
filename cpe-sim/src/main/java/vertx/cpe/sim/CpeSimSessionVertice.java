package vertx.cpe.sim;

import vertx.util.CpeDataModelMgmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.platform.Verticle;

/**
 * Project:  CPE Simulator
 *
 * CWMP Session Vertice.
 *
 * @author: ronyang
 */
public class CpeSimSessionVertice extends Verticle {
    private static final Logger log = LoggerFactory.getLogger(CpeSimSessionVertice.class.getName());

    /**
     * Event Bus
     */
    EventBus eventBus;

    /**
     * Start the Vertice
     */
    public void start() {
        log.info("Starting a CPE Session Vertice...");

        /**
         * Save event bus
         */
        eventBus = vertx.eventBus();

        /**
         * Initialize Data Models
         */
        CpeDataModelMgmt.init(vertx, "tr_data_models/");

        /**
         * Read the default CPE data
         */
        CpeSimUtils.initDefaultCpeData(vertx);

        /**
         * Register Handler for new-session events
         */
        eventBus.registerLocalHandler(
                CpeSimConstants.VERTX_ADDRESS_NEW_SESSION,
                new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> event) {
                        String sn = CpeSimUtils.snToHexString(event.body().getLong("sn"));

                        if (CpeSimUtils.allSessions.contains(sn)) {
                            log.error("Session exists for SN " + sn);
                        } else {
                            CpeSession session = new CpeSession(
                                    vertx,
                                    event.body().getObject("queryResult"),
                                    event.body().getString("orgId"),
                                    event.body().getLong("sn"),
                                    event.body().getString("eventCode"),
                                    event.body().getBoolean("newCpe", false),
                                    event.body().getObject("newValues"),
                                    event.body().getString("commandKey")
                            );
                        }
                    }
                }
        );
    }
}