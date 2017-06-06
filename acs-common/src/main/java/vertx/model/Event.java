package vertx.model;

import io.vertx.ext.mongo.MongoClient;
import vertx.VertxException;
import vertx.VertxMongoUtils;
import vertx.util.AcsConstants;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp (aka CCFG)
 *
 * Event Data model.
 *
 * @author: ronyang
 */
public class Event {
    /**
     * DB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "CWMP-events";

    /**
     * Field Name Constants
     */
    public static final String FIELD_NAME_TIMESTAMP = "timestamp";
    public static final String FIELD_NAME_DEVICE_SN = "deviceSn";
    public static final String FIELD_NAME_TYPE = "type";
    public static final String FIELD_NAME_SEVERITY = "severity";
    public static final String FIELD_NAME_SOURCE = "source";
    public static final String FIELD_NAME_DETAILS = "details";

    /**
     * Save an event
     */
    public static void saveEvent(
            MongoClient mongoClient,
            String orgId,
            String deviceSn,
            EventTypeEnum eventType,
            EventSourceEnum source,
            JsonObject details) {
        JsonObject jsonObject = new JsonObject()
                .put(AcsConstants.FIELD_NAME_ORG_ID, orgId)
                .put(FIELD_NAME_TIMESTAMP, VertxMongoUtils.getDateObject())
                .put(FIELD_NAME_TYPE, eventType.typeString)
                .put(FIELD_NAME_SEVERITY, eventType.severity.name())
                .put(FIELD_NAME_SOURCE, source.name());

        if (details != null) {
            jsonObject.put(FIELD_NAME_DETAILS, details);
        }
        if (deviceSn != null) {
            jsonObject.put(FIELD_NAME_DEVICE_SN, deviceSn);
        }

        // Persist it
        //log.debug("Persisting a " + type + " ...");
        try {
            VertxMongoUtils.save(
                    mongoClient,
                    DB_COLLECTION_NAME,
                    jsonObject,
                    null
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }
}
