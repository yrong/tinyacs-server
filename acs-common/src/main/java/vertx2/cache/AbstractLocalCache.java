package vertx2.cache;

import vertx2.VertxException;
import vertx2.VertxMongoUtils;
import vertx2.model.AcsApiCrudTypeEnum;
import vertx2.util.AcsConstants;
import vertx2.util.AcsMiscUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;

/**
 * Project:  cwmp
 * 
 * Abstract Local Cache which can be extended to implement any type of local cache.
 *
 * @author: ronyang
 */
public abstract class AbstractLocalCache {
    public final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * The Type (Class) of the cached objects
     */
    public String cachedObjectType;

    /**
     * DB Collection Name
     */
    public String dbCollectionName;

    /**
     * A HashMap that contains all the raw JSON Objects.
     *
     * The Index is a string, which could be the org id or name or URL.
     */
    public HashMap<String, JsonObject> rawJsonObjectHashMap;

    /**
     * A HashMap that contains all POJOs.
     * 
     * The Index is a string, which could be the org id or name or URL.
     */
    public HashMap<String, Object> hashMap;

    /**
     * A tmp hash map used when doing refresh.
     */
    public HashMap<String, JsonObject> tmpHashMap;

    /**
     * Static Vertx Instance
     */
    public Vertx vertx;

    /**
     * DB Query Result Handler
     */
    public VertxMongoUtils.FindHandler mongoQueryResultHandler;

    /**
     * Constructor.
     *
     * @param vertx
     * @param crudEventAddress
     * @param dbCollectionName
     * @param cachedObjectType
     */
    public AbstractLocalCache(
            Vertx vertx,
            String crudEventAddress,
            final String dbCollectionName,
            final String cachedObjectType) {
        this.vertx = vertx;
        this.rawJsonObjectHashMap = new HashMap<>();
        this.tmpHashMap = new HashMap<>();
        this.cachedObjectType = cachedObjectType;
        this.dbCollectionName = dbCollectionName;
        this.hashMap = new HashMap<>();

        /**
         * Register Group CRUD Event Handler
         */
        log.info("Registering event handler for " + crudEventAddress);
        vertx.eventBus().registerHandler(crudEventAddress, getCrudEventHandler());

        // Initialize DB Query Result Handler
        mongoQueryResultHandler = getMongoQueryResultHandler();

        // Initialize the cache by querying the DB
        refresh();

        // Also start a timer to refresh the whole cache every 10 minutes
        vertx.setPeriodic(
                getRefreshInterval(),
                new Handler<Long>() {
                    @Override
                    public void handle(Long event) {
                        refresh();
                    }
                }
        );
    }

    /**
     * Build actual index String by the raw JSON Object.
     *
     * @param jsonObject
     */
    public abstract String getIndexString(JsonObject jsonObject);

    /**
     * Get a POJO by JSON Object.
     *
     * @param jsonObject
     */
    public abstract Object getPojoByJsonObject(JsonObject jsonObject) throws Exception;

    /**
     * Refresh the Cache
     */
    public void refresh() {
        //Make the query batch size large enough to return all results in one batch
        Integer batchSize = null;
        if (rawJsonObjectHashMap.size() > 0) {
            batchSize = Math.max(100, rawJsonObjectHashMap.size() * 2);
        }

        try {
            VertxMongoUtils.find(
                    vertx.eventBus(),
                    dbCollectionName,
                    getDbQueryMatcher(),
                    null,
                    0,
                    -1,
                    mongoQueryResultHandler,
                    null,
                    batchSize,
                    false);
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get DB Query Matcher
     */
    public static final JsonObject DEFAULT_EMPTY_MATCHER = new JsonObject();
    public JsonObject getDbQueryMatcher() {
        return DEFAULT_EMPTY_MATCHER;
    }

    /**
     * Get Cache Refresh Interval which is between 10-20 minutes.
     */
    public static final int MAX_REFRESH_INTERVAL = 1200000;
    public static final int MIN_REFRESH_INTERVAL = 600000;
    public long getRefreshInterval() {
        long interval = AcsMiscUtils.randInt(MIN_REFRESH_INTERVAL, MAX_REFRESH_INTERVAL);
        log.info("Refresh Interval: " + interval + " milliseconds.");
        return interval;
    }

    /**
     * Return the Default CRUD Event Handler
     * @return
     */
    public Handler<Message<JsonObject>> getCrudEventHandler() {
        return  new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                JsonObject crudEvent = event.body();

                /**
                 * Extract CRUD Type
                 */
                String crudTypeString = crudEvent.getString(AcsConstants.FIELD_NAME_ACS_CRUD_TYPE);
                if (crudTypeString == null) {
                    log.error("No CRUD Type Found! Invalid " + cachedObjectType
                            + " CRUD Event:\n" + crudEvent.encodePrettily());
                    return;
                }
                AcsApiCrudTypeEnum crudType = AcsApiCrudTypeEnum.getCrudTypeEnumByNameString(crudTypeString);
                log.info("Received a " + cachedObjectType + " " + crudType.name() + " Event:\n"
                        + crudEvent.encodePrettily());
                crudEvent.removeField(AcsConstants.FIELD_NAME_ACS_CRUD_TYPE);

                // Get index value
                String index = getIndexString(crudEvent);

                /**
                 * Check CRUD Type
                 */
                switch (crudType) {
                    case Create:
                    case Update:
                        try {
                            Object pojo = getPojoByJsonObject(crudEvent);
                            if (pojo != null) {
                                hashMap.put(index, pojo);
                                rawJsonObjectHashMap.put(index, convertRawJsonObject(crudEvent));
                            }
                        } catch (Exception e) {
                            log.error("Failed to create a new cached object due to exception "
                                    + e.getMessage() + "! CRUD Event Details:\n" + crudEvent.encodePrettily());
                            e.printStackTrace();
                        }
                        break;

                    case Delete:
                        if (index != null) {
                            hashMap.remove(index);
                            rawJsonObjectHashMap.remove(index);
                        } else {
                            /**
                             * Bulk delete or delete by orgId+name
                             *
                             * Simply refresh the entire cache
                             */
                            refresh();
                        }
                }
            }
        };
    }

    /**
     * Get an instance of MongoDB Query Result Handler
     * @return
     */
    public VertxMongoUtils.FindHandler getMongoQueryResultHandler() {
        return new MongoQueryResultHandler();
    }

    /**
     * Inner Class for MongoDB Query Result Handler
     */
    public class MongoQueryResultHandler extends VertxMongoUtils.FindHandler {
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            super.handle(jsonObjectMessage);

            // Do nothing if MongoDB timed out
            if (queryResults == null || VertxMongoUtils.FIND_TIMED_OUT.equals(queryResults)) {
                return;
            }

            //log.debug("Refreshing cache with DB query result from collection " + dbCollectionName + "...");
            for (int i = 0; i < queryResults.size(); i++) {
                JsonObject jsonObject = queryResults.get(i);
                String index = getIndexString(jsonObject);
                if (index == null) {
                    log.error("Cannot get index string out of Query Result!\n"
                            + jsonObject.encodePrettily());
                } else {
                    tmpHashMap.put(index, convertRawJsonObject(jsonObject));
                }
            }

            if (moreExist == false) {
                // Compare hashMap and tmpHashMap
                // Check for unexpected deletion
                for (String index : hashMap.keySet().toArray(new String[0])) {
                    if (tmpHashMap.containsKey(index) == false) {
                        log.info("Deleting key " + index + " from cache..");
                        hashMap.remove(index);
                    }
                }
                // Check for unexpected creation
                for (String index : tmpHashMap.keySet().toArray(new String[0])) {
                    if (hashMap.containsKey(index) == false) {
                        log.info("Adding key " + index + " to cache..");
                        try {
                            hashMap.put(index, getPojoByJsonObject(tmpHashMap.get(index)));
                            rawJsonObjectHashMap.put(index, tmpHashMap.get(index));
                        } catch (Exception e) {
                            log.error("Failed to convert DB object to a " + cachedObjectType + " POJO due to "
                                    + e.getMessage() + "! DB Object Details:\n"
                                    + tmpHashMap.get(index).encodePrettily());
                            e.printStackTrace();
                        }
                    } else {
                        // Replace if changed
                        if(!tmpHashMap.get(index).equals(rawJsonObjectHashMap.get(index))) {
                            log.info("Replacing key " + index + " ..");
                            log.info("Old record: \n" + rawJsonObjectHashMap.get(index).encodePrettily());
                            log.info("New record: \n" + tmpHashMap.get(index).encodePrettily());
                            try {
                                hashMap.put(index, getPojoByJsonObject(tmpHashMap.get(index)));
                                rawJsonObjectHashMap.put(index, tmpHashMap.get(index));
                            } catch (Exception e) {
                                log.error("Failed to convert DB object to a " + cachedObjectType + " POJO!\n"
                                        + tmpHashMap.get(index).encodePrettily());
                            }
                        }
                    }
                }

                // clean up as tmpHashMap is no longer needed
                tmpHashMap.clear();
            }
        }
    }

    /**
     * Convert raw JSON Objects before saving into rawJsonObjectHashMap.
     *
     * Default to no action.
     *
     * @param rawJsonObject
     */
    public JsonObject convertRawJsonObject(JsonObject rawJsonObject) {
       return rawJsonObject;
    }

    /**
     * Continent Get Method.
     * @param index
     * @param <T>
     * @return
     */
    public <T> T get(String index) {
        return (T)hashMap.get(index);
    }
}
