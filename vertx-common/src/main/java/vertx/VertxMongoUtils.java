package vertx;

import io.vertx.core.Vertx;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.UpdateOptions;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Date;
import java.util.UUID;

/**
 * Vertx MongoDB (mod-mongo-persistor) Related Utils.
 *
 * @author: ronyang
 */
public class VertxMongoUtils {
    private static final Logger log = LoggerFactory.getLogger(VertxMongoUtils.class.getName());

    /**
     * Default Timeout is 10 mins for updating multi docs and 60 seconds for all other cases
     */
    public static final long DEFAULT_TIMEOUT = 60000;
    public static final long DEFAULT_MULTI_TIMEOUT = 600000;
    public static final String TIMED_OUT = "MongoDB Timed Out!";
    public static final JsonObject FIND_ONE_TIMED_OUT =
            new JsonObject().put("error", "MongoDB Timed Out!");
    public static final JsonArray FIND_TIMED_OUT = new JsonArray().add(FIND_ONE_TIMED_OUT);

    /**
     * MAX # of instances of mod-mongo-persistor
     */
    public static final int MAX_INSTANCES = 16;

    /**
     * String Constants
     */
    // Collection Name
    public static final String MOD_MONGO_FIELD_NAME_COLLECTION = "collection";

    // Actions
    public static final String MOD_MONGO_FIELD_NAME_ACTION = "action";
    public static final String MOD_MONGO_FIELD_NAME_ACTION_UPDATE = "update";
    public static final String MOD_MONGO_FIELD_NAME_ACTION_SAVE = "save";
    public static final String MOD_MONGO_FIELD_NAME_ACTION_FIND = "find";
    public static final String MOD_MONGO_FIELD_NAME_ACTION_FIND_ONE = "findone";
    public static final String MOD_MONGO_FIELD_NAME_ACTION_FIND_AND_MODIFY = "find_and_modify";
    public static final String MOD_MONGO_FIELD_NAME_ACTION_DELETE = "delete";
    public static final String MOD_MONGO_FIELD_NAME_ACTION_COUNT = "count";

    // Save
    public static final String MOD_MONGO_FIELD_NAME_DOCUMENT = "document";

    // Update
    public static final String MOD_MONGO_FIELD_NAME_UPDATE_CRITERIA = "criteria";
    public static final String MOD_MONGO_FIELD_NAME_UPDATE = "objNew";
    public static final String MOD_MONGO_FIELD_NAME_NUMBER = "number";
    public static final String MOD_MONGO_FIELD_NAME_MULTI = "multi";

    // Count
    public static final String MOD_MONGO_FIELD_NAME_COUNT = "count";

    // find-and-modify
    public static final String MOD_MONGO_FIELD_NAME_FIND_MODIFY_UPDATE = "update";

    // Result Status
    public static final String MOD_MONGO_FIELD_NAME_STATUS = "status";
    public static final String MOD_MONGO_FIELD_NAME_STATUS_VALUE_OK = "ok";
    public static final String MOD_MONGO_FIELD_NAME_STATUS_VALUE_MORE_EXIST = "more-exist";
    public static final String MOD_MONGO_FIELD_NAME_RESULT = "result";
    public static final String MOD_MONGO_FIELD_NAME_RESULTS = "results";

    // Find/FindOne
    public static final String MOD_MONGO_FIELD_NAME_KEYS = "keys";
    public static final String MOD_MONGO_FIELD_NAME_ID = "_id";
    public static final String MOD_MONGO_FIELD_NAME_BATCH_SIZE = "batch_size";
    public static final String MOD_MONGO_FIELD_NAME_MATCHER = "matcher";
    public static final String MOD_MONGO_FIELD_NAME_SORT = "sort";
    public static final String MOD_MONGO_FIELD_NAME_SKIP = "skip";
    public static final String MOD_MONGO_FIELD_NAME_LIMIT = "limit";

    // MongoDB Query Operators
    public static final String MOD_MONGO_QUERY_OPERATOR_IN = "$in";
    public static final String MOD_MONGO_QUERY_OPERATOR_ALL = "$all";
    public static final String MOD_MONGO_QUERY_OPERATOR_OR = "$or";
    public static final String MOD_MONGO_QUERY_OPERATOR_ELEM_MATCH = "$elemMatch";
    public static final String MOD_MONGO_QUERY_OPERATOR_GREATER_THAN = "$gt";
    public static final String MOD_MONGO_QUERY_OPERATOR_GREATER_THAN_AND_EQUAL = "$gte";
    public static final String MOD_MONGO_QUERY_OPERATOR_LESS_THAN = "$lt";
    public static final String MOD_MONGO_QUERY_OPERATOR_LESS_THAN_AND_EQUAL = "$lte";
    public static final String MOD_MONGO_QUERY_OPERATOR_NOT_EQUAL = "$ne";
    public static final String MOD_MONGO_QUERY_OPERATOR_EXISTS = "$exists";
    public static final JsonObject EXISTS = new JsonObject().put(MOD_MONGO_QUERY_OPERATOR_EXISTS, true);
    public static final JsonObject EXISTS_FALSE =
            new JsonObject().put(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_EXISTS, false);
    public static final JsonObject GREATER_THAN_ZERO = new JsonObject().put(MOD_MONGO_QUERY_OPERATOR_GREATER_THAN, 0);


    // MongoDB Update Operators
    public static final String MOD_MONGO_UPDATE_OPERATOR_INCR = "$inc";
    public static final String MOD_MONGO_UPDATE_OPERATOR_PUSH = "$push";
    public static final String MOD_MONGO_UPDATE_OPERATOR_PULL = "$pull";
    public static final String MOD_MONGO_UPDATE_OPERATOR_SET = "$set";
    public static final String MOD_MONGO_UPDATE_OPERATOR_UNSET = "$unset";
    public static final String MOD_MONGO_UPDATE_OPERATOR_CURRENT_DATE = "$currentDate";

    // Extended Date Object
    public static final String MOD_MONGO_DATE = "$date";
    public static final VertxException INVALID_MONGO_DATE_OBJECT =
            new VertxException("Not a valid Mongo Date Object!");

    /**
     * Build mod-mongo-persistor config JSON object.
     */
    public static JsonObject getModMongoPersistorConfig() {
        JsonObject config = new JsonObject()
                .put("db_name", VertxConfigProperties.mongoDbName)
                .put("address", VertxConstants.VERTX_ADDRESS_MONGODB);

        if (VertxConfigProperties.mongoSeeds != null) {
            /**
             * Parse the "seeds" string which should be in this format:
             *
             * "192.168.1.100:27000,192.168.1.101:27001"
             */
            JsonArray seedsArray = new JsonArray();
            for (String aSeed: VertxConfigProperties.mongoSeeds.split(",")){
                int colonIndex = aSeed.indexOf(':');
                if (colonIndex < 1 || colonIndex >= (aSeed.length() - 1)) {
                    log.error("Found an invalid MongoDB Seed " + aSeed + "!");
                } else {
                    try {
                        String host = aSeed.substring(0, colonIndex);
                        int port = Integer.valueOf(aSeed.substring(colonIndex + 1));
                        JsonObject seed = new JsonObject()
                                .put("host", host)
                                .put("port", port);

                        log.info("Found a mongo seed: " + aSeed);
                        seedsArray.add(seed);
                    } catch (Exception ex) {
                        log.error("Found an invalid MongoDB Seed " + aSeed + "!");
                    }
                }
            }

            if (seedsArray.size() > 0) {
                config.put("seeds", seedsArray)
                        .put("read_preference ", "primaryPreferred");
            } else {
                config.put("host", VertxConfigProperties.mongoHost)
                        .put("port", VertxConfigProperties.mongoPort);
            }
        } else {
            config.put("host", VertxConfigProperties.mongoHost)
                    .put("port", VertxConfigProperties.mongoPort);
        }
        return config;
    }


    /**
     * Get the # of instances of mod-mongo-persistor
     * @return
     */
    public static int getNumberOfInstances() {
        if ((VertxUtils.getNumberOfCpuCores()) > MAX_INSTANCES) {
            return MAX_INSTANCES;
        } else {
            return VertxUtils.getNumberOfCpuCores();
        }
    }


    /**
     * Query MongoDB and return one or more matching records.
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param matcher
     * @param handler
     * @param keys
     * @param batchSize
     * @throws VertxException
     */
    public static void find(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            JsonObject matcher,
            Handler handler,
            /**
             * Optional Args
             */
            JsonObject keys,
            Number batchSize
    ) throws VertxException {
        find(mongoClient, collectionName, matcher, null, 0, -1, handler, keys, batchSize);
    }

    /**
     * Query MongoDB with "skip" and "limit".
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param matcher
     * @param sort
     * @param skip              A number which determines the number of documents to skip.
     * @param limit             A number which determines the maximum total number of documents to return.
     * @param handler
     * @param keys
     * @param batchSize
     * @throws VertxException
     */
    public static void find(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            JsonObject matcher,
            JsonObject sort,
            int skip,
            int limit,
            final Handler handler,
            /**
             * Optional Args
             */
            JsonObject keys,
            Number batchSize
    ) throws VertxException {
        find(
                mongoClient,
                collectionName,
                matcher,
                sort,
                skip,
                limit,
                handler,
                keys,
                batchSize,
                matcher.size() > 0
        );
    }

    /**
     * Query MongoDB with "skip" and "limit".
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param matcher
     * @param sort
     * @param skip              A number which determines the number of documents to skip.
     * @param limit             A number which determines the maximum total number of documents to return.
     * @param handler
     * @param keys
     * @param batchSize
     * @param bDebugPrint
     * @throws VertxException
     */
    public static void find(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            JsonObject matcher,
            JsonObject sort,
            int skip,
            int limit,
            final Handler handler,
            /**
             * Optional Args
             */
            JsonObject keys,
            Number batchSize,
            boolean bDebugPrint
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (mongoClient == null || collectionName == null || matcher == null || handler == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        FindOptions options = new FindOptions();
        if (sort != null) {
            options.setSort(sort);
        }
        if (skip > 0) {
            options.setSkip(skip);
        }
        if (limit > 0) {
            options.setLimit(limit);
        }
        if (keys != null) {
            options.setFields(keys);
        }

        mongoClient.findWithOptions(collectionName,matcher,options,res->{
            if (res.succeeded()) {
                handler.handle(res.result());
            } else {
                res.cause().printStackTrace();
                handler.handle(null);
            }
        });
    }

    /**
     * Query MongoDB and return the first matching record.
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param matcher
     * @param handler
     * @param keys
     * @throws VertxException
     */
    public static void findOne(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            JsonObject matcher,
            final Handler handler,
            /**
             * Optional Arg(s)
             */
            JsonObject keys
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (mongoClient == null || collectionName == null || matcher == null || handler == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the find message
         */
        mongoClient.findOne(collectionName,matcher,keys,res->{
            if (res.succeeded()) {
                handler.handle(res.result());
            } else {
                res.cause().printStackTrace();
                handler.handle(null);
            }
        });

    }

    /**
     * Save a document which may result in creating a new document or replacing an existing document.
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param document
     * @param customHandler
     * @throws VertxException
     */
    public static void save(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            final String collectionName,
            final JsonObject document,
            /**
             * Optional Custom Handler
             */
            Handler customHandler
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (mongoClient == null || collectionName == null || document == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        mongoClient.save(collectionName,document,res->{
            if (res.succeeded()) {
                if(customHandler!=null){
                    customHandler.handle(res.result());
                }
            } else {
                res.cause().printStackTrace();
            }
        });
    }

    /**
     * Update an existing document by id.
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param id
     * @param update
     * @param customHandler
     * @throws VertxException
     */
    public static void update(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            String id,
            JsonObject update,
            /**
             * Optional Custom Handler
             */
            Handler<Long> customHandler
    ) throws VertxException {
        updateWithMatcher(
                mongoClient,
                collectionName,
                new JsonObject().put(MOD_MONGO_FIELD_NAME_ID, id),
                update,
                DEFAULT_TIMEOUT,
                customHandler
        );
    }

    /**
     * Update one or more existing documents by a custom matcher with default timeout.
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param matcher
     * @param update
     * @param customHandler
     * @throws VertxException
     */
    public static void updateWithMatcher(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            JsonObject matcher,
            JsonObject update,
            /**
             * Optional Custom Handler
             */
            Handler<Long> customHandler
    ) throws VertxException {
        updateWithMatcher(
                mongoClient,
                collectionName,
                matcher,
                update,
                DEFAULT_TIMEOUT,
                customHandler
        );
    }

    /**
     * Update an existing document by a custom matcher.
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param matcher
     * @param update
     * @param customHandler
     * @throws VertxException
     */
    public static void updateWithMatcher(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            JsonObject matcher,
            JsonObject update,
            long timeout,
            /**
             * Optional Custom Handler
             */
            Handler<Long> customHandler
    ) throws VertxException {
        updateWithMatcher(
                mongoClient,
                collectionName,
                matcher,
                update,
                null,
                timeout,
                customHandler
        );
    }

    /**
     * Update an existing document by a custom matcher.
     *
     * TODO: Add exception handler.
     *
     * @param eventBus
     * @param collectionName
     * @param matcher
     * @param update
     * @param customHandler
     * @throws VertxException
     */
    public static void updateWithMatcher(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            JsonObject matcher,
            JsonObject update,
            JsonObject binaryFields,
            long timeout,
            /**
             * Optional Custom Handler
             */
            Handler<Long> customHandler
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (mongoClient == null || collectionName == null || matcher == null || update == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the update message
         */
        UpdateOptions options = new UpdateOptions();
        if (!matcher.containsKey(MOD_MONGO_FIELD_NAME_ID)) {
            options.setMulti(true);
        }
        mongoClient.updateCollectionWithOptions(collectionName,matcher,update,options,res->{
            if (res.succeeded()) {
                if(customHandler!=null){
                    customHandler.handle(res.result().getDocModified());
                }
            } else {
                res.cause().printStackTrace();
            }
        });
    }

    /**
     * Delete one existing document by id.
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param id
     * @param customHandler
     * @throws VertxException
     */
    public static void delete(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            final String id,
            /**
             * Optional Custom Handler
             */
            Handler<JsonObject> customHandler
    ) throws VertxException {
        deleteWithMatcher(
                mongoClient,
                collectionName,
                new JsonObject().put(MOD_MONGO_FIELD_NAME_ID, id),
                customHandler
        );
    }

    /**
     * Delete one existing document by id.
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param matcher
     * @param customHandler
     * @throws VertxException
     */
    public static void deleteWithMatcher(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            final String collectionName,
            final JsonObject matcher,
            /**
             * Optional Custom Handler
             */
            Handler<JsonObject> customHandler
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (mongoClient == null || collectionName == null || matcher == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the delete message
         */
        mongoClient.findOneAndDelete(collectionName,matcher,res->{
            if (res.succeeded()) {
                if(customHandler!=null){
                    customHandler.handle(res.result());
                }
            } else {
                res.cause().printStackTrace();
            }
        });
    }

    /**
     * Get count by matcher
     *
     * TODO: Add exception handler.
     *
     * @param mongoClient
     * @param collectionName
     * @param matcher
     * @param customHandler
     * @throws VertxException
     */
    public static void count(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            final JsonObject matcher,
            /**
             * Optional Custom Handler
             */
            Handler<Long> customHandler
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (mongoClient == null || collectionName == null || matcher == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the delete message
         */
        mongoClient.count(collectionName,matcher,res->{
            if (res.succeeded()) {
                if(customHandler!=null){
                    customHandler.handle(res.result());
                }
            } else {
                res.cause().printStackTrace();
            }
        });
    }

    /**
     * Atomic Find-and-Modify.
     *
     * @param mongoClient
     * @param collectionName
     * @param matcher
     * @param handler
     * @param updates
     * @throws VertxException
     */
    public static void findAndModify(
            /**
             * Mandatory Args
             */
            MongoClient mongoClient,
            String collectionName,
            JsonObject matcher,
            JsonObject updates,
            Handler handler
    ) throws VertxException {
        if (mongoClient == null || collectionName == null || matcher == null || handler == null) {
            throw new VertxException("Null Pointer(s)!");
        }
        mongoClient.updateCollection(collectionName,matcher,updates,res->{
            if (res.succeeded()) {
                handler.handle(res.result());
            } else {
                res.cause().printStackTrace();
            }
        });
    }


    /**
     * Build a JSON Object for $set operator, or append a new field to the existing JSON object if any.
     *
     * @param base
     * @param name
     * @param value
     * @return
     */
    public static JsonObject addSet(JsonObject base, String name, Object value) {
        return VertxJsonUtils.append(base, name, value);
    }

    /**
     * Build a JSON Object for $unset operator, or append a new field to the existing JSON object if any.
     * @param base
     * @param name
     * @return
     */
    public static JsonObject addUnset(JsonObject base, String name) {
        return VertxJsonUtils.append(base, name, "");
    }

    /**
     * Build a JSON Object for $pull operator
     *
     * @param base
     * @param name
     * @param value
     * @return
     */
    public static JsonObject addPull(JsonObject base, String name, Object value) {
        return VertxJsonUtils.append(base, name, value);
    }

    /**
     * Build a JSON Object for $push operator
     *
     * @param base
     * @param name
     * @param value
     * @return
     */
    public static JsonObject addPush(JsonObject base, String name, Object value) {
        return VertxJsonUtils.append(base, name, value);
    }

    /**
     * Build a JSON Object for $currentDate operator, or append a new field to the existing JSON object if any.
     * @param base
     * @param name
     * @return
     */
    public static JsonObject addTimeStamp(JsonObject base, String name) {
        return VertxJsonUtils.append(base, name, true);
    }

    /**
     * Build a JSON Object that contains $set/$unset/$currentDate.
     *
     * @param sets
     * @param unsets
     * @param timestamps
     * @param pulls
     * @param pushes
     * @return
     */
    public static JsonObject getUpdatesObject(
            JsonObject sets,
            JsonObject unsets,
            JsonObject timestamps,
            JsonObject pulls,
            JsonObject pushes) {
        if (sets != null || unsets != null || timestamps != null || pulls != null || pushes != null) {
            JsonObject updates = new JsonObject();
            if (sets != null) {
                updates.put(MOD_MONGO_UPDATE_OPERATOR_SET, sets);
            }
            if (unsets != null) {
                updates.put(MOD_MONGO_UPDATE_OPERATOR_UNSET, unsets);
            }
            if (timestamps != null) {
                updates.put(MOD_MONGO_UPDATE_OPERATOR_CURRENT_DATE, timestamps);
            }
            if (pulls != null) {
                updates.put(MOD_MONGO_UPDATE_OPERATOR_PULL, pulls);
            }
            if (pushes != null) {
                updates.put(MOD_MONGO_UPDATE_OPERATOR_PUSH, pushes);
            }

            return updates;
        } else {
            return null;
        }
    }

    /**
     * Build a MongoDB "$date" JSON Object.
     *
     * We need to give MongoDB a hint that what we are passing is a date by specifying the type in the message.
     *
     * You can specify that an element is a date using a message like this:
     *
     * { “<document_attribute>”:  { “$date”:  “<date_value>” }
     *
     * Where <document_attribute> is the attribute that you want to be stored as a date in MongoDB. <date_value> is
     * either a number representing the number of milliseconds since January 1, 1970, 00:00:00 GMT or a string
     * representing a date in ISO-8601 format. The $date tag tells MongoDB that the number or string should be
     * converted to a date type when stored.
     *
     * So, instead of sending:
     *
     * {“account_id”: “444XYZ”, “start_date”: “2014-09-18T10:32:59.000+0000″}
     *
     * We would be sending:
     *
     * {“account_id”: “444XYZ”, “start_date”: {“$date“: 1411101068495}}
     */
    // Get a Date Object for the current date/time
    public static JsonObject getDateObject() {
        return new JsonObject().put(MOD_MONGO_DATE, System.currentTimeMillis());
    }
    // Get a Date Object for a custom date/time
    public static JsonObject getDateObject(long customDateTime) {
        return new JsonObject().put(MOD_MONGO_DATE, customDateTime);
    }

    /**
     * Get a Java Date POJO with a MongoDB Date Object.
     * @param mongoDateObject
     * @return
     * @throws VertxException
     */
    public static Date getDateFromMongoDateObject(JsonObject mongoDateObject) throws VertxException {
        if (mongoDateObject == null || !mongoDateObject.containsKey(MOD_MONGO_DATE)) {
            log.error("getDateFromMongoDateObject(): Null Pointer!");
            throw VertxException.NULL_POINTER;
        }
        if (!mongoDateObject.containsKey(MOD_MONGO_DATE)) {
            log.error("getDateFromMongoDateObject(): Not a valid Mongo Date Object!");
            throw INVALID_MONGO_DATE_OBJECT;
        }

        try {
            String isoDateString = mongoDateObject.getString(MOD_MONGO_DATE);
            return VertxJsonUtils.iso8601StringToDate(isoDateString);
        } catch (Exception ex) {
            log.error("getDateFromMongoDateObject(): Mongo Date Object contain illegal ISO8601 date string!");
            throw INVALID_MONGO_DATE_OBJECT;
        }
    }

    /**
     * Return true if the given String is an UUID, or false.
     * @param id
     * @return
     */
    public static boolean isUuid(String id) {
        try {
            UUID uuid = UUID.fromString(id);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * Return true if the given String is an UUID, or false.
     * @param id
     * @return
     */
    public static boolean isObjectId(String id) {
        return ObjectId.isValid(id);
    }
}
