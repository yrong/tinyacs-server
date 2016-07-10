package vertx2;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

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
            new JsonObject().putString("error", "MongoDB Timed Out!");
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
    public static final JsonObject EXISTS = new JsonObject().putBoolean(MOD_MONGO_QUERY_OPERATOR_EXISTS, true);
    public static final JsonObject EXISTS_FALSE =
            new JsonObject().putBoolean(VertxMongoUtils.MOD_MONGO_QUERY_OPERATOR_EXISTS, false);
    public static final JsonObject GREATER_THAN_ZERO = new JsonObject().putNumber(MOD_MONGO_QUERY_OPERATOR_GREATER_THAN, 0);


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
                .putString("db_name", VertxConfigProperties.mongoDbName)
                .putString("address", VertxConstants.VERTX_ADDRESS_MONGODB);

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
                                .putString("host", host)
                                .putNumber("port", port);

                        log.info("Found a mongo seed: " + aSeed);
                        seedsArray.add(seed);
                    } catch (Exception ex) {
                        log.error("Found an invalid MongoDB Seed " + aSeed + "!");
                    }
                }
            }

            if (seedsArray.size() > 0) {
                config.putArray("seeds", seedsArray)
                        .putString("read_preference ", "primaryPreferred");
            } else {
                config.putString("host", VertxConfigProperties.mongoHost)
                        .putNumber("port", VertxConfigProperties.mongoPort);
            }
        } else {
            config.putString("host", VertxConfigProperties.mongoHost)
                    .putNumber("port", VertxConfigProperties.mongoPort);
        }
        return config;
    }

    /**
     * Deploy one or more Mod Mongo Persistor Vertices with a done handler
     * @param container
     * @param instances
     * @param doneHandler
     */
    public static void deploy(Container container, int instances, Handler<AsyncResult<String>> doneHandler) {
        container.deployModule(
                VertxConstants.MOD_MONGO_PERSISTOR,
                VertxConstants.MOD_MONGO_PERSISTOR_CONFIG,
                instances,
                doneHandler
        );
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
     * Deploy Mod Mongo Persistor without done handler
     * @param container
     */
    public static void deploy(Container container) {
        container.deployModule(
                VertxConstants.MOD_MONGO_PERSISTOR,
                VertxConstants.MOD_MONGO_PERSISTOR_CONFIG
        );
    }

    /**
     * Query MongoDB and return one or more matching records.
     *
     * TODO: Add exception handler.
     *
     * @param eventBus
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
            EventBus eventBus,
            String collectionName,
            JsonObject matcher,
            FindHandler handler,
            /**
             * Optional Args
             */
            JsonObject keys,
            Number batchSize
    ) throws VertxException {
        find(eventBus, collectionName, matcher, null, 0, -1, handler, keys, batchSize);
    }

    /**
     * Query MongoDB with "skip" and "limit".
     *
     * TODO: Add exception handler.
     *
     * @param eventBus
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
            EventBus eventBus,
            String collectionName,
            JsonObject matcher,
            JsonObject sort,
            int skip,
            int limit,
            final FindHandler handler,
            /**
             * Optional Args
             */
            JsonObject keys,
            Number batchSize
    ) throws VertxException {
        find(
                eventBus,
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
     * @param eventBus
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
            EventBus eventBus,
            String collectionName,
            JsonObject matcher,
            JsonObject sort,
            int skip,
            int limit,
            final FindHandler handler,
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
        if (eventBus == null || collectionName == null || matcher == null || handler == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the find message
         */
        final JsonObject findMessage = new JsonObject();
        findMessage.putString(MOD_MONGO_FIELD_NAME_ACTION, MOD_MONGO_FIELD_NAME_ACTION_FIND);
        findMessage.putString(MOD_MONGO_FIELD_NAME_COLLECTION, collectionName);
        findMessage.putObject(MOD_MONGO_FIELD_NAME_MATCHER, matcher);
        if (sort != null) {
            findMessage.putObject(MOD_MONGO_FIELD_NAME_SORT, sort);
        }
        if (skip > 0) {
            findMessage.putNumber(MOD_MONGO_FIELD_NAME_SKIP, skip);
        }
        if (limit > 0) {
            findMessage.putNumber(MOD_MONGO_FIELD_NAME_LIMIT, limit);
        }
        if (keys != null) {
            findMessage.putObject(MOD_MONGO_FIELD_NAME_KEYS, keys);
        }
        if (batchSize != null) {
            findMessage.putNumber(MOD_MONGO_FIELD_NAME_BATCH_SIZE, batchSize);
        }

        /**
         * Send it
         */
        if (bDebugPrint && log.isDebugEnabled()) {
            log.debug("Sending the following find message:\n" + findMessage.encodePrettily());
        }
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                findMessage,
                DEFAULT_TIMEOUT,
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        if (asyncResult.succeeded()) {
                            handler.handle(asyncResult.result());
                        } else {
                            log.error(VertxUtils.highlightWithHashes(
                                    "MongoDB Query Failed due to " + asyncResult.cause() + "! message:"
                                            + findMessage.encode()));
                            handler.handle(null);
                        }
                    }
                }
        );
    }

    /**
     * Query MongoDB and return the first matching record.
     *
     * TODO: Add exception handler.
     *
     * @param eventBus
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
            EventBus eventBus,
            String collectionName,
            JsonObject matcher,
            final FindOneHandler handler,
            /**
             * Optional Arg(s)
             */
            JsonObject keys
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (eventBus == null || collectionName == null || matcher == null || handler == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the find message
         */
        final JsonObject findMessage = new JsonObject();
        findMessage.putString(MOD_MONGO_FIELD_NAME_ACTION, MOD_MONGO_FIELD_NAME_ACTION_FIND_ONE);
        findMessage.putString(MOD_MONGO_FIELD_NAME_COLLECTION, collectionName);
        findMessage.putObject(MOD_MONGO_FIELD_NAME_MATCHER, matcher);
        if (keys != null) {
            findMessage.putObject(MOD_MONGO_FIELD_NAME_KEYS, keys);
        }

        /**
         * Send it
         */
        log.debug("Sending the following findOne message to mod-mongo-persistor:\n" + findMessage.encodePrettily());
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                findMessage,
                DEFAULT_TIMEOUT,
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        if (asyncResult.succeeded()) {
                            handler.handle(asyncResult.result());
                        } else {
                            log.error(VertxUtils.highlightWithHashes(
                                    "MongoDB Query Failed due to " + asyncResult.cause() + "! message:"+ findMessage.encode()));
                            handler.handle(null);
                        }
                    }
                }
        );
    }

    /**
     * Save a document which may result in creating a new document or replacing an existing document.
     *
     * TODO: Add exception handler.
     *
     * @param eventBus
     * @param collectionName
     * @param document
     * @param customHandler
     * @throws VertxException
     */
    public static void save(
            /**
             * Mandatory Args
             */
            EventBus eventBus,
            final String collectionName,
            final JsonObject document,
            /**
             * Optional Custom Handler
             */
            Handler<Message<JsonObject>> customHandler
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (eventBus == null || collectionName == null || document == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the save message
         */
        final JsonObject saveMessage = new JsonObject();
        saveMessage.putString(MOD_MONGO_FIELD_NAME_ACTION, MOD_MONGO_FIELD_NAME_ACTION_SAVE);
        saveMessage.putString(MOD_MONGO_FIELD_NAME_COLLECTION, collectionName);
        saveMessage.putObject(MOD_MONGO_FIELD_NAME_DOCUMENT, document);

        final Handler<Message<JsonObject>> handler =
                (customHandler != null)? customHandler :
                new Handler<Message<JsonObject>>() {
                    /**
                     * Default Save Result Handler
                     */
                    @Override
                    public void handle(Message<JsonObject> jsonObjectMessage) {
                        if (jsonObjectMessage == null) {
                            return;
                        }
                        String status = jsonObjectMessage.body().getString(MOD_MONGO_FIELD_NAME_STATUS);
                        if (status.equals(MOD_MONGO_FIELD_NAME_STATUS_VALUE_OK)) {
                            if (jsonObjectMessage.body().containsField(MOD_MONGO_FIELD_NAME_ID)) {
                                log.debug("Successfully saved document into " + collectionName
                                        + ", auto-generated id: " +
                                        jsonObjectMessage.body().getString(MOD_MONGO_FIELD_NAME_ID));
                            } else {
                                log.debug("Successfully saved document into " + collectionName
                                        + ", original id: " +
                                        document.getString(MOD_MONGO_FIELD_NAME_ID));
                            }
                        } else {
                            /**
                             * Log the error message upon failures
                             */
                            log.error("Failed to save a document!\n" + jsonObjectMessage.body().encodePrettily());
                        }
                    }
                };

        /**
         * Send it
         */
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                saveMessage,
                DEFAULT_TIMEOUT,
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        if (asyncResult.succeeded()) {
                            handler.handle(asyncResult.result());
                        } else {
                            log.error(VertxUtils.highlightWithHashes(
                                    "MongoDB Save Failed due to " + asyncResult.cause() + "! message:" + saveMessage.encode()));
                            handler.handle(null);
                        }

                    }
                }
        );
    }

    /**
     * Update an existing document by id.
     *
     * TODO: Add exception handler.
     *
     * @param eventBus
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
            EventBus eventBus,
            String collectionName,
            String id,
            JsonObject update,
            /**
             * Optional Custom Handler
             */
            Handler<Long> customHandler
    ) throws VertxException {
        updateWithMatcher(
                eventBus,
                collectionName,
                new JsonObject().putString(MOD_MONGO_FIELD_NAME_ID, id),
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
            EventBus eventBus,
            String collectionName,
            JsonObject matcher,
            JsonObject update,
            /**
             * Optional Custom Handler
             */
            Handler<Long> customHandler
    ) throws VertxException {
        updateWithMatcher(
                eventBus,
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
            EventBus eventBus,
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
                eventBus,
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
    public static final UpdateHandler DEFAULT_UPDATE_HANDLER = getDefaultUpdateHandler();
    public static void updateWithMatcher(
            /**
             * Mandatory Args
             */
            EventBus eventBus,
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
        if (eventBus == null || collectionName == null || matcher == null || update == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the update message
         */
        final JsonObject updateMessage = new JsonObject();
        updateMessage.putString(MOD_MONGO_FIELD_NAME_ACTION, MOD_MONGO_FIELD_NAME_ACTION_UPDATE);
        updateMessage.putString(MOD_MONGO_FIELD_NAME_COLLECTION, collectionName);
        updateMessage.putObject(MOD_MONGO_FIELD_NAME_UPDATE_CRITERIA, matcher);
        if (!matcher.containsField(MOD_MONGO_FIELD_NAME_ID)) {
            updateMessage.putBoolean(MOD_MONGO_FIELD_NAME_MULTI, true);
        }

        if (update.containsField(MOD_MONGO_UPDATE_OPERATOR_SET) &&
                update.getObject(MOD_MONGO_UPDATE_OPERATOR_SET).size() > 10) {
            log.debug("Mongo Persistor Update Message:\n" + updateMessage.encodePrettily());
            log.debug("(Update object too big to display.)");

            if (binaryFields != null) {
                VertxJsonUtils.merge(update, binaryFields);
            }
            updateMessage.putObject(MOD_MONGO_FIELD_NAME_UPDATE, update);
        } else {
            updateMessage.putObject(MOD_MONGO_FIELD_NAME_UPDATE, update);
            log.debug("Mongo Persistor Update Message:\n" + updateMessage.encodePrettily());
            if (binaryFields != null) {
                VertxJsonUtils.merge(update, binaryFields);
                updateMessage.putObject(MOD_MONGO_FIELD_NAME_UPDATE, update);
            }
        }

        // Handler Instance
        final UpdateHandler handler = (customHandler != null)? new UpdateHandler(customHandler) : DEFAULT_UPDATE_HANDLER;

        /**
         * Send it
         */
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                updateMessage,
                timeout,
                handler
        );
    }

    /**
     * Delete one existing document by id.
     *
     * TODO: Add exception handler.
     *
     * @param eventBus
     * @param collectionName
     * @param id
     * @param customHandler
     * @throws VertxException
     */
    public static void delete(
            /**
             * Mandatory Args
             */
            EventBus eventBus,
            String collectionName,
            final String id,
            /**
             * Optional Custom Handler
             */
            Handler<Message<JsonObject>> customHandler
    ) throws VertxException {
        deleteWithMatcher(
                eventBus,
                collectionName,
                new JsonObject().putString(MOD_MONGO_FIELD_NAME_ID, id),
                customHandler
        );
    }

    /**
     * Delete one existing document by id.
     *
     * TODO: Add exception handler.
     *
     * @param eventBus
     * @param collectionName
     * @param matcher
     * @param customHandler
     * @throws VertxException
     */
    public static void deleteWithMatcher(
            /**
             * Mandatory Args
             */
            EventBus eventBus,
            final String collectionName,
            final JsonObject matcher,
            /**
             * Optional Custom Handler
             */
            Handler<Message<JsonObject>> customHandler
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (eventBus == null || collectionName == null || matcher == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the delete message
         */
        JsonObject deleteMessage = new JsonObject();
        deleteMessage.putString(MOD_MONGO_FIELD_NAME_ACTION, MOD_MONGO_FIELD_NAME_ACTION_DELETE);
        deleteMessage.putString(MOD_MONGO_FIELD_NAME_COLLECTION, collectionName);
        deleteMessage.putObject(MOD_MONGO_FIELD_NAME_MATCHER, matcher);

        log.info("Deleting from " + collectionName + " with matcher:\n" + matcher.encodePrettily());

        // Handler Instance
        final Handler<Message<JsonObject>> handler =
                (customHandler != null)? customHandler :
                        new Handler<Message<JsonObject>>() {
                            /**
                             * Default Save Result Handler
                             */
                            @Override
                            public void handle(Message<JsonObject> jsonObjectMessage) {
                                if (jsonObjectMessage == null) {
                                    return;
                                }
                                String status = jsonObjectMessage.body().getString(MOD_MONGO_FIELD_NAME_STATUS);
                                if (!status.equals(MOD_MONGO_FIELD_NAME_STATUS_VALUE_OK)) {
                                    /**
                                     * Log the error message upon failures
                                     */
                                    log.error("Failed to delete document by matcher " + matcher.encode() + "!");
                                } else {
                                    log.debug("Deleted "
                                            + jsonObjectMessage.body().getField(MOD_MONGO_FIELD_NAME_NUMBER)
                                            + " record(s) from collection " + collectionName);
                                }
                            }
                        };

        /**
         * Send it
         */
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                deleteMessage,
                DEFAULT_TIMEOUT,
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        if (asyncResult.succeeded()) {
                            handler.handle(asyncResult.result());
                        } else {
                            log.error(VertxUtils.highlightWithHashes(
                                    "MongoDB Delete Failed due to " + asyncResult.cause() + "! matcher:" + matcher.encode() + "!"));
                            handler.handle(null);
                        }
                    }
                }
        );
    }

    /**
     * Get count by matcher
     *
     * TODO: Add exception handler.
     *
     * @param eventBus
     * @param collectionName
     * @param matcher
     * @param customHandler
     * @throws VertxException
     */
    public static void count(
            /**
             * Mandatory Args
             */
            EventBus eventBus,
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
        if (eventBus == null || collectionName == null || matcher == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the delete message
         */
        JsonObject countMessage = new JsonObject();
        countMessage.putString(MOD_MONGO_FIELD_NAME_ACTION, MOD_MONGO_FIELD_NAME_ACTION_COUNT);
        countMessage.putString(MOD_MONGO_FIELD_NAME_COLLECTION, collectionName);
        countMessage.putObject(MOD_MONGO_FIELD_NAME_MATCHER, matcher);

        log.debug("Sending to following count message to MongoDB:\n" + countMessage.encodePrettily());

        /**
         * Send it
         */
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                countMessage,
                DEFAULT_TIMEOUT,
                new CountHandler(customHandler)
        );
    }

    /**
     * Atomic Find-and-Modify.
     *
     * @param eventBus
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
            EventBus eventBus,
            String collectionName,
            JsonObject matcher,
            JsonObject updates,
            FindOneHandler handler
    ) throws VertxException {
        /**
         * Validate the mandatory arguments
         */
        if (eventBus == null || collectionName == null || matcher == null || handler == null) {
            throw new VertxException("Null Pointer(s)!");
        }

        /**
         * Build the find message
         */
        JsonObject message = new JsonObject();
        message.putString(MOD_MONGO_FIELD_NAME_ACTION, MOD_MONGO_FIELD_NAME_ACTION_FIND_AND_MODIFY);
        message.putString(MOD_MONGO_FIELD_NAME_COLLECTION, collectionName);
        message.putObject(MOD_MONGO_FIELD_NAME_MATCHER, matcher);
        message.putObject(MOD_MONGO_FIELD_NAME_FIND_MODIFY_UPDATE, updates);

        /**
         * Send it
         */
        log.debug("Sending the following findAndModify message to mod-mongo-persistor:\n" + message.encodePrettily());
        eventBus.send(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                message,
                handler
        );
    }



    /**
     * Base Inner Class to handle async save result.
     */
    public static class SaveHandler implements Handler<Message<JsonObject>> {
        public String status = null;
        public JsonObject newDoc = null;

        /**
         * Default Save Result Handler
         */
        @Override
        public void handle(Message<JsonObject> saveResult) {
            if (saveResult == null) {
                status = TIMED_OUT;
            } else {

                status = saveResult.body().getString(MOD_MONGO_FIELD_NAME_STATUS);
                if (status.equals(MOD_MONGO_FIELD_NAME_STATUS_VALUE_OK)) {
                    if (saveResult.body().containsField(MOD_MONGO_FIELD_NAME_ID)) {
                        log.debug("Successfully saved document, auto-generated id: " +
                                saveResult.body().getString(MOD_MONGO_FIELD_NAME_ID));
                    } else {
                        log.debug("Successfully saved document" +
                                newDoc == null ? "." : ("original id: " + newDoc.getString(MOD_MONGO_FIELD_NAME_ID)));
                    }
                } else {
                    /**
                     * Log the error message upon failures
                     */
                    log.error("Failed to save a document!\n" + saveResult.body().encodePrettily());
                }
            }
        }
    }

    /**
     * Base Inner Class to handle async find result.
     */
    public static class FindHandler implements Handler<Message<JsonObject>> {
        private static final Logger log = LoggerFactory.getLogger(FindHandler.class.getName());

        /**
         * Actual Handler that processes the find result (extracted from the raw message)
         *
         * The implementation must take care of null-pointer case.
         */
        public Handler<JsonArray> resultHandler = null;

        /**
         * Are there more batch(es) waiting?
         */
        public boolean moreExist = false;

        /**
         * Query Status String
         */
        public String status = null;

        /**
         * Actual Query Results (as a JSON Array)
         */
        public JsonArray queryResults = null;

        /**
         * Constructor that requires a resultHandler instance
         * @param resultHandler
         */
        public FindHandler(Handler<JsonArray> resultHandler) throws VertxException {
            if (resultHandler == null) {
                throw new VertxException("Null Pointer!");
            }

            this.resultHandler = resultHandler;
        }

        /**
         * Empty Constructor
         */
        public FindHandler() {
        }

        /**
         * The handler method body.
         * @param jsonObjectMessage
         */
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            if (jsonObjectMessage == null) {
                // Probably had a timeout
                queryResults = FIND_TIMED_OUT;

                if (resultHandler != null) {
                    resultHandler.handle(FIND_TIMED_OUT);
                }
                return;
            }

            status = jsonObjectMessage.body().getString(MOD_MONGO_FIELD_NAME_STATUS);

            queryResults = jsonObjectMessage.body().getArray(MOD_MONGO_FIELD_NAME_RESULTS);
            if (queryResults != null) {
                //log.debug("Found " + queryResults.size() + " records in the query results, status: " + status);
            } else {
                log.error(jsonObjectMessage.body().encodePrettily());
                queryResults = null;
            }

            /**
             * If more exist, get next batch by replying with an empty message
             */
            if (status.equals(MOD_MONGO_FIELD_NAME_STATUS_VALUE_MORE_EXIST)) {
                jsonObjectMessage.reply(this);
                moreExist = true;
            } else {
                moreExist = false;
            }

            /**
             * Call the registered worker handler either way
             */
            if (resultHandler != null) {
                resultHandler.handle(queryResults);
            }
        }
    }

    /**
     * Base Inner Class to handle async find-one result.
     */
    public static class FindOneHandler implements Handler<Message<JsonObject>> {
        private static final Logger log = LoggerFactory.getLogger(FindOneHandler.class.getName());

        public String status = null;
        public JsonObject queryResult = null;

        /**
         * Actual Handler that processes the find-one result (extracted from the raw message)
         *
         * The implementation must take care of null-pointer case.
         */
        public Handler<JsonObject> resultHandler;

        /**
         * Constructor that requires a resultHandler instance
         * @param resultHandler
         */
        public FindOneHandler(Handler<JsonObject> resultHandler) throws VertxException {
            if (resultHandler == null) {
                throw new VertxException("Null Pointer!");
            }

            this.resultHandler = resultHandler;
        }

        public FindOneHandler() {
        }

        /**
         * The handler method body.
         * @param jsonObjectMessage
         */
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            if (jsonObjectMessage == null) {
                // Probably had a timeout
                queryResult = FIND_ONE_TIMED_OUT;

                if (resultHandler != null) {
                    resultHandler.handle(FIND_ONE_TIMED_OUT);
                }
                return;
            }

            status = jsonObjectMessage.body().getString(MOD_MONGO_FIELD_NAME_STATUS);

            if (!MOD_MONGO_FIELD_NAME_STATUS_VALUE_OK.equals(status)) {
                log.error(jsonObjectMessage.body().encodePrettily());
            } else {
                queryResult = jsonObjectMessage.body().getObject(MOD_MONGO_FIELD_NAME_RESULT);
            }

            /**
             * Call the registered worker handler either way
             */
            if (resultHandler != null) {
                resultHandler.handle(queryResult);
            }
        }
    }

    /**
     * Base Inner Class to handle async update result.
     */
    public static class UpdateHandler implements Handler<AsyncResult<Message<JsonObject>>> {
        private static final Logger log = LoggerFactory.getLogger(UpdateHandler.class.getName());

        public String status = null;
        public Long numberOfRecords = null;

        /**
         * Actual Handler that processes the find-one result (extracted from the raw message)
         *
         * The implementation must take care of null-pointer case.
         */
        public Handler<Long> resultHandler;

        /**
         * Constructor that requires a resultHandler instance
         * @param resultHandler
         */
        public UpdateHandler(Handler<Long> resultHandler) throws VertxException {
            if (resultHandler == null) {
                throw new VertxException("Null Pointer!");
            }

            this.resultHandler = resultHandler;
        }

        /**
         * The handler method body.
         */
        @Override
        public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
            if (asyncResult.failed() || asyncResult.result() == null || asyncResult.result().body() == null) {
                if (asyncResult.failed()) {
                    log.error("Update failed due to " + asyncResult.cause());
                } else {
                    log.error("Update failed due to null pointer!");
                }
                resultHandler.handle(null);
            } else {
                status = asyncResult.result().body().getString(MOD_MONGO_FIELD_NAME_STATUS);
                numberOfRecords = asyncResult.result().body().getLong(MOD_MONGO_FIELD_NAME_NUMBER);

                if (!MOD_MONGO_FIELD_NAME_STATUS_VALUE_OK.equals(status) || numberOfRecords == null) {
                    log.error(asyncResult.result().body().encodePrettily());
                    numberOfRecords = null;
                }

                /**
                 * Call the registered worker handler either way
                 */
                resultHandler.handle(numberOfRecords);
            }
        }
    }

    /**
     * Get a default Update Handler
     * @return
     */
    public static UpdateHandler getDefaultUpdateHandler() {
        try {
            return new UpdateHandler(
                    new Handler<Long>() {
                        @Override
                        public void handle(Long numberOfRecords) {
                            if (numberOfRecords == null) {
                                log.error("Update Failed!");
                            }
                        }
                    }
            );
        } catch (VertxException e) {
            return null;
        }
    }

    /**
     * Base Inner Class to handle async delete result.
     */
    public static class DeleteHandler implements Handler<Message<JsonObject>> {
        private static final Logger log = LoggerFactory.getLogger(DeleteHandler.class.getName());

        public String status = null;
        public Number numberOfRecords = null;

        /**
         * Actual Handler that processes the find-one result (extracted from the raw message)
         *
         * The implementation must take care of null-pointer case.
         */
        public Handler<Number> resultHandler;

        /**
         * Constructor that requires a resultHandler instance
         * @param resultHandler
         */
        public DeleteHandler(Handler<Number> resultHandler) throws VertxException {
            if (resultHandler == null) {
                throw new VertxException("Null Pointer!");
            }

            this.resultHandler = resultHandler;
        }

        public DeleteHandler() {
        }

        /**
         * The handler method body.
         * @param jsonObjectMessage
         */
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            if (jsonObjectMessage != null && jsonObjectMessage.body() != null) {
                status = jsonObjectMessage.body().getString(MOD_MONGO_FIELD_NAME_STATUS);
                numberOfRecords = jsonObjectMessage.body().getNumber(MOD_MONGO_FIELD_NAME_NUMBER);
            }

            if (!MOD_MONGO_FIELD_NAME_STATUS_VALUE_OK.equals(status) || numberOfRecords == null) {
                log.error(jsonObjectMessage.body().encodePrettily());
                numberOfRecords = null;
            }

            /**
             * Call the registered worker handler either way
             */
            if (resultHandler != null) {
                resultHandler.handle(numberOfRecords);
            }
        }
    }

    /**
     * Base Inner Class to handle async find-one result.
     */
    public static class CountHandler implements Handler<AsyncResult<Message<JsonObject>>> {
        public String status = null;
        public Long count = null;

        /**
         * Actual Handler that processes the count result (extracted from the raw message)
         *
         * The implementation must take care of null-pointer case (i.e. timed-out).
         */
        public Handler<Long> resultHandler;

        /**
         * Constructor that requires a resultHandler instance
         * @param resultHandler
         */
        public CountHandler(Handler<Long> resultHandler) {
            this.resultHandler = resultHandler;
        }

        /**
         * Something has happened, so handle it.
         *
         * @param asyncResult
         */
        @Override
        public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
            if (asyncResult.succeeded()) {
                status =  asyncResult.result().body().getString(MOD_MONGO_FIELD_NAME_STATUS);
                count = asyncResult.result().body().getLong(MOD_MONGO_FIELD_NAME_COUNT);
            } else {
                status = asyncResult.cause().getLocalizedMessage();
            }

            if (!MOD_MONGO_FIELD_NAME_STATUS_VALUE_OK.equals(status)) {
                log.error("Failed to get count! status: " + status);
                count = null;
            }

            /**
             * Call the registered worker handler either way
             */
            if (resultHandler != null) {
                resultHandler.handle(count);
            }
        }
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
                updates.putObject(MOD_MONGO_UPDATE_OPERATOR_SET, sets);
            }
            if (unsets != null) {
                updates.putObject(MOD_MONGO_UPDATE_OPERATOR_UNSET, unsets);
            }
            if (timestamps != null) {
                updates.putObject(MOD_MONGO_UPDATE_OPERATOR_CURRENT_DATE, timestamps);
            }
            if (pulls != null) {
                updates.putObject(MOD_MONGO_UPDATE_OPERATOR_PULL, pulls);
            }
            if (pushes != null) {
                updates.putObject(MOD_MONGO_UPDATE_OPERATOR_PUSH, pushes);
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
        return new JsonObject().putNumber(MOD_MONGO_DATE, System.currentTimeMillis());
    }
    // Get a Date Object for a custom date/time
    public static JsonObject getDateObject(long customDateTime) {
        return new JsonObject().putNumber(MOD_MONGO_DATE, customDateTime);
    }

    /**
     * Get a Java Date POJO with a MongoDB Date Object.
     * @param mongoDateObject
     * @return
     * @throws VertxException
     */
    public static Date getDateFromMongoDateObject(JsonObject mongoDateObject) throws VertxException {
        if (mongoDateObject == null || !mongoDateObject.containsField(MOD_MONGO_DATE)) {
            log.error("getDateFromMongoDateObject(): Null Pointer!");
            throw VertxException.NULL_POINTER;
        }
        if (!mongoDateObject.containsField(MOD_MONGO_DATE)) {
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
