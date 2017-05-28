package vertx;

import io.vertx.core.AsyncResult;
import io.vertx.redis.RedisClient;
import io.vertx.redis.op.RangeLimitOptions;
import io.vertx.redis.op.SetOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * Vert.x Redis Client Utils that will work with vert.x mod-redis.
 *
 * @author: ronyang
 */
public class VertxRedisUtils {
    private static final Logger log = LoggerFactory.getLogger(VertxRedisUtils.class.getName());

    /**
     * MAX # of instances of mod-redis is 8
     */
    public static final int MAX_INSTANCES = 8;

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
     * Return String Values
     */
    public static final String OK = "OK";
    
    /**
     * Errors
     */
    public static final String NOT_EXIST_AND_EXIST_BOTH_TRUE = "\"notExist\" and \"exist\" cannot both be true!";

    /**
     * Default Handlers
     */

    public static final Handler<AsyncResult<String>> DEFAULT_STRING_RESULT_HANDLER = res->{
        if (res.succeeded()) {
            log.info("Received string result " + res.result());
        }
    };

    public static final Handler<AsyncResult<Long>> DEFAULT_LONG_RESULT_HANDLER = res->{
        if (res.succeeded()) {
            log.info("Received long result " + res.result());
        }
    };

    public static final Handler<AsyncResult<Void>> DEFAULT_VOID_RESULT_HANDLER = res->{};

    /**
     * Base Redis Command Result Handler Class, which can be constructed with a custom String handler.
     */
    public static class ResultHandler implements Handler<Message<JsonObject>> {
        // Custom String Result Handler
        public Handler<String> customStringHandler = null;
        // Custom Json Object Result Handler
        public Handler<JsonObject> customJsonObjectHandler = null;
        // Custom Long Result Handler
        public Handler<Long> customLongHandler = null;
        // Custom JSON Array Result Handler
        public Handler<JsonArray> customJsonArrayHandler = null;

        // Constructor with custom handler(s)
        public ResultHandler(
                Handler<String> customStringHandler,
                Handler<JsonObject> customJsonObjectHandler,
                Handler<Long> customLongHandler,
                Handler<JsonArray> customJsonArrayHandler) {
            this.customStringHandler = customStringHandler;
            this.customLongHandler = customLongHandler;
            this.customJsonObjectHandler = customJsonObjectHandler;
            this.customJsonArrayHandler = customJsonArrayHandler;
        }

        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            JsonObject result = jsonObjectMessage.body();
            if (result == null || !("ok".equals(result.getString("status")))) {
                log.error("Received unexpected result from mod-redis!"
                        + (result == null ? "(null)" : result.encode()));
                String message = null;
                if (result != null) {
                    message = result.getString("message");
                }

                if (customStringHandler != null) {
                    customStringHandler.handle(message);
                } else if (customLongHandler != null) {
                    customLongHandler.handle(Long.valueOf(-1));
                } else if (customJsonObjectHandler != null) {
                    customJsonObjectHandler.handle(result);
                } else if (customJsonArrayHandler != null) {
                    customJsonArrayHandler.handle(null);
                } else {
                    log.error("Received NULL value but No handler registered!");
                }
            } else {
                //log.info("Received result from mod-redis: " + result.encode());

                Object value = result.getValue("value");
                if (value == null) {
                    if (customStringHandler != null) {
                        customStringHandler.handle(null);
                    } else if (customLongHandler != null) {
                        customLongHandler.handle(null);
                    } else if (customJsonObjectHandler != null) {
                        customJsonObjectHandler.handle(null);
                    } else if (customJsonArrayHandler != null) {
                        customJsonArrayHandler.handle(null);
                    } else {
                        log.error("Received NULL value but No handler registered!");
                    }
                } else if (value instanceof String && customStringHandler != null) {
                    customStringHandler.handle((String)value);
                } else if ((value instanceof Long || value instanceof Integer) && customLongHandler != null) {
                    customLongHandler.handle((Long) value);
                } else if (value instanceof JsonObject && customJsonObjectHandler != null) {
                    customJsonObjectHandler.handle((JsonObject)value);
                } else if (value instanceof JsonArray && customJsonArrayHandler != null) {
                    customJsonArrayHandler.handle((JsonArray)value);
                } else {
                    log.info("No applicable handler registered for " + value.getClass().getSimpleName()
                            + " value: " + value.toString());
                    log.info("StringHandler:" + customStringHandler
                            + ", LongHandler: " + customLongHandler
                            + ", JsonObject Handler: " + customJsonObjectHandler
                            + ", JsonArray Handler: " + customJsonArrayHandler);
                }
            }
        }
    }

    /**
     * Get a String value by key name.
     *
     * @param redisClient
     * @param key
     * @param handler
     */
    public static void getValue(
            RedisClient redisClient,
            String key,
            Handler<String> handler) {
        redisClient.get(key, res -> {
            if (res.succeeded()) {
                handler.handle(res.result());
            }
        });
    }

    /**
     * Set a String value by key name with the following options:
     *
     * PX milliseconds -- Set the specified expire time, in milliseconds.
     * NX -- Only set the key if it does not already exist.
     * XX -- Only set the key if it already exist.
     *
     * If SET was executed correctly, the return value will be Simple String "OK";
     * Otherwise Redis will return a NULL value.
     *
     * @param redisClient
     * @param key
     * @param value
     * @param expiration    expiration in # of milli seconds
     * @param notExist
     * @param exist
     * @param handler
     */
    public static void set(
            RedisClient redisClient,
            String key,
            String value,
            long expiration,
            Boolean notExist,
            Boolean exist,
            Handler<String> handler) {

        Handler<AsyncResult<String>> resultStringHandler = res->{
            if (res.succeeded()) {
                handler.handle(res.result());
            }else{
                handler.handle(null);
            }
        };

        Handler<AsyncResult<Void>> resultVoidHandler = res->{
            handler.handle(null);
        };

        if (expiration > 0) {
            if (!Boolean.TRUE.equals(notExist) && !Boolean.TRUE.equals(exist)) {
                redisClient.setWithOptions(key, value, new SetOptions().setPX(expiration),resultStringHandler);
            } else if (Boolean.TRUE.equals(notExist) && !Boolean.TRUE.equals(exist)) {
                redisClient.setWithOptions(key, value,new SetOptions().setPX(expiration).setNX(true), resultStringHandler);
            } else if (!Boolean.TRUE.equals(notExist) && Boolean.TRUE.equals(exist)) {
                redisClient.setWithOptions(key, value, new SetOptions().setPX(expiration).setXX(true),resultStringHandler);
            } else {
                log.error(NOT_EXIST_AND_EXIST_BOTH_TRUE);
                handler.handle(NOT_EXIST_AND_EXIST_BOTH_TRUE);
            }
        } else {
            if (!Boolean.TRUE.equals(notExist) && !Boolean.TRUE.equals(exist)) {
                redisClient.set(key, value, resultVoidHandler);
            } else if (Boolean.TRUE.equals(notExist) && !Boolean.TRUE.equals(exist)) {
                redisClient.setWithOptions(key, value, new SetOptions().setNX(true), resultStringHandler);
            } else if (!Boolean.TRUE.equals(notExist) && Boolean.TRUE.equals(exist)) {
                redisClient.setWithOptions(key, value, new SetOptions().setXX(true), resultStringHandler);
            } else {
                log.error(NOT_EXIST_AND_EXIST_BOTH_TRUE);
                handler.handle(NOT_EXIST_AND_EXIST_BOTH_TRUE);
            }
        }
    }

    /**
     * A simpler version of Set with no options or handler required.
     *
     * @param redisClient
     * @param key
     * @param value
     */
    public static void set(
            RedisClient redisClient,
            String key,
            String value) {
        redisClient.set(key, value, DEFAULT_VOID_RESULT_HANDLER);
    }

    /**
     * Simple del.
     *
     * @param redisClient
     * @param key
     */
    public static void del(
            RedisClient redisClient,
            String key) {
        redisClient.del(key, DEFAULT_LONG_RESULT_HANDLER);
    }

    /**
     * Delete with a custom handler.
     *
     * @param redisClient
     * @param key
     */
    public static void del(
            RedisClient redisClient,
            String key,
            ResultHandler handler) {
        redisClient.del(key, (res)->{
            handler.customLongHandler.handle(res.result());
        });
    }

    /**
     * Add a new value into a Redis Set.
     *
     * If succeeded, the number of elements that were added to the set shall be returned, not including all
     * the elements already present into the set.
     *
     * @param redisClient
     * @param key
     * @param value
     * @param handler
     */
    public static void sadd(
            RedisClient redisClient,
            String key,
            String value,
            Handler<Long> handler) {
        redisClient.sadd(key, value, (res)->{
            handler.handle(res.result());
        });
    }

    /**
     * Get all the members of a Redis Set.
     *
     * @param redisClient
     * @param key
     * @param handler
     */
    public static void smembers(
            RedisClient redisClient,
            String key,
            Handler<JsonArray> handler) {
        redisClient.smembers(key, (res)->{
            handler.handle(res.result());
        });
    }

    /**
     * Get a String value by key name and start/stop range.
     *
     * The list elements are returned in a JSON Array. If key does not exist, a NULL JSON Array will be returned.
     *
     * To get all element, set start to 0 and stop to -1.
     *
     * @param redisClient
     * @param key
     * @param start
     * @param stop
     * @param handler
     */
    public static void lrange(
            RedisClient redisClient,
            String key,
            int start,
            int stop,
            Handler<JsonArray> handler) {
        redisClient.lrange(
                key,
                start,
                stop,
                (res)->{
                    handler.handle(res.result());
                }
        );
    }

    /**
     * Get the length of a given Redis List.
     *
     * @param redisClient
     * @param key
     * @param handler
     */
    public static void llen(
            RedisClient redisClient,
            String key,
            Handler<Long> handler) {
        redisClient.llen(key,res->{
            handler.handle(res.result());
        });
    }

    /**
     * Pop the oldest element of a Redis List.
     *
     * If the key does not exist, the caller will not be blocked.
     *
     * @param redisClient
     * @param key
     * @param handler
     */
    public static void lpop(
            RedisClient redisClient,
            String key,
            Handler<String> handler) {
        redisClient.lpop(
                key,
                res->{
                    handler.handle(res.result());
                }
        );
    }

    /**
     * Add a new String element into tail of a Redis List.
     *
     * The result will be the length of the list after the operation
     *
     * @param redisClient
     * @param key
     * @param value
     */
    public static void rpush(
            RedisClient redisClient,
            String key,
            String value,
            Handler<Long> handler) {
        redisClient.rpush(
                key,
                value,
                res->{
                    handler.handle(res.result());
                }
        );
    }

    /**
     * Remove an existing String element from a Redis List.
     *
     * The result will either 1 if element was successfully removed, or 0.
     *
     * @param redisClient
     * @param key
     * @param value
     */
    public static void lrem(
            RedisClient redisClient,
            String key,
            String value,
            Handler<Long> handler) {
        redisClient.lrem(
                key,
                1,
                value,
                res->{
                    handler.handle(res.result());
                }
        );
    }

    /**
     * Remove an existing String element from a Redis Sorted Set.
     *
     * The result will either 1 if element was successfully removed, or 0.
     *
     * @param redisClient
     * @param key
     * @param value
     */
    public static void zrem(
            RedisClient redisClient,
            String key,
            String value,
            Handler<Long> handler) {
        redisClient.zrem(
                key,
                value,
                res->{
                    handler.handle(res.result());
                }
        );
    }

    /**
     * Add a new member (with a score) to a Redis Sorted Set.
     *
     * The result will either 1 if element was successfully added, or 0.
     *
     * @param redisClient
     * @param key
     * @param score
     * @param value
     */
    public static void zadd(
            RedisClient redisClient,
            String key,
            long score,
            String value,
            Handler<Long> handler) {
        redisClient.zadd(
                key,
                score,
                value,
                res->{
                    handler.handle(res.result());
                }
        );
    }

    /**
     * Read a Redis Sorted Set with range of scores plus offset/count.
     *
     * The result will either 1 if element was successfully added, or 0.
     *
     * @param redisClient
     * @param key
     * @param minScore
     * @param maxScore
     */
    public static void zrangeByScore(
            RedisClient redisClient,
            String key,
            long minScore,
            long maxScore,
            int offset,
            int count,
            Handler<JsonArray> handler) {
        redisClient.zrangebyscore(
                key,
                String.valueOf(minScore),
                String.valueOf(maxScore),
                (RangeLimitOptions)new RangeLimitOptions().useWithScores().setLimit(offset,count),
                res->{
                    handler.handle(res.result());
                }
        );
    }

    /**
     * Get a String value from a sorted set by key name and start/stop range.
     *
     * The set elements are returned in a JSON Array. If key does not exist, a NULL JSON Array will be returned.
     *
     * To get all element, set start to 0 and stop to -1.
     *
     * @param redisClient
     * @param key
     * @param start
     * @param stop
     * @param handler
     */
    public static void zrange(
            RedisClient redisClient,
            String key,
            int start,
            int stop,
            Handler<JsonArray> handler) {
        redisClient.zrange(
                key,
                start,
                stop,
                res->{
                    handler.handle(res.result());
                }
        );
    }

    /**
     * Acquire a Distributed Lock.
     *
     * @param redisClient       Redis Client Instance.
     * @param lockKey           The Key (i.e. "name") of the lock
     * @param lockHolderName    The name of the lock holder (i.e. caller of this method)
     * @param expiration        How long with the caller want to hold the lock
     * @param maxRetries        If not able to acquire the lock, how many retry attempts shall be made.
                                Default to 0 (no retries).
     * @param retryInterval     If maxRetry is greater than 0, retry in how many milliseconds. Must be >= 1000 (1 sec).
     * @param resultHandler     Handler for handling the boolean result (lock acquired or not)
     */
    public static void acquireLock(
            RedisClient redisClient,
            String lockKey,
            String lockHolderName,
            long expiration,
            long retryInterval,
            int maxRetries,
            Handler<Boolean> resultHandler
    ) {

    }
}
