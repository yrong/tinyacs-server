package com.calix.sxa.cc.util;

import com.calix.sxa.VertxRedisUtils;
import com.calix.sxa.cc.model.Cpe;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA-CC
 *
 * Util Methods related to Auto Backups.
 *
 * Mainly around adding to/removing from the auto-backup task queue (a Redis Sorted Set).
 *
 * @author: jqin
 */
public class AutoBackupUtils {
    private static final Logger log = LoggerFactory.getLogger(AutoBackupUtils.class.getName());

    /**
     * Add a new CPE into the queue.
     *
     * @param redisClient
     * @param cpe
     */
    public static void addToQueue(RedisClient redisClient, final JsonObject cpe) {
        final long delay = AcsConfigProperties.AUTO_BACKUP_SOAK_TIME
                // plus a random delay
                + AcsMiscUtils.randInt(0, AcsConfigProperties.AUTO_BACKUP_SOAK_TIME);
        final String delayString = (delay / 60) + " min(s) " + (delay % 60) + " second(s)";

        VertxRedisUtils.zadd(
                redisClient,
                AcsConstants.REDIS_KEY_AUTO_BACKUP_QUEUE,
                System.currentTimeMillis() + (delay * 1000),
                cpe.encode(),
                new Handler<Long>() {
                    @Override
                    public void handle(Long result) {
                        log.info("Successfully " +
                                ((result != null && result > 0)?"added " : "updated ")
                                + cpe.getString(Cpe.DB_FIELD_NAME_SN)
                                + ". Delay: " + delayString);
                    }
                }
        );
    }

    /**
     * Read auto-backup tasks that pasted the soaking time from the queue.
     *
     * If read one successfully, call the provided handler.
     *
     * @param redisClient
     * @param maxCount
     * @param handler
     */
    public static void readTasks(
            RedisClient redisClient,
            int maxCount,
            Handler<JsonArray> handler) {
        VertxRedisUtils.zrangeByScore(
                redisClient,
                AcsConstants.REDIS_KEY_AUTO_BACKUP_QUEUE,
                -1,
                System.currentTimeMillis(),
                0,
                maxCount,
                handler
        );
    }
}
