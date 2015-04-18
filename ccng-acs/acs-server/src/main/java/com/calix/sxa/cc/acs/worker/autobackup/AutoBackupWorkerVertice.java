package com.calix.sxa.cc.acs.worker.autobackup;

import com.calix.sxa.VertxConstants;
import com.calix.sxa.VertxRedisUtils;
import com.calix.sxa.cc.model.*;
import com.calix.sxa.cc.util.AcsApiUtils;
import com.calix.sxa.cc.util.AcsConfigProperties;
import com.calix.sxa.cc.util.AcsConstants;
import com.calix.sxa.cc.util.AutoBackupUtils;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * Project:  SXA-CC
 *
 * Auto Backup Worker Vertice.
 *
 * @author: jqin
 */
public class AutoBackupWorkerVertice extends Verticle{
    private Logger log = LoggerFactory.getLogger(this.getClass());

    // Async Redis Client Instance
    public RedisClient redisClient;

    // # of Current Outstanding Auto-Backup Tasks
    int outstandingAutoBackupTasks = 0;

    /**
     * Start the worker
     */
    public void start() {
        /**
         * Initialize Redis Client
         */
        redisClient = new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);

        /**
         * Start a 10-second timer to read the auto-backup task queue (in Redis)
         */
        vertx.setPeriodic(
                10000,
                new Handler<Long>() {
                    @Override
                    public void handle(Long event) {
                        readFromQueue();
                    }
                }
        );

        log.info("Base Auto Backup Soak Time: " + AcsConfigProperties.AUTO_BACKUP_SOAK_TIME + " seconds.");
        log.info("Current System Time (in ms): " + System.currentTimeMillis());
    }

    /**
     * Read the next CPE (if any) from Discovery Queue
     */
    public void readFromQueue() {
        if (outstandingAutoBackupTasks >= AcsConfigProperties.MAX_CONCURRENT_AUTO_BACKUP_TASKS) {
            // Do not exceed the cap
            log.debug("Current # of outstanding auto-backup tasks: " + outstandingAutoBackupTasks);
            return;
        }

        // Read from Redis
        AutoBackupUtils.readTasks(
                redisClient,
                AcsConfigProperties.MAX_CONCURRENT_AUTO_BACKUP_TASKS - outstandingAutoBackupTasks,
                readTaskResultHandler
        );
    }

    /**
     * Handler to process read results of the CPE-discovery queue from Redis
     */
    Handler<JsonArray> readTaskResultHandler = new Handler<JsonArray>() {
        @Override
        public void handle(JsonArray allTasks) {
            if (allTasks == null || allTasks.size() == 0) {
                return;
            }

            for (int i = 0; i < allTasks.size(); i ++) {
                final String rawString = allTasks.get(i);
                /**
                 * Try to delete this task from the queue
                 */
                VertxRedisUtils.zrem(
                        redisClient,
                        AcsConstants.REDIS_KEY_AUTO_BACKUP_QUEUE,
                        rawString,
                        new Handler<Long>() {
                            @Override
                            public void handle(Long result) {
                                if (result != null && result == 1) {
                                    /**
                                     * Successfully deleted it from Redis
                                     */
                                    try {
                                        JsonObject cpe = new JsonObject(rawString);
                                        doAutoBackup(cpe);
                                    } catch (Exception ex) {
                                        log.error("Invalid auto backup task from Redis! " + rawString);
                                    }
                                }
                            }
                        }
                );
            }
        }
    };

    /**
     * Perform the auto backup by sending device-op request via event bus.
     *
     * @param cpe
     */
    public void doAutoBackup(JsonObject cpe) {
        final String cpeKey = cpe.getString(AcsConstants.FIELD_NAME_ID);
        final String orgId = Cpe.getOrgIdByCpeKey(cpeKey);
        log.info("Performing Auto Backup for " + cpeKey + "...");

        outstandingAutoBackupTasks++;

        // Build a device-op API request
        JsonObject requestBody = new JsonObject()
                .putObject(CpeDeviceOp.FIELD_NAME_CPE_DB_OBJECT, cpe)
                .putString(CpeDeviceOp.FIELD_NAME_OPERATION, CpeDeviceOpTypeEnum.Upload.name())
                .putString(CpeDeviceOp.FIELD_NAME_FILE_TYPE, AcsFileType.ConfigFile.tr069DownloadFileTypeString)
                .putObject(CpeDeviceOp.FIELD_NAME_FILE_STRUCT, AcsFile.buildAutoBackupFileRecordWithUploadURL(cpeKey));

        /**
         * Send the device-op request
         */
        AcsApiUtils.sendApiRequest(
                vertx.eventBus(),
                AcsConstants.ACS_API_SERVICE_DEVICE_OP,
                orgId,
                AcsApiCrudTypeEnum.Create,
                requestBody,
                ExecPolicy.DEFAULT_DEVICE_OP_TIMEOUT + 10000,    // wait 10 extra seconds
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        if (asyncResult == null || asyncResult.failed()) {
                            log.error(cpeKey + ":Auto Backup Failed due to "
                                    + asyncResult == null? "(null)" : asyncResult.cause() + "!");
                        } else {
                            log.info(cpeKey + ": Auto Backup Succeeded.");
                        }

                        outstandingAutoBackupTasks--;
                    }
                }
        );
    }
}
