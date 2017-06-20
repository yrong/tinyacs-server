package vertx.acs.worker.autobackup;

import io.vertx.core.AbstractVerticle;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import vertx.VertxConfigProperties;
import vertx.VertxConstants;
import vertx.VertxRedisUtils;
import vertx.model.*;
import vertx.util.AcsApiUtils;
import vertx.util.AcsConfigProperties;
import vertx.util.AcsConstants;
import vertx.util.AutoBackupUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * Auto Backup Worker Vertice.
 *
 * @author: ronyang
 */
public class AutoBackupWorkerVertice extends AbstractVerticle{
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
        RedisOptions options = new RedisOptions().setHost(VertxConfigProperties.redisHost).setPort(VertxConfigProperties.redisPort);
        redisClient = RedisClient.create(vertx,options);

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
                final String rawString = allTasks.getString(i);
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
                .put(CpeDeviceOp.FIELD_NAME_CPE_DB_OBJECT, cpe)
                .put(CpeDeviceOp.FIELD_NAME_OPERATION, CpeDeviceOpTypeEnum.Upload.name())
                .put(CpeDeviceOp.FIELD_NAME_FILE_TYPE, AcsFileType.ConfigFile.tr069DownloadFileTypeString)
                .put(CpeDeviceOp.FIELD_NAME_FILE_STRUCT, AcsFile.buildAutoBackupFileRecordWithUploadURL(cpeKey));

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
