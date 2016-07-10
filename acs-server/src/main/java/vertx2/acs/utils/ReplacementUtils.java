package vertx2.acs.utils;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxMongoUtils;
import vertx2.model.*;
import vertx2.util.AcsApiUtils;
import vertx2.util.AcsConstants;
import vertx2.util.AutoBackupUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.TreeMap;

/**
 * Project:  SXA-CC (aka CCFG)
 *
 * Replacement Utils.
 *
 * @author: ronyang
 */
public class ReplacementUtils {
    private static Logger log = LoggerFactory.getLogger(ReplacementUtils.class);


    /**
     * Perform a replacement.
     *
     * @param eventBus
     * @param discoveryWorkFlowHashMap
     * @param cpe
     * @param orgId
     * @param cpeKey
     * @param oldDeviceKey
     */
    public static void doReplacement(
            final EventBus eventBus,
            final RedisClient redisClient,
            final TreeMap<String, Workflow> discoveryWorkFlowHashMap,
            final JsonObject cpe,
            final String orgId,
            final String cpeKey,
            final String oldDeviceKey,
            final boolean bFsanBasedReplacement) {
        /**
         * TODO: Save Decommission Event for the old device
         */

        if (bFsanBasedReplacement) {
            /**
             * This is FSAN based replacement.
             *
             * Update Old Device Record:
             */
            try {
                VertxMongoUtils.update(
                        eventBus,
                        Cpe.CPE_COLLECTION_NAME,
                        oldDeviceKey,
                        VertxMongoUtils.getUpdatesObject(
                                null,
                                VertxMongoUtils.addUnset(null, Cpe.DB_FIELD_NAME_TO_BE_REPLACED_BY),
                                null,
                                null,
                                null
                        ),
                        null
                );
            } catch (SxaVertxException e) {
                e.printStackTrace();
            }
        } else {
            /**
             * RegId-based Replacement.
             */
        }

        /**
         * Upgrade SW Image per discovery workflow if applicable
         */
        WorkflowAction upgradeAction = null;
        if (discoveryWorkFlowHashMap != null) {
            // This org has one or more discovery workflows
            // Traverse all discovery workflows and find the first matching workflow that has SW upgrade action
            for (Object aPojo : discoveryWorkFlowHashMap.values()) {
                Workflow aWorkflow = (Workflow) aPojo;
                if (aWorkflow == null
                        || !Workflow.STATE_IN_PROGRESS.equals(aWorkflow.state) // Workflow has been suspended
                        || aWorkflow.cpeMatcher == null    // No matcher (internal error)
                        || aWorkflow.matchCpe(cpe) == false  // Does not match the workflow
                        ) {
                    continue;
                }

                // Found a match
                for (WorkflowAction anAction : aWorkflow.actionChain) {
                    if (WorkflowActionEnum.DOWNLOAD_FW_IMAGE.equals(anAction.actionEnum) && anAction.file != null) {
                        // Found it
                        upgradeAction = anAction;
                        break;
                    }
                }

                if (upgradeAction != null) {
                    // Skip the rest workflows
                    // TODO: Do we want to traverse all workflows and do longest match?
                    break;
                }
            }
        }

        if (upgradeAction != null) {
            log.info(cpeKey + ": Replacement: Upgrading new device to "
                    + upgradeAction.file.getField(AcsFile.FIELD_NAME_VERSION));

            // Build a device-op API request
            JsonObject requestBody = new JsonObject()
                    .putObject(CpeDeviceOp.FIELD_NAME_CPE_DB_OBJECT, cpe)
                    .putString(CpeDeviceOp.FIELD_NAME_OPERATION, CpeDeviceOpTypeEnum.Download.name())
                    .putObject(CpeDeviceOp.FIELD_NAME_EXEC_POLICY, ExecPolicy.EXEC_POLICY_WITH_DOWNLOAD_TIMEOUT)
                    .putString(CpeDeviceOp.FIELD_NAME_FILE_TYPE, AcsFileType.Image.tr069DownloadFileTypeString)
                    .putObject(CpeDeviceOp.FIELD_NAME_FILE_STRUCT, upgradeAction.file);

            /**
             * Send the device-op request
             */
            AcsApiUtils.sendApiRequest(
                    eventBus,
                    AcsConstants.ACS_API_SERVICE_DEVICE_OP,
                    orgId,
                    AcsApiCrudTypeEnum.Create,
                    requestBody,
                    ExecPolicy.DEFAULT_DOWNLOAD_TIMEOUT + 10000,    // wait 10 extra seconds
                    new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                            if (asyncResult == null || asyncResult.failed()) {
                                log.error(cpeKey + ": Upgrade-before-Replacement Failed due to "
                                        + (asyncResult == null ? "(null)" : asyncResult.cause().getMessage() + "!"));
                            } else {
                                log.info(cpeKey + ": Upgrade-before-Replacement Succeeded.");

                                /**
                                 * Step 2: Restore backup config file of the old device to the new device
                                 */
                                restoreBackupConfigFile(
                                        eventBus,
                                        redisClient,
                                        cpe,
                                        orgId,
                                        cpeKey,
                                        oldDeviceKey
                                );
                            }
                        }
                    }
            );
        } else {
            /**
             * Just Restore backup config file of the old device to the new device
             */
            restoreBackupConfigFile(
                    eventBus,
                    redisClient,
                    cpe,
                    orgId,
                    cpeKey,
                    oldDeviceKey
            );
        }
    }

    /**
     * Restore the auto backup config file of an old device to a new device.
     *
     * @param cpe
     * @param orgId
     * @param cpeKey
     * @param oldDeviceKey
     */
    public static void restoreBackupConfigFile(
            final EventBus eventBus,
            final RedisClient redisClient,
            final JsonObject cpe,
            final String orgId,
            final String cpeKey,
            final String oldDeviceKey) {
        log.info("Replacement: Restoring auto backup of " + oldDeviceKey + " to " + cpeKey + "...");

        /**
         * Check if the old device has an auto-backup config file
         */
        try {
            VertxMongoUtils.count(
                    eventBus,
                    AcsFile.DB_COLLECTION_NAME,
                    new JsonObject().putString(AcsConstants.FIELD_NAME_ID, AcsFile.getAutoBackupConfigFileId(oldDeviceKey)),
                    new Handler<Long>() {
                        @Override
                        public void handle(Long count) {
                            if (count == null) {
                                log.error("Replacement Failed due to internal DB timeout when "
                                        + "query the auto backup record for " + oldDeviceKey + "!");
                                // Save event
                                Event.saveEvent(
                                        eventBus,
                                        orgId,
                                        Cpe.getSnByCpeKey(cpeKey),
                                        EventTypeEnum.ReplacementFailure,
                                        EventSourceEnum.System,
                                        new JsonObject()
                                                .putString("old device", Cpe.getSnByCpeKey(oldDeviceKey))
                                                .putString("cause", "Internal DB Error")
                                );
                            } else if (count == 0) {
                                log.error("Replacement Failed due to no auto backup found for the old device "
                                        + oldDeviceKey + "!");
                                // Save event
                                Event.saveEvent(
                                        eventBus,
                                        orgId,
                                        Cpe.getSnByCpeKey(cpeKey),
                                        EventTypeEnum.ReplacementFailure,
                                        EventSourceEnum.System,
                                        new JsonObject()
                                                .putString("old device", Cpe.getSnByCpeKey(oldDeviceKey))
                                                .putString("cause", "No auto backup found for the old device")
                                );
                            } else {
                                // Send the "Download" device op
                                sendBackupDeviceOp(
                                        eventBus,
                                        redisClient,
                                        cpe,
                                        orgId,
                                        cpeKey,
                                        oldDeviceKey
                                );
                            }
                        }
                    }
            );
        } catch (SxaVertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send the actual "Download" Device Op for the restoration.
     *
     * @param eventBus
     * @param cpe
     * @param orgId
     * @param cpeKey
     * @param oldDeviceKey
     */
    public static void sendBackupDeviceOp(
            final EventBus eventBus,
            final RedisClient redisClient,
            final JsonObject cpe,
            final String orgId,
            final String cpeKey,
            final String oldDeviceKey) {
        // Build a device-op API request
        JsonObject requestBody = new JsonObject()
                .putObject(CpeDeviceOp.FIELD_NAME_CPE_DB_OBJECT, cpe)
                .putString(CpeDeviceOp.FIELD_NAME_OPERATION, CpeDeviceOpTypeEnum.Download.name())
                .putString(CpeDeviceOp.FIELD_NAME_FILE_TYPE, AcsFileType.ConfigFile.tr069DownloadFileTypeString)
                .putObject(CpeDeviceOp.FIELD_NAME_EXEC_POLICY, ExecPolicy.EXEC_POLICY_WITH_DOWNLOAD_TIMEOUT)
                .putObject(
                        CpeDeviceOp.FIELD_NAME_FILE_STRUCT,
                        AcsFile.buildAutoBackupFileRecordWithUploadURL(oldDeviceKey)
                );

        /**
         * Send the device-op request
         */
        AcsApiUtils.sendApiRequest(
                eventBus,
                AcsConstants.ACS_API_SERVICE_DEVICE_OP,
                orgId,
                AcsApiCrudTypeEnum.Create,
                requestBody,
                ExecPolicy.DEFAULT_DOWNLOAD_TIMEOUT + 10000,    // wait 10 extra seconds
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                        EventTypeEnum eventType;
                        JsonObject details = new JsonObject().putString(
                                "old device",
                                Cpe.getSnByCpeKey(oldDeviceKey)
                        );

                        if (asyncResult == null || asyncResult.failed()) {
                            log.error(cpeKey + ": Replacement Failed due to "
                                    + (asyncResult == null? "(null)" : asyncResult.cause().getMessage() + "!"));
                            if (asyncResult != null && asyncResult.cause() != null) {
                                details.putString("cause", asyncResult.cause().getMessage());
                            }
                            eventType = EventTypeEnum.ReplacementFailure;
                        } else if (asyncResult.succeeded()) {
                            JsonObject deviceOpResult = asyncResult.result().body();
                            String deviceOpStatusCode =
                                    deviceOpResult == null?
                                            HttpResponseStatus.INTERNAL_SERVER_ERROR.toString()
                                            :
                                            deviceOpResult.getString(
                                                    AcsConstants.FIELD_NAME_STATUS_CODE,
                                                    HttpResponseStatus.INTERNAL_SERVER_ERROR.toString()
                                            );
                            if (HttpResponseStatus.OK.toString().equals(deviceOpStatusCode)) {
                                /**
                                 * Device Op Succeeded
                                 */
                                eventType = EventTypeEnum.Replacement;
                                log.info(cpeKey + ": Replacement Succeeded. oldDevice: " + oldDeviceKey);
                            } else {
                                /**
                                 * Device Op Failed
                                 */
                                if (deviceOpResult != null &&
                                        deviceOpResult.containsField(AcsConstants.FIELD_NAME_ERROR)) {
                                    details.putString("cause", deviceOpResult.getString(AcsConstants.FIELD_NAME_ERROR));
                                }
                                eventType = EventTypeEnum.ReplacementFailure;
                            }

                            /**
                             * Save Replacement Event
                             */
                            Event.saveEvent(
                                    eventBus,
                                    orgId,
                                    Cpe.getSnByCpeKey(cpeKey),
                                    eventType,
                                    EventSourceEnum.System,
                                    details
                            );

                            /**
                             * Enqueue an auto backup task for this new device
                             */
                            AutoBackupUtils.addToQueue(
                                    redisClient,
                                    Cpe.toBasicJsonObjectForConnReq(cpe)
                            );
                        }
                    }
                }
        );
    }

}
