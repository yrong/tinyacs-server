package vertx.cpeserver.deviceop;

import vertx.cpeserver.session.CwmpRequest;
import vertx.cpeserver.session.CwmpSession;
import vertx.cwmp.CwmpException;
import vertx.cwmp.CwmpMessage;
import vertx.model.*;
import vertx.util.AcsApiUtils;
import vertx.util.AcsConfigProperties;
import vertx.util.AcsConstants;
import dslforumOrgCwmp12.UploadDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * This class defines methods for Upload Operations.
 *
 * @author: ronyang
 */
public class Upload {
    private static final Logger log = LoggerFactory.getLogger(Upload.class.getName());

    /**
     * Static Response(s)
     */
    private static String INVALID_FILE_TYPE = "Invalid File Type!";
    private static JsonObject INTERNAL_SERVER_ERROR = new JsonObject()
            .put(AcsConstants.FIELD_NAME_ERROR, "Internal ACS Server Error.");

    /**
     * Create the request and add it to the queue using a custom response handler.
     *
     * @param session
     * @param deviceOp
     * @
     * @throws vertx.cwmp.CwmpException
     */
    public static void start(final JsonObject deviceOp, final CwmpSession session)
            throws CwmpException {
        // Extract file type (config vs. log)
        String cwmpFileType = deviceOp.getString(CpeDeviceOp.FIELD_NAME_FILE_TYPE);
        final AcsFileType acsFileType = AcsFileType.getAcsFileTypeByDeviceOpFileType(cwmpFileType);
        if (!AcsFileType.ConfigFile.equals(acsFileType) && !AcsFileType.LogFile.equals(acsFileType)) {
            // Must be config file or log file
            DeviceOpUtils.callbackInvalidReq(
                    session,
                    deviceOp,
                    INVALID_FILE_TYPE
            );
            return;
        }

        log.info(session.cpeKey + ": Processing a " + acsFileType.typeString + " Upload request...");

        // Extract the file struct if any
        JsonObject fileStruct = deviceOp.getJsonObject(CpeDeviceOp.FIELD_NAME_FILE_STRUCT);
        if (fileStruct != null) {
            /**
             * File Struct is present (i.e. system performed auto backup).
             *
             * Enqueue the "Upload" Request now
             */
            doUpload(
                    session,
                    deviceOp,
                    fileStruct
                            .getString(AcsFile.FIELD_NAME_UPLOAD_URL)
                            .replaceFirst(AcsConfigProperties.FILE_SERVER_HOST, session.getAcsHostname()),
                    fileStruct.getString(AcsFile.FIELD_NAME_USERNAME),
                    fileStruct.getString(AcsFile.FIELD_NAME_PASSWORD)
            );
        } else {
            /**
             * File Struct is NOT present (i.e. Operator requested manual backup or log file upload).
             *
             * First talk to the ACS "File" Service to register this new upload and get an upload URL
             */
            // Build a new file creation request
            String fileName = AcsFile.autoGenerateFileName(session.cpe.deviceId.sn, acsFileType);
            JsonObject fileCreationReq = new JsonObject()
                    // add CPE Identifier
                    .put(AcsConstants.FIELD_NAME_CPE_ID,
                            new JsonObject()
                                    .put(CpeIdentifier.FIELD_NAME_OUI, session.cpe.deviceId.oui)
                                    .put(CpeIdentifier.FIELD_NAME_SN, session.cpe.deviceId.sn)
                    )
                    .put(AcsConstants.FIELD_NAME_NAME, fileName)
                    .put(AcsFile.FIELD_NAME_TYPE, acsFileType.typeString);

            // CSR Username
            String csrUsername = deviceOp.getString(CpeDeviceOp.FIELD_NAME_CSR_USERNAME);
            if (csrUsername != null) {
                fileCreationReq.put(AcsFile.FIELD_NAME_CSR_USERNAME, csrUsername);
            }
            // Description
            String description = deviceOp.getString(CpeDeviceOp.FIELD_NAME_DESCRIPTION);
            if (description != null) {
                fileCreationReq.put(AcsConstants.FIELD_NAME_DESCRIPTION, description);
            }

            // Send the request to File Service API
            AcsApiUtils.sendApiRequest(
                    session.vertx.eventBus(),
                    AcsConstants.ACS_API_SERVICE_FILE,
                    deviceOp.getString(AcsConstants.FIELD_NAME_ORG_ID),
                    AcsApiCrudTypeEnum.Create,
                    fileCreationReq,
                    ExecPolicy.DEFAULT_DEVICE_OP_TIMEOUT,
                    new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> asyncResult) {
                            /**
                             * Try to extract upload URL from result
                             */
                            String uploadUrl = null;
                            String internalFileId = null;
                            String username = null;
                            String password = null;
                            if (asyncResult.succeeded()) {
                                // received reply from API server
                                JsonObject result = asyncResult.result().body();
                                uploadUrl = result.getString(AcsFile.FIELD_NAME_UPLOAD_URL)
                                        .replaceFirst(AcsConfigProperties.FILE_SERVER_HOST, session.getAcsHostname());
                                internalFileId = result.getString(AcsConstants.FIELD_NAME_ID);
                                if (uploadUrl == null || internalFileId == null) {
                                    log.error("ACS File Service API returned unexpected error!\n" + result.encodePrettily());
                                } else {
                                    log.info("Async File Service Result:\n" + result.encodePrettily());
                                    username = result.getString(AcsFile.FIELD_NAME_USERNAME);
                                    password = result.getString(AcsFile.FIELD_NAME_PASSWORD);

                                    // save credentials
                                    if (AcsFileType.LogFile.equals(acsFileType)) {
                                        deviceOp.put(AcsFile.FIELD_NAME_USERNAME, username);
                                        deviceOp.put(AcsFile.FIELD_NAME_PASSWORD, password);
                                    }
                                }
                            } else {
                                log.error("Failed to invoke ACS File Service API!");
                            }

                            if (uploadUrl == null) {
                                /**
                                 * Failed
                                 */
                                DeviceOpUtils.callback(
                                        session,
                                        deviceOp,
                                        CpeDeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                                        INTERNAL_SERVER_ERROR
                                );
                            } else {
                                // Save the upload URL into the device op
                                deviceOp.put(CpeDeviceOp.FIELD_NAME_INTERNAL_FILE_ID, internalFileId);

                                /**
                                 * Enqueue the "Upload" Request
                                 */
                                doUpload(
                                        session,
                                        deviceOp,
                                        uploadUrl,
                                        username,
                                        password
                                );
                            }
                        }
                    }
            );
        }
    }

    /**
     * Build and Enqueue the  "Upload" RPC Message.
     *
     * @param session
     * @param deviceOp
     * @param uploadUrl
     * @param username
     * @param password
     */
    public static void doUpload(
            CwmpSession session,
            JsonObject deviceOp,
            String uploadUrl,
            String username,
            String password) {
        // Build a new "Upload" Message
        CwmpMessage uploadMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        uploadMessage.rpcMessageName = "Upload";
        UploadDocument.Upload upload =
                uploadMessage.soapEnv.getBody().addNewUpload();

        /**
         * Add File Type and URL
         */
        upload.setFileType(deviceOp.getString(CpeDeviceOp.FIELD_NAME_FILE_TYPE));
        upload.setURL(uploadUrl);

        /**
         * Add username/password if any
         */
        if (username != null) {
            upload.setUsername(username);
            if (password != null) {
                upload.setPassword(password);
            }
        }

        /**
         * could be used later for correlation with the M_Boot Inform
         */
        upload.setCommandKey(deviceOp.getString(CpeDeviceOp.FIELD_NAME_ID));

        /**
         * Build a new CWMP Request and add the new request to the queue of the this session
         */
        session.addNewRequest(new CwmpRequest(
                        CwmpRequest.CWMP_REQUESTER_ACS,
                        uploadMessage,
                        "Upload",
                        new UploadResponseHandler(deviceOp)
                )
        );
    }

    /**
     * Response handler for the "Upload" request
     */
    public static class UploadResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {

        /**
         * Constructor
         *
         * @param deviceOp
         */
        public UploadResponseHandler(JsonObject deviceOp) {
            this.deviceOp = deviceOp;
        }

        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            /**
             * Check the response status.
             * Per TR-069:
             *
             * A successful response to this method returns an integer enumeration defined as
             * follows:
             * 0 = Upload has completed and been applied.
             * 1 = Upload has not yet been completed and applied (for example, if the CPE needs
             * to reboot itself before it can perform the file Upload, or if the CPE needs to reboot
             * itself before it can apply the Uploaded file).
             *
             * If the value of this argument is non-zero, the CPE MUST subsequently call the
             * TransferComplete method to indicate the completion status of this Upload (either
             * successful or unsuccessful) either later in the same Session or in a subsequent Session.
             */
            int status = responseMessage.soapEnv.getBody().getUploadResponse().getStatus();

            if (status == 0) {
                /**
                 * Upload has completed.
                 *
                 * send the result to the provided callback URL if any
                 */
                DeviceOpUtils.callback(
                        session,
                        deviceOp,
                        CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                        null
                );
            } else {
                /**
                 * Upload has not yet been completed and applied, the CPE MUST subsequently call the
                 * TransferComplete method to indicate the completion status of this Upload.
                 *
                 * Let us mark this device op as in-progress
                 */
                DeviceOpUtils.saveInProgressDeviceOp(session, deviceOp, CpeDeviceOpTypeEnum.Upload);
            }
        }
    }
}
