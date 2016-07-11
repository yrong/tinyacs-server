package vertx2.cpeserver.deviceop;

import vertx2.VertxException;
import vertx2.VertxJsonUtils;
import vertx2.VertxMongoUtils;
import vertx2.cpeserver.session.CwmpRequest;
import vertx2.cpeserver.session.CwmpSession;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import vertx2.model.*;
import vertx2.util.AcsConstants;
import dslforumOrgCwmp12.DownloadDocument;
import dslforumOrgCwmp12.GetParameterValuesResponseDocument;
import dslforumOrgCwmp12.ParameterNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * This class defines methods for Download Operations.
 *
 * @author: ronyang
 */
public class Download {
    private static final Logger log = LoggerFactory.getLogger(Download.class.getName());

    /**
     * TR098 Path for Vendor Config Files
     */
    public static final String ALL_VENDOR_CONFIG_FILES = "InternetGatewayDevice.DeviceInfo.VendorConfigFile.";
    public static final ParameterNames PARAM_NAMES_FOR_ALL_VENDOR_CFG_FILES = GetAllVendorConfigFilesParamNames();

    /**
     * Calix Vendor Config File Names
     */
    public static final String CALIX_IGD_CFG_FILE_NAME = "calix.igd.conf";
    public static final String CALIX_SIP_CFG_FILE_NAME = "calix.sip.conf";

    /**
     * Static Response(s)
     */
    private static String INVALID_FILE_ID = "Internal ACS Server Error! (Invalid Internal File Id)";
    private static JsonObject INTERNAL_SERVER_ERROR = new JsonObject()
            .putString(AcsConstants.FIELD_NAME_ERROR, "Internal ACS Server Error.");
    public static final JsonObject MONGODB_TIMED_OUT = new JsonObject()
            .putString(AcsConstants.FIELD_NAME_ERROR, "ACS DB Timed Out! Please contact Calix Support Team.");

    /**
     * Add download URL/username/password info into the given "Download" RPC message, and send RPC if needed.
     *
     * @param session
     * @param deviceOp
     * @param anAcsFile
     */
    public static void doDownload(
            CwmpSession session,
            JsonObject deviceOp,
            JsonObject anAcsFile) {
        final CwmpMessage downloadMessage = new CwmpMessage(session.cwmpVersion, session.messageId++);
        downloadMessage.rpcMessageName = "Download";
        final DownloadDocument.Download download =
                downloadMessage.soapEnv.getBody().addNewDownload();

        /**
         * could be used later for correlation with the M_Boot Inform
         */
        download.setCommandKey(deviceOp.getString(CpeDeviceOp.FIELD_NAME_ID));

        // Extract internal file id string
        String internalFileId = anAcsFile.getString(AcsConstants.FIELD_NAME_ID);
        String fileType = anAcsFile.getString(AcsFile.FIELD_NAME_TYPE);

        if (AcsFileType.Image.typeString.equals(fileType)) {
            /**
             * Download SW Image
             */
            download.setFileType(AcsFileType.Image.tr069DownloadFileTypeString);
            if (anAcsFile.containsField(AcsFile.FIELD_NAME_VERSION)) {
                /**
                 * Return now if the target device is already running the same version.
                 */
                String imageVersion = anAcsFile.getString(AcsFile.FIELD_NAME_VERSION);
                if (imageVersion.equals(session.cpe.deviceId.swVersion)) {
                    log.info(session.cpeKey + ": Already running " + imageVersion + ".");
                    DeviceOpUtils.callback(
                            session,
                            deviceOp,
                            CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                            null
                    );
                    return;
                } else {
                    /**
                     * Add version into device op
                     */
                    deviceOp.putString(CpeDeviceOp.FIELD_NAME_VERSION, imageVersion);
                    log.debug(session.cpeKey + ": Downloading Image version " + imageVersion);
                }
            }

            // Set the download URL/Credentials
            Organization org = session.sessionVertice.organizationCache
                    .getOrgById(session.cpe.orgId);
            if (org != null &&
                    org.extImageServer != null &&
                    org.extImageServer.baseUrl != null) {
                /**
                 * Use External File Server URL and credentials
                 */
                download.setURL(
                        org.extImageServer.baseUrl + AcsFile.getExternalFilePath(anAcsFile)
                );

                log.debug("Org Image Server Credential: "
                        + org.extImageServer.username + ":" + org.extImageServer.password);
                if (org.extImageServer.username != null) {
                    download.setUsername(org.extImageServer.username);
                    if (org.extImageServer.password != null) {
                        download.setPassword(org.extImageServer.password);
                    }
                }
            } else {
                /**
                 * Use Internal File Server
                 */
                download.setURL(AcsFile.getDownloadUrl(session.getAcsHostname(), internalFileId));
                if (anAcsFile.containsField(AcsFile.FIELD_NAME_USERNAME)) {
                    download.setUsername(anAcsFile.getString(AcsFile.FIELD_NAME_USERNAME));
                    if (anAcsFile.containsField(AcsFile.FIELD_NAME_PASSWORD)) {
                        download.setPassword(anAcsFile.getString(AcsFile.FIELD_NAME_PASSWORD));
                    }
                }
            }

            /**
             * Enqueue the "Download" request now
             */
            session.addNewRequest(new CwmpRequest(
                            CwmpRequest.CWMP_REQUESTER_ACS,
                            downloadMessage,
                            "Download",
                            new DownloadResponseHandler(deviceOp)
                    )
            );
        } else {
            /**
             * Download Config File. Always Use Internal File Server
             */
            if (AcsFileType.ConfigFile.typeString.equals(fileType)) {
                download.setFileType(AcsFileType.ConfigFile.tr069DownloadFileTypeString);
            } else if (AcsFileType.SipConfigFile.typeString.equals(fileType)) {
                download.setFileType(AcsFileType.SipConfigFile.tr069DownloadFileTypeString);
            } else {
                String error = "Unsupported File Type " + fileType + "!";
                log.error(error);
                DeviceOpUtils.callbackInternalError(
                        session,
                        deviceOp,
                        new JsonObject().putString(AcsConstants.FIELD_NAME_ERROR, error)
                );
                return;
            }
            download.setURL(AcsFile.getDownloadUrl(session.getAcsHostname(), internalFileId));
            if (anAcsFile.containsField(AcsFile.FIELD_NAME_USERNAME)) {
                download.setUsername(anAcsFile.getString(AcsFile.FIELD_NAME_USERNAME));
                if (anAcsFile.containsField(AcsFile.FIELD_NAME_PASSWORD)) {
                    download.setPassword(anAcsFile.getString(AcsFile.FIELD_NAME_PASSWORD));
                }
            }

            /**
             * When restoring a previous backup, Reset ChangeCounter (on our side) to 1 to avoid auto backup
             */
            if (AcsFile.isAutoBackup(anAcsFile)) {
                session.cpe.deviceId.changeCounter = 1;
                session.cpe.addSet(Cpe.DB_FIELD_NAME_CHANGE_COUNTER, 1);
            }

            if (anAcsFile.containsField(AcsFile.FIELD_NAME_VERSION)) {
                /**
                 * Send a "GetParameterValues" message to get all vendor config files (for version check)
                 */
                GetParameterValues.start(
                        session,
                        PARAM_NAMES_FOR_ALL_VENDOR_CFG_FILES,
                        new GetConfigFileVersionResponseHandler(
                                deviceOp,
                                fileType,
                                anAcsFile.getString(AcsFile.FIELD_NAME_VERSION),
                                downloadMessage
                        ),
                        CwmpRequest.CWMP_REQUESTER_LOCAL
                );
            } else {
                /**
                 * Enqueue the "Download" request now (no version check)
                 */
                session.addNewRequest(new CwmpRequest(
                                CwmpRequest.CWMP_REQUESTER_ACS,
                                downloadMessage,
                                "Download",
                                new DownloadResponseHandler(deviceOp)
                        )
                );
            }
        }
    }

    /**
     * Create the request and add it to the queue using a custom response handler.
     *
     * @param session
     * @param deviceOp
     * @
     * @throws vertx2.cwmp.CwmpException
     */
    public static void start(final JsonObject deviceOp, final CwmpSession session)
            throws CwmpException {
        final JsonObject anAcsFile = deviceOp.getObject(CpeDeviceOp.FIELD_NAME_FILE_STRUCT);
        if (anAcsFile != null) {
            /**
             * File Struct is present.
             */
            doDownload(session, deviceOp, anAcsFile);
        } else {
            /**
             * File URL is not present. Check for internal file id
             */
            final String internalFileId = deviceOp.getString(CpeDeviceOp.FIELD_NAME_INTERNAL_FILE_ID);
            if (internalFileId != null) {
                /**
                 * Query "files" DB to get the URL
                 */
                try {
                    VertxMongoUtils.findOne(
                            session.vertx.eventBus(),
                            AcsFile.DB_COLLECTION_NAME,
                            new JsonObject().putString(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, internalFileId),
                            new VertxMongoUtils.FindOneHandler(new Handler<JsonObject>() {
                                @Override
                                public void handle(JsonObject anAcsFile) {
                                    // Check for MongoDB timeouts
                                    if (VertxMongoUtils.FIND_ONE_TIMED_OUT.equals(anAcsFile)) {
                                        DeviceOpUtils.callbackInternalError(
                                                session,
                                                deviceOp,
                                                MONGODB_TIMED_OUT
                                        );
                                        return;
                                    }
                                    if (anAcsFile == null) {
                                        log.error("Invalid Internal File Id " + internalFileId + "!");
                                        DeviceOpUtils.callbackInvalidReq(
                                                session,
                                                deviceOp,
                                                INVALID_FILE_ID
                                        );
                                    } else {
                                        doDownload(session, deviceOp, anAcsFile);
                                    }
                                }
                            }),
                            null
                    );
                } catch (VertxException e) {
                    log.error(session.cpeKey + ": failed to query files DB with internal id " + internalFileId
                            + " due to exception " + e.getMessage() + "!");
                    /**
                     * Failed for some weird reason
                     */
                    DeviceOpUtils.callback(
                            session,
                            deviceOp,
                            CpeDeviceOp.CPE_DEVICE_OP_STATE_FAILED,
                            INTERNAL_SERVER_ERROR
                    );
                }
            } else {
                DeviceOpUtils.callbackInvalidReq(
                        session,
                        deviceOp,
                        "Both File URL and Internal File Id are missing!"
                );
                return;
            }
        }
    }

    /**
     * Response handler for the "Download" request
     */
    public static class DownloadResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {

        /**
         * Constructor
         *
         * @param deviceOp
         */
        public DownloadResponseHandler(JsonObject deviceOp) {
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
             * 0 = Download has completed and been applied.
             * 1 = Download has not yet been completed and applied (for example, if the CPE needs
             * to reboot itself before it can perform the file download, or if the CPE needs to reboot
             * itself before it can apply the downloaded file).
             *
             * If the value of this argument is non-zero, the CPE MUST subsequently call the
             * TransferComplete method to indicate the completion status of this download (either
             * successful or unsuccessful) either later in the same Session or in a subsequent Session.
             */
            int status = responseMessage.soapEnv.getBody().getDownloadResponse().getStatus();

            log.info(session.cpeKey + ": DownloadResponse with status " + status);

            if (status == 0) {
                /**
                 * Download has completed and been applied.
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
                 * Download has not yet been completed and applied, the CPE MUST subsequently call the
                 * TransferComplete method to indicate the completion status of this download.
                 *
                 * Let us mark this device op as in-progress
                 */
                DeviceOpUtils.saveInProgressDeviceOp(session, deviceOp, CpeDeviceOpTypeEnum.Download);
            }
        }
    }

    /**
     * Response handler for the "GetParameterValues" request (for retrieving the existing config file version)
     */
    public static class GetConfigFileVersionResponseHandler extends DeviceOpUtils.NbiDeviceOpResponseHandler {
        public String fileType;
        public String version;
        public CwmpMessage downloadMessage;

        /**
         * Constructor.
         *
         * @param deviceOp
         * @param fileType
         * @param version
         * @param downloadMessage
         */
        public GetConfigFileVersionResponseHandler(
                JsonObject deviceOp,
                String fileType,
                String version,
                CwmpMessage downloadMessage) {
            this.deviceOp = deviceOp;
            this.fileType = fileType;
            this.version = version;
            this.downloadMessage = downloadMessage;
        }

        /**
         * Abstract Response Handler Class that must be extended by actual requests
         *
         * @param session
         * @param request
         * @param responseMessage
         */
        @Override
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException {
            boolean bVersionMatch = false;

            /**
             * Validation
             */
            GetParameterValuesResponseDocument.GetParameterValuesResponse response =
                    responseMessage.soapEnv.getBody().getGetParameterValuesResponse();
            if (response == null || response.getParameterList() == null) {
                log.error("Null or Invalid GetParameterValuesResponse message!\n" +
                        responseMessage.soapEnv.getBody().xmlText());
            } else {
                /**
                 * Convert all parameter name/value pairs into a big JSON Object
                 */
                JsonObject paramValues = GetParameterValuesNbi.parameterValueStructsToJsonObject(
                        response.getParameterList().getParameterValueStructArray());
                JsonObject allCfgFiles = VertxJsonUtils.deepGet(paramValues, ALL_VENDOR_CONFIG_FILES);
                if (allCfgFiles != null && allCfgFiles.size() > 0) {
                    String currVersion = null;

                    /**
                     * Traverse all config file entries
                     */
                    for (String fileEntryIndex : allCfgFiles.getFieldNames()) {
                        JsonObject cfgFile = allCfgFiles.getObject(fileEntryIndex);

                        // Extract the name/version of this config file entry
                        String name = cfgFile.getString("Name");

                        if (AcsFileType.ConfigFile.typeString.equals(fileType)) {
                            if (CALIX_IGD_CFG_FILE_NAME.equals(name)) {
                                // Found the IGD Config File Entry that we are looking for
                                currVersion = cfgFile.getString("Version");
                                log.info(session.cpeKey + ": Current Calix IGD Config File Version: " + currVersion);
                                break;
                            }
                        } else if (AcsFileType.SipConfigFile.typeString.equals(fileType)) {
                            if (CALIX_SIP_CFG_FILE_NAME.equals(name)) {
                                // Found the SIP Config File Entry that we are looking for
                                currVersion = cfgFile.getString("Version");
                                log.info(session.cpeKey + ": Current Calix SIP Config File Version: " + currVersion);
                                break;
                            }
                        }
                    }

                    bVersionMatch = version.equals(currVersion);
                }
            }

            if (bVersionMatch) {
                log.info(session.cpeKey + ": Current Version Matches the Config File Version to be"
                        + " downloaded. Skip the actual download.");
                DeviceOpUtils.callback(
                        session,
                        deviceOp,
                        CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                        null
                );
                return;
            } else {
                log.info(session.cpeKey + ": Download " + fileType + " (version: " + version + ")...");

                /**
                 * Enqueue the "Download" request now (no version check)
                 */
                session.addNewRequest(new CwmpRequest(
                                CwmpRequest.CWMP_REQUESTER_ACS,
                                downloadMessage,
                                "Download",
                                new DownloadResponseHandler(deviceOp)
                        )
                );
            }
        }
    }

    /**
     * Return a ParameterNames to get all vendor config files
     */
    public static ParameterNames GetAllVendorConfigFilesParamNames() {
        ParameterNames parameterNames = ParameterNames.Factory.newInstance();
        parameterNames.addString(ALL_VENDOR_CONFIG_FILES);
        return parameterNames;
    }
}
