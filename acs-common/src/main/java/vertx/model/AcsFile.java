package vertx.model;

import io.vertx.ext.mongo.MongoClient;
import vertx.VertxException;
import vertx.VertxJsonUtils;
import vertx.VertxMongoUtils;
import vertx.CcException;
import vertx.util.AcsConfigProperties;
import vertx.util.AcsConstants;
import vertx.util.AcsMiscUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Project:  cwmp
 *
 * ACS "File" Data Model Definition.
 *
 * An ACS "File" could be one of the following types:
 *  - SW/FW Image
 *  - Configuration File
 *  - Log File
 *  - etc
 *
 * @author: ronyang
 */
public class AcsFile {
    private static final Logger log = LoggerFactory.getLogger(AcsFile.class.getName());

    /**
     * DB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "CWMP-files";

    /**
     * Auto Backup Constants
     */
    public static final String AUTO_BACKUP_CONFIG_FILE_NAME = "Auto Backup";
    public static final String AUTO_BACKUP_CONFIG_FILE_ID_PREFIX = "auto-backup-";
    public static final String AUTO_BACKUP_CONFIG_FILE_PASSWORD = "auto-backup";
    // password is same as CPE id/key

    /**
     * Field Name Constants
     */
    public static final String FIELD_NAME_TYPE = "type";
    public static final String FIELD_NAME_VERSION = "version";
    public static final String FIELD_NAME_MANUFACTURER = "manufacturer";
    public static final String FIELD_NAME_MODELS = "models";
    public static final String FIELD_NAME_SIZE = "size";
    public static final String FIELD_NAME_UPLOAD_TIME = "uploadTime";
    public static final String FIELD_NAME_NUMBER_OF_DOWNLOADS = "numberOfDownloads";
    public static final String FIELD_NAME_UPLOAD_URL = "uploadUrl";
    public static final String FIELD_NAME_DOWNLOAD_URL = "downloadUrl";
    public static final String FIELD_NAME_CSR_USERNAME = "csrUsername";
    public static final String FIELD_NAME_USERNAME = AcsConstants.FIELD_NAME_USERNAME;
    public static final String FIELD_NAME_PASSWORD = AcsConstants.FIELD_NAME_PASSWORD;
    public static final String FIELD_NAME_EXTERNAL_URL = "externalUrl";
    public static final String FIELD_NAME_EXTERNAL_USERNAME = "externalUsername";
    public static final String FIELD_NAME_EXTERNAL_PASSWORD = "externalPassword";
    public static final String FIELD_NAME_TEXT_CONTENT = "content";   // ASCII Text Content
    public static final String FIELD_NAME_BINARY_CONTENT = "binaryContent";   // Binary Content
    public static final String FIELD_NAME_CRC32 = "crc32";

    /**
     * Editable Fields
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{
            add(AcsConstants.FIELD_NAME_DESCRIPTION);
            /*
            add(FIELD_NAME_VERSION);
            add(FIELD_NAME_MODELS);
            add(FIELD_NAME_TEXT_CONTENT);
            add(FIELD_NAME_BINARY_CONTENT);
            */
    }};

    /**
     * Define static JSON Field Validators for SW/FW Images
     */
    public static final VertxJsonUtils.JsonFieldValidator imageMandatoryFields = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_TYPE, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_MANUFACTURER, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator imageOptionalFields = new VertxJsonUtils.JsonFieldValidator()
            .append(FIELD_NAME_EXTERNAL_URL, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_USERNAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PASSWORD, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_CSR_USERNAME, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_MODELS, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_VERSION, VertxJsonUtils.JsonFieldType.String);

    /**
     * Define static JSON Field Validators for Config Files
     */
    public static final VertxJsonUtils.JsonFieldValidator configFileMandatoryFields = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_TYPE, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator configFileOptionalFields = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_CPE_ID, VertxJsonUtils.JsonFieldType.JsonObject)
            .append(FIELD_NAME_CSR_USERNAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_BINARY_CONTENT, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_TEXT_CONTENT, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_EXTERNAL_URL, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_USERNAME, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_PASSWORD, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_MANUFACTURER, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_MODELS, VertxJsonUtils.JsonFieldType.JsonArray)
            .append(FIELD_NAME_CRC32, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_VERSION, VertxJsonUtils.JsonFieldType.String);

    /**
     * Define static JSON Field Validators for Log Files
     */
    public static final VertxJsonUtils.JsonFieldValidator logFileMandatoryFields = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_CPE_ID, VertxJsonUtils.JsonFieldType.JsonObject)
            .append(FIELD_NAME_TYPE, VertxJsonUtils.JsonFieldType.String);

    public static final VertxJsonUtils.JsonFieldValidator logFileOptionalFields = new VertxJsonUtils.JsonFieldValidator()
            .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
            .append(FIELD_NAME_CSR_USERNAME, VertxJsonUtils.JsonFieldType.String)
            .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String);

    /**
     * Static Exception(s)
     */
    public static final VertxException INVALID_FILE_NAME =
            new VertxException("Invalid File Name!");
    public static final VertxException INVALID_EXTERNAL_URL =
            new VertxException("Invalid External URL!");

    /**
     * Validate a JSON Object
     */
    public static void validate(JsonObject file) throws VertxException{
        /**
         * Get File Type
         */
        AcsFileType fileType;
        try {
            fileType = AcsFileType.getAcsFileType(file.getString(FIELD_NAME_TYPE));
        } catch (Exception ex) {
            throw new CcException("Invalid File Type! " + ex.getMessage() + "!");
        }

        /**
         * Validate other field by file type
         */
        switch (fileType) {
            case Image:
                VertxJsonUtils.validateFields(file, imageMandatoryFields, imageOptionalFields);
                break;

            case ConfigFile:
            case SipConfigFile:
                VertxJsonUtils.validateFields(file, configFileMandatoryFields, configFileOptionalFields);
                String content = file .getString(FIELD_NAME_TEXT_CONTENT);
                if (content != null) {
                    // TODO: Content of config file must be a valid XML string
                }
                break;

            case LogFile:
                VertxJsonUtils.validateFields(file, logFileMandatoryFields, logFileOptionalFields);
                break;

            default:
                throw new VertxException("Invalid File Type " + file.getString(FIELD_NAME_TYPE));
        }

        /**
         * Validate File Name if any
         */
        String name = file.getString(AcsConstants.FIELD_NAME_NAME);
        if (name != null) {
            validateFilename(name);
        }

        /**
         * Validate External URL if any
         */
        String externalUrl = file.getString(FIELD_NAME_EXTERNAL_URL);
        if (externalUrl != null) {
            try {
                URL url = new URL(externalUrl);
                if (!url.getProtocol().equals("http") && !url.getProtocol().equals("ftp")) {
                    log.error("Protocol " + url.getProtocol() + " is not supported!");
                    throw INVALID_EXTERNAL_URL;
                }
            } catch (MalformedURLException e) {
                log.error("Malformed URL!");
                throw INVALID_EXTERNAL_URL;
            }
        }
    }

    /**
     * Validate File name.
     *
     * Rules:
     * Can only contain digits (0..9), letters (A..Z/a..z), and/or dots/underscores/dashes ('.'/'_'/'-').
     * Dot ('.') cannot be the last char in file name.
     *
     * @param name
     * @throws VertxException
     */
    public static void validateFilename(String name) throws VertxException{
        for (int i = 0; i < name.length(); i ++) {
            char aChar = name.charAt(i);
            if (aChar == '_' || aChar == '-') {
                continue;
            }
            if (aChar >= '0' && aChar <= '9') {
                continue;
            }
            if (aChar >= 'a' && aChar <= 'z') {
                continue;
            }
            if (aChar >= 'A' && aChar <= 'Z') {
                continue;
            }
            if (aChar == '.') {
                if (i == (name.length() - 1)) {
                    log.error("Dot cannot be the last char of file names!");
                    throw INVALID_FILE_NAME;
                } else {
                    continue;
                }
            }
            log.error("Found illegal char " + aChar + "!");
            throw INVALID_FILE_NAME;
        }
    }

    /**
     * Get the file path (within the ACS file store) by orgId/type/name.
     *
     * @param orgId
     * @param type
     * @param name
     * @return
     */
    public static String getFilePath(String orgId, AcsFileType type, String name) {
        return orgId + "/" + type.name() + "/" + name;
    }

    /**
     * Get the file root path for a given orgId
     *
     * @param orgId
     * @return
     */
    public static String getOrgFileRoot(String orgId) {
        return AcsConfigProperties.ACS_FILE_STORE_PATH + "/" + orgId;
    }

    /**
     * Get the file path (within the ACS file store) by File JSON Object.
     *
     * @param fileJsonObject
     */
    public static String getFilePath(JsonObject fileJsonObject) {
        return getFilePath(
                fileJsonObject.getString(AcsConstants.FIELD_NAME_ORG_ID),
                AcsFileType.getAcsFileType(fileJsonObject.getString(AcsFile.FIELD_NAME_TYPE)),
                fileJsonObject.getString(AcsConstants.FIELD_NAME_NAME)
        );
    }

    /**
     * Get the full file path (within the ACS file store) by File JSON Object.
     *
     * @param fileJsonObject
     */
    public static String getFullFilePath(JsonObject fileJsonObject) {
        return AcsConfigProperties.ACS_FILE_STORE_PATH + "/"
                + getFilePath(
                fileJsonObject.getString(AcsConstants.FIELD_NAME_ORG_ID),
                AcsFileType.getAcsFileType(fileJsonObject.getString(AcsFile.FIELD_NAME_TYPE)),
                fileJsonObject.getString(AcsConstants.FIELD_NAME_NAME)
        );
    }

    /**
     * Get the file path in the external file server ("/[file type]/[file name]"
     *
     * Rules:
     *
     *  http://[hostname]:[port]/images/filename
     *  http://[hostname]:[port]/golden-config-files/filename
     *  http://[hostname]:[port]/config-file-backups/[FSAN SN]/daily
     *  http://[hostname]:[port]/config-file-backups/[FSAN SN]/[timestamp]
     *
     * @param fileJsonObject
     */
    public static String getExternalFilePath(JsonObject fileJsonObject) {
        String folder = "/";
        String filename = fileJsonObject.getString(AcsConstants.FIELD_NAME_NAME);

        switch (AcsFileType.getAcsFileType(fileJsonObject.getString(AcsFile.FIELD_NAME_TYPE))) {
            case Image:
                folder = "/images/";
                break;

            case ConfigFile:
                if (!fileJsonObject.containsKey(AcsConstants.FIELD_NAME_CPE_ID)) {
                    folder = "/golden-config-files/";
                } else {
                    folder = "/config-file-backups/";
                    // Replace the "-" in filename to "/"
                    filename = filename.replaceFirst("-", "/");
                }
                break;
            case SipConfigFile:
                folder = "/sip-config-file-backups/";
                // Replace the "-" in filename to "/"
                filename = filename.replaceFirst("-", "/");
                break;
        }

        //return  folder + filename;

        /**
         * For now, since we are allowing separate servers for images and config files, the folder shall be
         * built into the base URL of the external image/config-file servers.
         */
        return filename;
    }

    /**
     * Get the file path (within the ACS file store) by Request Body.
     *
     * @param orgId
     * @param type
     */
    public static String getFileParentPath(String orgId, AcsFileType type) {
        return orgId + "/" + type.name();
    }

    /**
     * Get the file parent path (within the ACS file store) by Request Body.
     *
     * @param reqBody
     */
    public static String getFileParentPath(JsonObject reqBody) {
        return getFileParentPath(
                reqBody.getString(AcsConstants.FIELD_NAME_ORG_ID),
                AcsFileType.getAcsFileType(reqBody.getString(AcsFile.FIELD_NAME_TYPE))
        );
    }

    /**
     * Get the MongoDB Matcher by File Path.
     *
     * @param path
     *
     * @return  The matcher.
     */
    public static JsonObject getMatcherByFilePath(String path) {
        String[] segments = path.split("/");
        if (segments.length != 4) {
            log.error("Invalid file path! " + path);
            return null;
        }

        return new JsonObject()
                .put(AcsConstants.FIELD_NAME_ORG_ID, segments[1])
                .put(FIELD_NAME_TYPE, AcsFileType.getAcsFileTypeString(segments[2]))
                .put(AcsConstants.FIELD_NAME_NAME, segments[3]);
    }

    /**
     * Auto generate a file name (for config or log files).
     *
     * @param cpeSn
     */
    public static String autoGenerateFileName(String cpeSn, AcsFileType type) {
        switch (type) {
            case LogFile:
                return  cpeSn + "-logs-" + System.currentTimeMillis();
            case ConfigFile:
            default:
                return cpeSn + "-" + System.currentTimeMillis();
        }
    }

    /**
     * Get the upload URL Suffix.
     *
     * @param id
     */
    public static String getUploadUrl(String id) {
        return AcsConfigProperties.BASE_FILE_SERVER_URL + id;
    }

    /**
     * Get the download URL.
     *
     * @param id
     */
    public static String getDownloadUrl(String id) {
        return AcsConfigProperties.BASE_FILE_SERVER_URL + id;
    }

    /**
     * Cleanup in-complete upload files from DB by CPE SN.
     *
     * @param mongoClient
     * @param cpeSn
     */
    public static void cleanupIncompleteUploads(MongoClient mongoClient, String cpeSn) {
        log.info(cpeSn + ": Cleaning up incomplete upload file(s)...");
        try {
            VertxMongoUtils.deleteWithMatcher(
                    mongoClient,
                    AcsFile.DB_COLLECTION_NAME,
                    new JsonObject()
                        .put(AcsConstants.FIELD_NAME_CPE_ID + "." + Cpe.DB_FIELD_NAME_SN, cpeSn)
                        .put(AcsFile.FIELD_NAME_SIZE, 0),
                    null
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the download URL with a custom URL base.
     *
     * @param customHostname
     * @param id
     */
    public static String getDownloadUrl(String customHostname, String id) {
        return "http://" + customHostname + ":"
                + AcsConfigProperties.FILE_SERVER_PORT
                + AcsConfigProperties.FILE_SERVER_URL_ROOT
                + id;
    }

    /**
     * Get the username for download/upload.
     *
     * If the "username" was not provided during creation (for external URL), will generate one using these fields:
     *  * name
     *  * orgId
     *
     * @param fileStruct
     * @return
     */
    public static String getUsername(JsonObject fileStruct) {
        return fileStruct.getString(
                FIELD_NAME_USERNAME,
                // Auto-generate MD5 Hash if username was not specified
                AcsMiscUtils.getMD5Hash(
                        fileStruct.getString(AcsConstants.FIELD_NAME_ID, ObjectId.get().toString()).toUpperCase()
                )
        );
    }

    /**
     * Get the password for download/upload.
     *
     * If the "password" was not provided during creation (for external URL), will generate one using these fields:
     *  * name
     *  * orgId
     *
     * @param fileStruct
     * @return
     */
    public static String getPassword(JsonObject fileStruct) {
        return fileStruct.getString(
                FIELD_NAME_PASSWORD,
                // Auto-generate MD5 Hash if password was not specified
                AcsMiscUtils.getMD5Hash(
                        fileStruct.getString(AcsConstants.FIELD_NAME_ID, ObjectId.get().toString()).toLowerCase()
                )
        );
    }

    /**
     * Check if the given file struct represents an auto backup config file.
     *
     * @param fileStruct
     * @return true if yes; or false if not.
     */
    public static boolean isAutoBackup(JsonObject fileStruct) {
        if (!fileStruct.containsKey(AcsConstants.FIELD_NAME_CPE_ID)) {
            return false;
        }

        return AUTO_BACKUP_CONFIG_FILE_NAME.equals(fileStruct.getString(AcsConstants.FIELD_NAME_NAME));
    }

    /**
     * Return an internal file id string for a given CPE id/key.
     * @param cpeKey
     * @return
     */
    public static String getAutoBackupConfigFileId(String cpeKey) {
        return AUTO_BACKUP_CONFIG_FILE_ID_PREFIX + cpeKey;
    }

    /**
     * Build an Auto Backup File Struct by CPE Key/_id.
     * @param cpeKey
     * @return
     */
    public static JsonObject buildAutoBackupFileRecord(String cpeKey) {
        return new JsonObject()
                .put(AcsConstants.FIELD_NAME_ID, getAutoBackupConfigFileId(cpeKey))
                .put(AcsConstants.FIELD_NAME_ORG_ID, Cpe.getOrgIdByCpeKey(cpeKey))
                .put(FIELD_NAME_TYPE, AcsFileType.ConfigFile.typeString)
                .put(FIELD_NAME_CSR_USERNAME, "n/a (System Performed Auto Backup)")
                .put(FIELD_NAME_USERNAME, cpeKey)
                .put(FIELD_NAME_PASSWORD, AUTO_BACKUP_CONFIG_FILE_PASSWORD)
                .put(AcsConstants.FIELD_NAME_NAME, AUTO_BACKUP_CONFIG_FILE_NAME)
                .put(
                        AcsConstants.FIELD_NAME_CPE_ID,
                        new JsonObject()
                                .put(CpeIdentifier.FIELD_NAME_OUI, Cpe.getOuiByCpeKey(cpeKey))
                                .put(CpeIdentifier.FIELD_NAME_SN, Cpe.getSnByCpeKey(cpeKey))
                );
    }


    /**
     * Build an Auto Backup File Struct by CPE Key/_id with Upload URL.
     * @param cpeKey
     * @return
     */
    public static JsonObject buildAutoBackupFileRecordWithUploadURL(String cpeKey) {
        return buildAutoBackupFileRecord(cpeKey)
                .put(
                        FIELD_NAME_UPLOAD_URL,
                        getDownloadUrl(AUTO_BACKUP_CONFIG_FILE_ID_PREFIX + cpeKey)
                );
    }
}
