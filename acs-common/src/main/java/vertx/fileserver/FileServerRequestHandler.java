package vertx.fileserver;

import io.vertx.ext.mongo.MongoClient;
import vertx.*;
import vertx.cache.OrganizationCache;
import vertx.model.AcsFile;
import vertx.model.AcsFileType;
import vertx.model.Cpe;
import vertx.model.Organization;
import vertx.util.AcsConfigProperties;
import vertx.util.AcsConstants;
import vertx.util.AcsMiscUtils;
import vertx.util.GigaCenter;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.auth.AUTH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;

/**
 * Project:  cwmp
 *
 * File Server Request Handler for both Upload and Download.
 *
 * @author: ronyang
 */
public class FileServerRequestHandler implements Handler<HttpServerRequest> {
    private static final Logger log = LoggerFactory.getLogger(FileServerRequestHandler.class.getName());

    // Vert.x Instance (set by the constructor)
    public Vertx vertx;

    /**
     * Regular Organization Cache
     */
    public OrganizationCache organizationCache;

    public MongoClient mongoClient;

    /**
     * Constructor
     */
    public FileServerRequestHandler(Vertx vertx) {
        this.vertx = vertx;

        mongoClient = MongoClient.createShared(vertx, VertxMongoUtils.getModMongoPersistorConfig());

        /**
         * Initialize Organization Cache
         */
        organizationCache = new OrganizationCache(
                vertx,
                AcsConstants.VERTX_ADDRESS_ACS_ORGANIZATION_CRUD,
                Organization.DB_COLLECTION_NAME,
                Organization.class.getSimpleName()
        );
    }

    /**
     * Query Key that excludes file content
     */
    public static final JsonObject QUERY_KEY_NO_CONTENT = new JsonObject()
            .put(AcsFile.FIELD_NAME_TEXT_CONTENT, 0)
            .put(AcsFile.FIELD_NAME_BINARY_CONTENT, 0);

    /**
     * HTTP Method Strings
     */
    public static final String HTTP_METHOD_GET = HttpMethod.GET.toString();
    public static final String HTTP_METHOD_PUT = HttpMethod.PUT.toString();
    public static final String HTTP_METHOD_POST = HttpMethod.POST.toString();

    /**
     * Handler Body
     */
    @Override
    public void handle(final HttpServerRequest req) {
        /**
         * Extract file id string
         */
        final String id = req.path().substring(AcsConfigProperties.FILE_SERVER_URL_ROOT.length());
        if (id == null) {
            VertxUtils.badHttpRequest(req, "Invalid/Malformed File Server URL!");
            return;
        }

        final boolean bIsDownload = req.method().equals(HTTP_METHOD_GET);
        if (id.startsWith(AcsFile.AUTO_BACKUP_CONFIG_FILE_ID_PREFIX)) {
            if (bIsDownload == false) {
                /**
                 * Upload Auto Backup Config File
                 */
                String cpeKey = id.substring(AcsFile.AUTO_BACKUP_CONFIG_FILE_ID_PREFIX.length());
                if (!Cpe.isValidCpeKey(cpeKey)) {
                    VertxUtils.badHttpRequest(req, "Invalid Upload File Server URL!");
                    return;
                }
                serveRequestWithFileRecord(
                        AcsFile.buildAutoBackupFileRecord(cpeKey),
                        req,
                        id,
                        null,
                        bIsDownload
                );
                return;
            }
        } else if(!VertxMongoUtils.isUuid(id) && !VertxMongoUtils.isObjectId(id)) {
            VertxUtils.badHttpRequest(req, "Invalid File Server URL!");
            return;
        }

        final JsonObject matcher = new JsonObject().put(AcsConstants.FIELD_NAME_ID, id);

        /**
         * Pause for now until the file is found
         */
        req.pause();

        /**
         * Try to find it in the "CWMP-files" collection
         */
        try {
            /**
             * MongoDB FindOne Handler
             */
            Handler findOneHandler = new Handler<JsonObject>() {
                @Override
                public void handle(final JsonObject aFileRecord) {
                    /**
                     * Resume the request
                     */
                    req.resume();

                    serveRequestWithFileRecord(aFileRecord, req, id, matcher, bIsDownload);
                }
            };

            /**
             * Find existing file record by "_id"
             */
            VertxMongoUtils.findOne(
                    mongoClient,
                    AcsFile.DB_COLLECTION_NAME,
                    matcher,
                    findOneHandler,
                    bIsDownload ? null : QUERY_KEY_NO_CONTENT
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Serve the HTTP Request after querying the file record.
     * @param aFileRecord
     * @param req
     * @param id
     * @param matcher
     * @param bIsDownload
     */
    public void serveRequestWithFileRecord(
            final JsonObject aFileRecord,
            final HttpServerRequest req,
            final String id,
            final JsonObject matcher,
            boolean bIsDownload) {
        if (aFileRecord == null) {
            VertxUtils.badHttpRequest(req, "Invalid File Id! (no such file)");
            return;
        }

        final String filename = aFileRecord.getString(AcsConstants.FIELD_NAME_NAME);
        final AcsFileType fileType = AcsFileType.getAcsFileType(aFileRecord.getString(AcsFile.FIELD_NAME_TYPE));

        /**
         * check Organization
         */
        final Organization organization = organizationCache
                .getOrgById(aFileRecord.getString(AcsConstants.FIELD_NAME_ORG_ID));
        if (organization == null) {
            VertxUtils.badHttpRequest(
                    req,
                    "Invalid orgId " + aFileRecord.getString(AcsConstants.FIELD_NAME_ORG_ID) + "!"
            );
            return;
        }

        /**
         * Verify Username/Password if any
         */
        if (aFileRecord.containsKey(AcsFile.FIELD_NAME_USERNAME)) {
            String authHeader = req.headers().get(AUTH.WWW_AUTH_RESP);
            if (authHeader != null) {
                log.debug("Received " + AUTH.WWW_AUTH_RESP + ": " + authHeader);

                String expectedAuthHeader = AcsMiscUtils.getHttpBasicAuthString(
                        aFileRecord.getString(AcsFile.FIELD_NAME_USERNAME),
                        aFileRecord.getString(AcsFile.FIELD_NAME_PASSWORD)
                );
                if (!authHeader.equals(expectedAuthHeader)) {
                    // Basic Auth Verification Failed
                    VertxUtils.responseWithStatusCode(HttpResponseStatus.UNAUTHORIZED, req);
                    return;
                }
            } else {
                // Send Auth Challenge
                req.response().putHeader(
                        AUTH.WWW_AUTH,
                        AcsConstants.HTTP_BASIC_AUTH_CHALLENGE
                );
                VertxUtils.responseWithStatusCode(HttpResponseStatus.UNAUTHORIZED, req);
                return;
            }
        }

        /**
         * Upload or Download?
         */
        if (bIsDownload) {
            /**
             * Download
             */
            int fileSize = aFileRecord.getInteger(AcsFile.FIELD_NAME_SIZE, 0);

            switch (fileType) {
                case Image:
                    if (fileSize == 0) {
                        endRequestWithError(req, filename, "Image has not been uploaded to this server yet!");
                        return;
                    } else {
                        log.debug(filename + ": Downloading from GridFS...");

                        /*todo*/
                    }
                    break;

                case LogFile:
                    if (fileSize == 0) {
                        endRequestWithError(req, filename, "Log File has not been uploaded to this server yet!");
                        return;
                    }

                    Buffer buffer = Buffer.buffer(aFileRecord.getBinary(AcsFile.FIELD_NAME_BINARY_CONTENT));

                    /**
                     * Add file name to header
                     */
                    // Extract FSAN
                    String sn = VertxJsonUtils.deepGet(
                            aFileRecord,
                            AcsConstants.FIELD_NAME_CPE_ID + "." + Cpe.DB_FIELD_NAME_SN
                    );

                    // Determine file type by checking first 10 bytes
                    boolean bIsTextFile = true;
                    for (int i = 0; i < 9; i ++) {
                        if (i >= fileSize) {
                            break;
                        }
                        if (!AcsMiscUtils.isPlainTextChar(buffer.getByte(i))) {
                            bIsTextFile = false;
                            break;
                        }
                    }
                    /*
                    String uploadTime = aFileRecord
                            .getObject(AcsFile.FIELD_NAME_UPLOAD_TIME)
                            .getString(VertxMongoUtils.MOD_MONGO_DATE);
                    */
                    String fileName = sn + "-logs." + (bIsTextFile? "txt" : "tar");

                    req.response().putHeader(
                            "Content-Disposition",
                            "attachment; filename=\"" + fileName +"\""
                    );

                    // Set Content Type accordingly
                    req.response().putHeader("Content-Type", bIsTextFile?"text/plain": "application/x-tar");

                    /**
                     * Serve the download request with embedded binary content
                     */
                    req.response().end(buffer);
                    break;

                case ConfigFile:
                case SipConfigFile:
                    if (aFileRecord.containsKey(AcsFile.FIELD_NAME_TEXT_CONTENT)) {
                        /**
                         * Serve the download request with embedded text content
                         */
                        req.response().end(aFileRecord.getString(AcsFile.FIELD_NAME_TEXT_CONTENT));
                    } else if (aFileRecord.containsKey(AcsFile.FIELD_NAME_BINARY_CONTENT)) {
                        /**
                         * Serve the download request with embedded binary content
                         */
                        req.response().end(Buffer.buffer(aFileRecord.getBinary(AcsFile.FIELD_NAME_BINARY_CONTENT)));
                    } else {
                        /**
                         * Serve the download request with a pump from a local file
                         *
                         * (for backward compatibility only)
                         */
                        VertxUtils.serveHttpFileDownloadRequest(
                                vertx,
                                req,
                                AcsConfigProperties.ACS_FILE_STORE_PATH + "/" + filename
                        );
                    }
                    break;

                default:
                    endRequestWithError(
                            req,
                            filename,
                            "Downloading " + fileType.typeString + " is prohibited!"
                    );
                    return;
            }

            /**
             * Increase download count for this file
             */
            JsonObject inc = new JsonObject()
                    .put(AcsFile.FIELD_NAME_NUMBER_OF_DOWNLOADS, 1);
            JsonObject update = new JsonObject().put("$inc", inc);
            try {
                VertxMongoUtils.updateWithMatcher(
                        mongoClient,
                        AcsFile.DB_COLLECTION_NAME,
                        matcher,
                        update,
                        null
                );
            } catch (VertxException e) {
                e.printStackTrace();
            }
        } else {
            /**
             * Upload. Define a body handler.
             */
            req.bodyHandler(new Handler<Buffer>() {
                @Override
                public void handle(final Buffer buffer) {
                    log.info(filename + ": Uploaded to memory buffer. Size: " + buffer.length());

                    /**
                     * Update File Size and Upload Time in "CWMP-files" collection
                     */
                    final JsonObject sets = VertxMongoUtils.addSet(null, AcsFile.FIELD_NAME_SIZE, buffer.length());
                    if (fileType.equals(AcsFileType.Image)) {
                        /**
                         * Check Image Metadata if needed
                         */
                        JsonObject metadata;
                        try {
                            metadata = GigaCenter.getImageMetadata(buffer);
                            VertxJsonUtils.merge(sets, metadata);
                        } catch (Exception ex) {
                            log.error(filename + ": " + ex.getMessage() + "!");
                        }
                    }
                    final JsonObject uploadTimeObj = VertxMongoUtils.addTimeStamp(
                            null, AcsFile.FIELD_NAME_UPLOAD_TIME);
                    final JsonObject dbUpdates = VertxMongoUtils.getUpdatesObject(
                            sets,
                            null,
                            uploadTimeObj,
                            null,
                            null
                    );

                    // Check File Type and do things differently
                    switch (fileType) {
                        case Image:
                            /**
                             * Does this organization have an external image server?
                             */
                            if (organization.extImageServer != null) {
                                /**
                                 * Response handler that saves the image after upload to external server
                                 */
                                Handler<HttpClientResponse> responseHandler = new Handler<HttpClientResponse>() {
                                    @Override
                                    public void handle(HttpClientResponse response) {
                                        if (HttpResponseStatus.OK.code() == response.statusCode() ||
                                                HttpResponseStatus.NO_CONTENT.code() == response.statusCode()) {
                                            log.info("Successfully uploaded " + filename + " to "
                                                    + organization.extImageServer.baseUrl + ".");

                                            /**
                                             * Dump the response body
                                             */
                                            response.bodyHandler(
                                                    new Handler<Buffer>() {
                                                        @Override
                                                        public void handle(Buffer bodyBuffer) {
                                                            log.debug(bodyBuffer.toString());
                                                        }
                                                    }
                                            );

                                            /**
                                             * Save Image into GridFS
                                             */
                                            saveToGridFS(
                                                    filename,
                                                    req,
                                                    aFileRecord,
                                                    buffer,
                                                    matcher,
                                                    dbUpdates
                                            );
                                        } else {
                                            String error = "Failed to upload " + filename + " to "
                                                    + organization.extImageServer.baseUrl
                                                    + ", status: " + response.statusCode() + " "
                                                    + response.statusMessage();
                                            endRequestWithError(req, filename, error);
                                        }
                                    }
                                };

                                /**
                                 * Upload
                                 */
                                uploadFileToExternalServer(
                                        filename,
                                        id,
                                        buffer,
                                        organization.extImageServer,
                                        req,
                                        responseHandler
                                );
                            } else {
                                /**
                                 * Save Image into GridFS
                                 */
                                saveToGridFS(
                                        filename,
                                        req,
                                        aFileRecord,
                                        buffer,
                                        matcher,
                                        dbUpdates
                                );
                            }
                            break;

                        case ConfigFile:
                        case SipConfigFile:
                        case LogFile:
                            /**
                             * Save the File Content into "CWMP-files" collection as embedded binary content.
                             */
                            if (matcher != null) {
                                updateFileRecord(
                                        req,
                                        filename,
                                        matcher,
                                        dbUpdates,
                                        VertxMongoUtils.getUpdatesObject(
                                                new JsonObject().put(
                                                        AcsFile.FIELD_NAME_BINARY_CONTENT, buffer.getBytes()
                                                ),
                                                null,
                                                uploadTimeObj,
                                                null,
                                                null
                                        )
                                );
                            } else {
                                /**
                                 * Uploading an Auto Backup Config File
                                 */
                                // Add file content/size and upload time
                                aFileRecord
                                        .put(AcsFile.FIELD_NAME_BINARY_CONTENT, buffer.getBytes())
                                        .put(AcsFile.FIELD_NAME_SIZE, buffer.length())
                                        .put(AcsFile.FIELD_NAME_UPLOAD_TIME, VertxMongoUtils.getDateObject());

                                // Save it (may overwrite existing record which is ok)
                                try {
                                    VertxMongoUtils.save(
                                            mongoClient,
                                            AcsFile.DB_COLLECTION_NAME,
                                            aFileRecord,
                                            new Handler<Message<JsonObject>>() {
                                                @Override
                                                public void handle(Message<JsonObject> saveResult) {
                                                    if (saveResult == null) {
                                                        endRequestWithError(
                                                                req,
                                                                filename,
                                                                "Internal DB Error!"
                                                        );
                                                    } else {
                                                        // Saved auto backup to DB successfully
                                                        req.response().end();
                                                    }
                                                }
                                            }
                                    );
                                } catch (VertxException e) {
                                    e.printStackTrace();
                                }
                            }
                            break;

                        default:
                            log.error("Unsupported File Type " + fileType + "!");
                            break;
                    }
                }
            });
        }

    }

    /**
     * Update the "CWMP-files" collection
     */
    public void updateFileRecord(
            final HttpServerRequest req,
            final String filename,
            final JsonObject matcher,
            final JsonObject update,
            final JsonObject binaryContent) {
        try {
            VertxMongoUtils.updateWithMatcher(
                    mongoClient,
                    AcsFile.DB_COLLECTION_NAME,
                    matcher,
                    update,
                    binaryContent,
                    VertxMongoUtils.DEFAULT_TIMEOUT,
                    new Handler<Long>() {
                        @Override
                        public void handle(Long result) {
                            if (result == null) {
                                endRequestWithError(
                                        req,
                                        filename,
                                        "Internal DB Error!"
                                );
                            } else {
                                req.response().end();
                            }
                        }
                    }
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }


    /**
     * End a HTTP Request due to internal error.
     *
     * @param req
     * @param filename
     * @param error
     */
    public void endRequestWithError(final HttpServerRequest req, String filename, String error) {
        log.error(filename + ": " + error);
        VertxUtils.setResponseStatus(req, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        req.response().end(new JsonObject().put(AcsConstants.FIELD_NAME_ERROR, error).encode());
    }

    /**
     * Check if the given HTTP Server Request is a File Server Request.
     *
     * @param req
     */
    public static boolean isFileServerRequest(HttpServerRequest req) {
        if (req == null || !req.path().startsWith(AcsConfigProperties.FILE_SERVER_URL_ROOT)) {
            return false;
        } else if (!req.method().equals(HTTP_METHOD_GET)
                && !req.method().equals(HTTP_METHOD_PUT)
                && !req.method().equals(HTTP_METHOD_POST)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Upload a file to an external file server.
     *
     * @param buffer
     * @param extServer
     * @param httpServerRequest
     */
    public void uploadFileToExternalServer(
            final String filename,
            final String internalFileId,
            final Buffer buffer,
            final Organization.ExternalFileServer extServer,
            final HttpServerRequest httpServerRequest,
            Handler<HttpClientResponse> responseHandler) {
        // Check for null pointers
        if (extServer == null || buffer == null || httpServerRequest == null) {
            return;
        }

        // Get URL POJO Instance
        URL url;
        HttpClient httpClient;
        try {
            url = new URL(extServer.baseUrl);
            httpClient = VertxHttpClientUtils.createHttpClient(vertx,url.getHost(),url.getPort());
        } catch (MalformedURLException e) {
            // This should never happen though
            log.error("Invalid External File Server URL " + extServer.baseUrl + "!");
            return;
        }

        // Build HTTP Client Request Instance
        final HttpClientRequest clientRequest = httpClient.request(
                io.vertx.core.http.HttpMethod.POST,
                url.getPath(),
                responseHandler
        );
        clientRequest.exceptionHandler(
                new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable exception) {
                        log.error("Caught " + exception.getClass().getSimpleName()
                                + " while uploading " + filename + " to " + extServer.baseUrl
                                + "! Exception details: " + exception.getMessage());
                        endRequestWithError(
                                httpServerRequest,
                                filename,
                                "Failed to upload to " + extServer.baseUrl
                                + " due to " + exception.getClass().getSimpleName()
                        );
                    }
                }
        );
        // Timeout in 1 min
        clientRequest.setTimeout(60000);

        // Basic Auth Header
        if (extServer.username != null) {
            String credentials = extServer.username + ":" + extServer.password;
            clientRequest.headers().set(
                    AUTH.WWW_AUTH_RESP,
                    "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes())
            );
        }

        // Payload/Content
        final String boundary = "----------------------------" + internalFileId;
        final String payloadHeader = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"fileupload\"; filename=\"" + filename + "\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\n";

        int totalLength = payloadHeader.length() + buffer.length() + boundary.length() + 8;

        clientRequest.headers().set("Accept", "*/*");
        clientRequest.headers().set("User-Agent", "Calix Compass - Consumer Connect");
        clientRequest.headers().set("Content-Length", String.valueOf(totalLength));
        clientRequest.headers().set("Content-Type", "multipart/form-data; boundary=" + boundary);

        /*
        clientRequest.write(payloadHeader);
        clientRequest.write(buffer);
        clientRequest.write("\r\n--" + boundary + "--\r\n");
        clientRequest.end();
        */

        clientRequest.headers().set("Expect", "100-continue");

        // Start a timer to continue if no "100-continue" is received within 1 second
        final long timerId = vertx.setTimer(
                1000,
                new Handler<Long>() {
                    @Override
                    public void handle(Long event) {
                        log.debug(filename + "Timed out waiting for 100-continue");
                        clientRequest.continueHandler(null);
                        clientRequest.write(payloadHeader);
                        clientRequest.write(buffer);
                        clientRequest.write("\r\n--" + boundary + "--\r\n");
                        clientRequest.end();
                    }
                }
        );
        clientRequest.continueHandler(
                new Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        vertx.cancelTimer(timerId);

                        log.debug(filename + ": received 100-continue, sending payload to " + extServer.baseUrl);
                        clientRequest.write(payloadHeader);
                        clientRequest.write(buffer);
                        clientRequest.write("\r\n--" + boundary + "--\r\n");
                        clientRequest.end();
                    }
                }
        );

        // Send headers and wait for "100-continue" before sending the payload
        clientRequest.sendHead();
    }

    /**
     * Save image into GridFS.
     *
     * @param filename
     * @param req
     * @param aFileRecord
     * @param buffer
     * @param matcher
     * @param dbUpdate
     */
    public void saveToGridFS(
            final String filename,
            final HttpServerRequest req,
            final JsonObject aFileRecord,
            final Buffer buffer,
            final JsonObject matcher,
            final JsonObject dbUpdate) {
        /**
         * Save Image into GridFS
         */
        /*todo*/
    }
}
