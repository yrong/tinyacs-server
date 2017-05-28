package vertx;

import com.mongodb.gridfs.GridFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * Vert.x MongoDB GridFS File.
 *
 * External References:
 *  http://docs.mongodb.org/manual/reference/gridfs/
 *  https://github.com/englishtown/vertx-mod-mongo-gridfs
 *
 * @author: ronyang
 */
public class VertxMongoGridFsFile {
    private static final Logger log = LoggerFactory.getLogger(VertxMongoGridFsFile.class.getName());

    /**
     * Build vertx-mod-mongo-gridfs config JSON object.
     */
    public static JsonObject getModMongoGridFsConfig() {
        // Same as mod-mongo-persistor config JSON object except the event bus address.
        return VertxMongoUtils.getModMongoPersistorConfig()
                .putString("address", VertxConstants.VERTX_ADDRESS_MONGODB_GRID_FS);
    }

    /**
     * Get the # of instances of mod-mongo-persistor
     */
    public static final int MAX_INSTANCES = 16;
    public static int getNumberOfInstances() {
        if ((VertxUtils.getNumberOfCpuCores()) > MAX_INSTANCES) {
            return MAX_INSTANCES;
        } else {
            return VertxUtils.getNumberOfCpuCores();
        }
    }

    /**
     * Default Chunk Size 255KB
     */
    public static final int DEFAULT_CHUNK_SIZE = GridFS.DEFAULT_CHUNKSIZE;

    /**
     * Default Timeout is 60 seconds
     */
    public static final long DEFAULT_TIMEOUT = 60000;
    public static final JsonObject TIMED_OUT = new JsonObject()
            .putString(VertxMongoGridFsVertice.FIELD_NAME_STATUS, VertxMongoGridFsVertice.FIELD_NAME_STATUS_VALUE_FAILED)
            .putString(VertxMongoGridFsVertice.FIELD_NAME_ERROR, "Internal DB Timeout!");

    /**
     * POJO Attributes
     */
    public EventBus eventBus;
    public String fileId;
    public String fileName;
    public int fileSize;
    public JsonObject metadata;
    // Chunk Index (start with 0)
    public int chunkNumber = 0;
    public int saveBufferOffset = 0;

    /**
     * Handlers/Objects needed for saving the file:
     */
    public Handler<JsonObject> saveHandler = null;
    // Buffer of the entire file
    public Buffer saveBuffer = null;
    // Create Save-Chunk-Result Handler
    public Handler<AsyncResult<Message<JsonObject>>> saveChunkResultHandler = null;
    public Handler<AsyncResult<Message<byte[]>>> getChunkResultHandler = null;

    /**
     * Constructor.
     *
     * @param eventBus
     * @param fileId
     * @param fileName
     * @param fileSize
     */
    public VertxMongoGridFsFile(
            EventBus eventBus,
            String fileId,
            String fileName,
            JsonObject metadata,
            int fileSize) {
        this.eventBus = eventBus;
        this.fileId = fileId;
        this.fileName = fileName;
        this.metadata = metadata;
        this.fileSize = fileSize;
    }

    /**
     * Save the next chunk to GridFS.
     */
    public void saveNextChunk() {
        /**
         * Save-Chunk Message Header
         */
        byte[] chunkMessageHeader = new JsonObject()
                .putString(VertxMongoGridFsVertice.FIELD_NAME_FILES_ID, fileId)
                .putString(VertxMongoGridFsVertice.FIELD_NAME_BUCKET, VertxMongoGridFsVertice.CWMP_BUCKET)
                .putNumber(VertxMongoGridFsVertice.FIELD_NAME_N, chunkNumber)
                .encode()
                .getBytes();

        // Create a new Buffer
        Buffer buffer = new Buffer();
        // Append the header
        buffer.appendInt(chunkMessageHeader.length);
        buffer.appendBytes(chunkMessageHeader);


        int chunkSize = saveBuffer.length() - saveBufferOffset;
        if (chunkSize > DEFAULT_CHUNK_SIZE) {
            chunkSize = DEFAULT_CHUNK_SIZE;
        }
        if (chunkSize > 0) {
            chunkNumber ++;
            if ((chunkNumber % 32) == 0 || chunkSize < DEFAULT_CHUNK_SIZE) {
                log.debug(fileName + ": Saving chunk # " + chunkNumber
                        + " (size " + chunkSize + ", offset: " + saveBufferOffset + ") ...");
            }

            // Send it over the event bus
            buffer.appendBytes(saveBuffer.getBytes(saveBufferOffset, saveBufferOffset + chunkSize));
            saveBufferOffset += chunkSize;
            eventBus.sendWithTimeout(
                    VertxConstants.VERTX_ADDRESS_MONGODB_GRID_FS_SAVE_CHUNK,
                    buffer,
                    DEFAULT_TIMEOUT,
                    saveChunkResultHandler
            );
        } else {
            saveHandler.handle(VertxMongoGridFsVertice.SUCCEEDED);
        }
    }

    /**
     * Save the file info (into the "files" collection) and start saving the chunks if succeeded.
     */
    public void saveFileInfo() {
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB_GRID_FS,
                new JsonObject()
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_ACTION, VertxMongoGridFsVertice.ACTION_SAVE_FILE)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_BUCKET, VertxMongoGridFsVertice.CWMP_BUCKET)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_ID, fileId)
                        .putObject(VertxMongoGridFsVertice.FIELD_NAME_METADATA, metadata)
                        .putNumber(VertxMongoGridFsVertice.FIELD_NAME_LENGTH, fileSize)
                        .putNumber(VertxMongoGridFsVertice.FIELD_NAME_CHUNK_SIZE, DEFAULT_CHUNK_SIZE)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_FILENAME, fileName),
                DEFAULT_TIMEOUT,
                new Handler<AsyncResult<Message<JsonObject>>>() {
                    @Override
                    public void handle(AsyncResult<Message<JsonObject>> event) {
                        log.info(fileName + ": Saved File Info into the files collection.");

                        /**
                         * Step 2: Read and Save the first the chunk
                         */
                        saveNextChunk();
                    }
                }
        );
    }

    /**
     * Save a file (which is already in the local file system) into GridFS.
     *
     * @param buffer
     * @param reUpload  If true, remove this file from the GridFS first
     * @param handler
     */
    public void saveFile(boolean reUpload, final Buffer buffer, final Handler<JsonObject> handler) {
        saveHandler = handler;
        saveBuffer = buffer;
        saveBufferOffset = 0;
        chunkNumber = 0;

        /**
         * Create Save-Chunk-Result Handler
         */
        saveChunkResultHandler = new Handler<AsyncResult<Message<JsonObject>>>() {
                @Override
                public void handle(AsyncResult<Message<JsonObject>> saveChunkResult) {
                    if (saveChunkResult.failed()) {
                        // Failed
                        log.error(fileName + ": Failed to receive result from mod-mongodb-gridfs! Cause: "
                                + saveChunkResult.cause());
                        handler.handle(
                                new JsonObject()
                                        .putString(VertxMongoGridFsVertice.FIELD_NAME_STATUS, VertxMongoGridFsVertice.FIELD_NAME_STATUS_VALUE_FAILED)
                                        .putString(VertxMongoGridFsVertice.FIELD_NAME_ERROR, saveChunkResult.cause().getMessage())
                        );
                    } else {
                        // Received reply from mod-mongodb-gridfs
                        JsonObject status = saveChunkResult.result().body();
                        if (status == null ||
                                !VertxMongoGridFsVertice.FIELD_NAME_STATUS_VALUE_OK.equals(status.getString(VertxMongoGridFsVertice.FIELD_NAME_STATUS))) {
                            String error = "null";
                            if (status != null) {
                                error = status.getString(VertxMongoGridFsVertice.FIELD_NAME_STATUS);
                            }
                            log.error(fileName + ": Received Save-Chunk Status: " + error);
                            handler.handle(
                                    new JsonObject()
                                            .putString(VertxMongoGridFsVertice.FIELD_NAME_STATUS, VertxMongoGridFsVertice.FIELD_NAME_STATUS_VALUE_FAILED)
                                            .putString(VertxMongoGridFsVertice.FIELD_NAME_ERROR, error)
                            );
                        } else {
                            // Succeeded. Move on to next chunk if any
                            //log.debug(fileName + ": Saved " + chunkNumber + " chunk(s).");

                            if (saveBufferOffset >= fileSize) {
                                log.info(fileName + ": No more chunks");
                                handler.handle(VertxMongoGridFsVertice.SUCCEEDED);
                            } else {
                                saveNextChunk();
                            }
                        }
                    }
                }
        };

        /**
         * Do we need to delete first?
         */
        if (reUpload) {
            deleteFile(
                    eventBus,
                    fileId,
                    new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> deleteResult) {
                            if (deleteResult.failed()) {
                                log.error(fileName + ": Failed to Delete due to " + deleteResult.cause() + "!");
                                handler.handle(
                                        new JsonObject()
                                                .putString(
                                                        VertxMongoGridFsVertice.FIELD_NAME_STATUS,
                                                        VertxMongoGridFsVertice.FIELD_NAME_STATUS_VALUE_FAILED
                                                )
                                                .putString(
                                                        VertxMongoGridFsVertice.FIELD_NAME_ERROR,
                                                        deleteResult.cause().getMessage()
                                                )
                                );
                            } else {
                                log.info(fileName + ": Deleted existing file info and chunks (if any)");
                                /**
                                 * Save File Info (into the "files" collection)
                                 */
                                saveFileInfo();
                            }
                        }
                    }
            );
        } else {
            /**
             * Save File Info (into the "files" collection)
             */
            saveFileInfo();
        }
    }

    /**
     * Get File Info
     *
     * @param eventBus
     * @param id
     * @param handler
     */
    public static void getFileInfo(
            EventBus eventBus,
            String id,
            Handler<AsyncResult<Message<JsonObject>>> handler) {
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB_GRID_FS,
                new JsonObject()
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_ACTION, VertxMongoGridFsVertice.ACTION_GET_FILE)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_BUCKET, VertxMongoGridFsVertice.CWMP_BUCKET)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_ID, id),
                DEFAULT_TIMEOUT,
                handler
        );
    }

    /**
     * Get a file chunk.
     */
    public void getChunk() {
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB_GRID_FS,
                new JsonObject()
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_ACTION, VertxMongoGridFsVertice.ACTION_GET_CHUNK)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_BUCKET, VertxMongoGridFsVertice.CWMP_BUCKET)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_FILES_ID, fileId)
                        .putNumber(VertxMongoGridFsVertice.FIELD_NAME_N, chunkNumber),
                DEFAULT_TIMEOUT,
                getChunkResultHandler
        );
    }

    /**
     * Serve an HTTP Download Request for a given File Id.
     * @param eventBus
     * @param fileName
     * @param id
     * @param request
     */
    public static void serveHttpDownRequest(
            EventBus eventBus,
            final String fileName,
            final String id,
            final int fileSize,
            final HttpServerRequest request) {
        final VertxMongoGridFsFile file = new VertxMongoGridFsFile(eventBus, id, fileName, null, fileSize);

        final long startTime = System.currentTimeMillis();

        /**
         * Enable Chunk Mode
         */
        request.response().setChunked(true);
        // Set Content Type
        request.response().putHeader("Content-Type", "application/octet-stream");
        /**
         * Set Content-Length
         */
        //request.response().putHeader("Content-Length", String.valueOf(fileSize));

        /**
         * Get-Chunk Result Handler
         */
        file.getChunkResultHandler = new Handler<AsyncResult<Message<byte[]>>>() {
            @Override
            public void handle(AsyncResult<Message<byte[]>> getChunkResult) {
                byte[] chunk = getChunkResult.result().body();
                boolean bEnd = false;

                if (getChunkResult.failed() || chunk == null) {
                    log.error(fileName + ": Unable to receive chunk # " + file.chunkNumber + "!");
                    bEnd = true;
                } else if (chunk.length == 0) {
                    // Empty/Last Chunk
                    bEnd = true;
                } else {
                    request.response().write(new Buffer(chunk));
                    if (chunk.length < DEFAULT_CHUNK_SIZE) {
                        // Empty/Last Chunk
                        bEnd = true;
                    } else {
                        // Get the next Chunk
                        file.chunkNumber ++;
                        file.getChunk();
                    }
                }

                if (bEnd == true) {
                    log.info(fileName + ": Download completed. Served "
                            + (file.chunkNumber * DEFAULT_CHUNK_SIZE + (chunk == null? 0 : chunk.length))
                            + " bytes in " + (System.currentTimeMillis() - startTime) + " milliseconds.");
                    request.response().end();
                }
            }
        };

        /**
         * Start with the first chunk
         */
        file.getChunk();
    }

    /**
     * Delete File Info and all Chunks
     */
    public static void deleteFile(
            EventBus eventBus,
            String id,
            Handler<AsyncResult<Message<JsonObject>>> handler) {
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB_GRID_FS,
                new JsonObject()
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_ACTION, VertxMongoGridFsVertice.ACTION_DELETE_FILE)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_BUCKET, VertxMongoGridFsVertice.CWMP_BUCKET)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_ID, id),
                DEFAULT_TIMEOUT,
                handler
        );
    }

    /**
     * Delete all files by orgId
     */
    public static void deleteFilesWithMetadata(
            EventBus eventBus,
            JsonObject metadata,
            Handler<AsyncResult<Message<JsonObject>>> handler) {
        eventBus.sendWithTimeout(
                VertxConstants.VERTX_ADDRESS_MONGODB_GRID_FS,
                new JsonObject()
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_ACTION, VertxMongoGridFsVertice.ACTION_DELETE_FILE)
                        .putString(VertxMongoGridFsVertice.FIELD_NAME_BUCKET, VertxMongoGridFsVertice.CWMP_BUCKET)
                        .putObject(VertxMongoGridFsVertice.FIELD_NAME_METADATA, metadata),
                DEFAULT_TIMEOUT,
                handler
        );
    }
}
