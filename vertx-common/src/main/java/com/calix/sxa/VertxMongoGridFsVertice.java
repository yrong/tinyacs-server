package com.calix.sxa;

import com.englishtown.vertx.GridFSModule;
import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.bson.types.ObjectId;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.List;

/**
 * Project:  SXA-CC
 *
 * Extending the existing vertx-mod-mongo-gridfs (from EnglishTown) to add delete function.
 *
 * @author: jqin
 */
public class VertxMongoGridFsVertice extends GridFSModule {
    /**
     * Action Types
     */
    public static final String ACTION_GET_FILE = "getFile";
    public static final String ACTION_GET_CHUNK = "getChunk";
    public static final String ACTION_SAVE_FILE = "saveFile";
    public static final String ACTION_DELETE_FILE = "deleteFile";

    /**
     * GridFS Bucket Used by SXACC Files
     */
    public static final String SXACC_BUCKET = "sxacc_fs";

    /**
     * Field Names
     */
    public static final String FIELD_NAME_ACTION = "action";
    public static final String FIELD_NAME_LENGTH = "length";
    public static final String FIELD_NAME_CHUNK_SIZE = "chunkSize";
    public static final String FIELD_NAME_UPLOAD_DATE = "uploadDate";
    public static final String FIELD_NAME_METADATA = "metadata";
    public static final String FIELD_NAME_FILES_ID = "files_id";
    public static final String FIELD_NAME_N = "n";
    public static final String FIELD_NAME_REPLY = "reply";
    /**
     * Status
     */
    public static final String FIELD_NAME_STATUS = "status";
    public static final String FIELD_NAME_STATUS_VALUE_OK = "ok";
    public static final String FIELD_NAME_STATUS_VALUE_FAILED = "failed";
    public static final String FIELD_NAME_ERROR = "error";
    public static final String FIELD_NAME_ID = "id";
    public static final String FIELD_NAME_BUCKET = "bucket";
    public static final String FIELD_NAME_FILENAME = "filename";
    public static final String FIELD_NAME_CONTENT_TYPE = "contentType";

    /**
     * MongoDB GridFS Instance
     */
    GridFS gridFS;

    /**
     * Frequently Used Status Reply
     */
    public static final JsonObject SUCCEEDED = new JsonObject().putString(FIELD_NAME_STATUS, FIELD_NAME_STATUS_VALUE_OK);

    /**
     * Start
     */
    @Override
    public void start() {
        try {
            /**
             * Call Super
             */
            super.start();

            /**
             * Initialize the Mongo GridFS Instance with custom bucket
             */
            gridFS = new GridFS(db, SXACC_BUCKET);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }
        logger.info("Started a " + this.getClass().getSimpleName() + " instance.");
    }

    /**
     * Main Handler
     */
    @Override
    public void handle(Message<JsonObject> message) {

        JsonObject jsonObject = message.body();
        String action = jsonObject.getString("action");
        if (action == null) {
            sendError(message, "\"action\" must be specified!");
        }

        try {
            switch (action) {
                case ACTION_GET_FILE:
                    getFile(message, jsonObject);
                    break;
                case ACTION_GET_CHUNK:
                    getChunk(message, jsonObject);
                    break;
                case ACTION_SAVE_FILE:
                    saveFile(message, jsonObject);
                    break;
                case ACTION_DELETE_FILE:
                    deleteFile(message, jsonObject);
                    break;
                default:
                    sendError(message, "action " + action + " is not supported");
            }

        } catch (Throwable e) {
            sendError(message, "Unexpected error in " + action + ": " + e.getMessage(), e);
        }
    }

    /**
     * Delete File
     */
    public void deleteFile(Message<JsonObject> message, JsonObject jsonObject) {
        /**
         * Get Object Id
         */
        String idString = jsonObject.getString("id");
        if (idString != null) {
            ObjectId objectId;
            try {
                objectId = new ObjectId(idString);
                gridFS.remove(objectId);
                sendOK(message, SUCCEEDED);
            } catch (Exception ex) {
                sendError(message, idString + " is not a valid ObjectId! Exception: ", ex);
            }
            return;
        }

        /**
         * Does it have a custom matcher on metadata?
         */
        JsonObject matcher = jsonObject.getObject(FIELD_NAME_METADATA);
        if (matcher != null) {
            logger.info("Deleting with matcher " + matcher + "...");
            BasicDBObject query = new BasicDBObject(matcher.toMap());
            List<GridFSDBFile> queryResults = gridFS.find(query);
            for ( GridFSDBFile f : gridFS.find(query) ){
                gridFS.remove((ObjectId)(f.getId()));
            }
            logger.info("Deleted " + queryResults.size() + " file(s).");
            sendOK(message, SUCCEEDED.copy().putNumber(VertxMongoUtils.MOD_MONGO_FIELD_NAME_NUMBER, queryResults.size()));
        } else {
            sendError(message, "No matcher found!");
        }
    }
}
