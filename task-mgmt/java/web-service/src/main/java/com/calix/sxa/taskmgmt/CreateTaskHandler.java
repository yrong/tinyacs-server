package com.calix.sxa.taskmgmt;

import com.calix.sxa.VertxUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA Task Mgmt
 *
 * @author: jqin
 */
public class CreateTaskHandler implements Handler<Buffer> {
    private static final Logger log = LoggerFactory.getLogger(CreateTaskHandler.class.getName());

    /**
     * Original HTTP Request
     */
    public HttpServerRequest request;

    /**
     * Constructor that requires an HttpServerRequest.
     *
     * @param request
     */
    public CreateTaskHandler(HttpServerRequest request) {
        this.request = request;
    }

    /**
     * The actual handler body.
     *
     * @param buffer
     */
    @Override
    public void handle(Buffer buffer) {
        /**
         * The entire body has now been received
         */
        JsonObject rawTask = null;
        try {
            rawTask = new JsonObject(buffer.toString());
        } catch (Exception ex) {
            badRequest("Invalid JSON Payload! " + ex.getMessage());
            return;
        }

        String error = null;
        JsonObject jesqueTask = null;
        String taskQueue = rawTask.getString("queue");
        try {
            /**
             * The payload is supposed to be an Sxa Task
             */
            jesqueTask = TaskUtils.convertRawTaskToJesqueTask(rawTask);
        } catch (Exception ex) {
            badRequest("Illegal JSON payload:\n" + rawTask.toString());
            return;
        }

        /**
         * Enqueue the Jesque task
         */
        TaskUtils.enqueueTask(
                taskQueue,
                jesqueTask,
                TaskMgmtRestWsVertice.redisClient,
                new asyncEnqueueHandler(request, jesqueTask.getString("_id"))
        );
    }

    /**
     * Send "BAD_REQUEST" Status back to client with an error string.
     *
     * @param error
     */
    public void badRequest(String error) {
        log.error(error);
        VertxUtils.setResponseStatus(request, HttpResponseStatus.BAD_REQUEST);
        VertxUtils.writeToResponse(request, error + "\n");
    }

    /**
     * Async Handler for the async result from mod redis
     */
    private class asyncEnqueueHandler implements Handler<Message<JsonObject>> {
        /**
         * Original HTTP Request
         */
        public  HttpServerRequest request;

        /**
         * TaskId
         */
        public String taskId = null;

        /**
         * Constructor that requires an HttpServerRequest.
         *
         * @param request
         */
        public asyncEnqueueHandler(HttpServerRequest request, String taskId) {
            this.request = request;
            this.taskId = taskId;
        }

        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            JsonObject jsonObject = jsonObjectMessage.body();
            String error = TaskMgmtWsUtils.checkAsyncResult(request, jsonObject);
            if (error != null) {
                log.error(error);
                return;
            }

            log.info("Successfully added a new task (id: " + taskId + " into redis.");
            VertxUtils.writeToResponse(request, taskId);
        }
    }
}
