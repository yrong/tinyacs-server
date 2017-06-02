package vertx.taskmgmt;

import vertx.VertxUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

/**
 * Project:  SXA Task Mgmt
 *
 * @author: ronang
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
    private class asyncEnqueueHandler implements Handler<Long> {
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
        public void handle(Long val) {
            log.info("Successfully added a new task (id: " + taskId + " into redis.");
            VertxUtils.writeToResponse(request, taskId);
        }
    }
}
