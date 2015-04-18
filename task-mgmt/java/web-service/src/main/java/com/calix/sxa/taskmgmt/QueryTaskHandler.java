package com.calix.sxa.taskmgmt;

import com.calix.sxa.VertxConstants;
import com.calix.sxa.VertxUtils;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;

import static net.greghaines.jesque.utils.ResqueConstants.QUEUE;

/**
 * Project:  SXA Task Management
 *
 * Although named as "QueryTaskHandler", this class also handles cancelling/deleting tasks.
 *
 * @author: jqin
 */
public class QueryTaskHandler  {
    private static final Logger log = LoggerFactory.getLogger(QueryTaskHandler.class.getName());

    /**
     * Create a JsonArray to store the result
     */
    JsonArray result = new JsonArray();

    /**
     * Filters
     */
    Filters filters;

    /**
     * Event Bus
     */
    EventBus eventBus;

    /**
     * Constructor that requires the HTTP request and the Vert.x Event Bus
     *
     * @param request
     * @param eventBus
     * @throws Exception
     */
    public QueryTaskHandler(HttpServerRequest request, EventBus eventBus) throws Exception {
        filters = new Filters(request);
        this.eventBus = eventBus;
    }

    /**
     * Public no-arg constructor
     */
    public QueryTaskHandler() {
    }

    /**
     * Enum that defines the process stage.
     *
     * Stage 1: Query Redis
     * Stage 2: Query MongoDB
     */
    public static enum Stage {
        Redis,
        MongoDB,
        Done,
        Null
    }

    /**
     * Inner class that defines query filters
     */
    public class Filters {
        /**
         * Raw HTTP Request
         */
        HttpServerRequest request;

        /**
         * Available Query Filters
         */
        Integer orgId= null;
        String id= null;
        String queueName= null;
        String username= null;
        String taskType= null;
        String producerHost= null;
        String producerApp= null;
        String state= null;
        boolean brief= true;

        /**
         * Boolean indicator: true if the task is to be deleted permanently
         */
        boolean bDelete = false;

        /**
         * Boolean indicator: true if the pending task is to be cancelled/aborted
         */
        boolean bCancelPending = false;

        /**
         * Constructor by HTTP Request with all filters being query parameters
         * @param request
         */
        public Filters(HttpServerRequest request) throws Exception {
            this.request = request;

            /**
             * Extract all the filters
             */
            String rawQueryString = request.query();
            if (rawQueryString != null) {
                String[] params = rawQueryString.split("&");
                for (String param : params) {
                    int splitterIndex = param.indexOf('=');
                    if (splitterIndex < 2 || splitterIndex == (param.length() - 1)) {
                        VertxUtils.setResponseStatus(request, HttpResponseStatus.BAD_REQUEST);
                        VertxUtils.writeToResponse(request, "Illegal Query String!(" + rawQueryString + ")");
                        throw new Exception("Illegal Query String!(" + rawQueryString + ")");
                    }

                    String value = param.substring(splitterIndex + 1, param.length());
                    switch (param.substring(0, splitterIndex)) {
                        case "orgId":           orgId = Integer.valueOf(value); break;
                        case "id":              id = value; break;
                        case "queue":           queueName = value; break;
                        case "username":        username = value; break;
                        case "taskType":        taskType = value; break;
                        case "producerHost":    producerHost = value; break;
                        case "producerApp":     producerApp = value; break;
                        case "state":           state = value; break;
                        //case "last":            lastId = value; break;
                        //case "max":             maxTasks = Integer.valueOf(value); break;
                        case "brief":           brief = Boolean.valueOf(value); break;
                        default:
                            throw new Exception("Illegal Query String!(" + rawQueryString + ")");
                    }
                }
            }

            /**
             * Check for cancel/abort/delete
             */
            if (request.method().equals(HttpMethod.DELETE.name())) {
                bDelete = true;
            } else if (request.method().equals(HttpMethod.PUT.name())) {
                bCancelPending = true;
                state = TaskConstants.TASK_STATE_PENDING;
            }

            /**
             * Task Queue must be specified
             */
            if (queueName == null && id == null) {
                throw new Exception("Illegal Query String (Task Queue Name or Task Id must be specified)!("
                         + rawQueryString + ")");
            }
        }

        /**
         * Build a MongoDB query message.
         *
         * @return
         */
        public JsonObject getMongoQueryMessage() {
            JsonObject queryMessage = new JsonObject();
            queryMessage.putString("action", "find");
            queryMessage.putString("collection", TaskConstants.MONGODB_TASK_COLLECTION);

            /**
             * Setup matcher.
             *
             * "matcher" is a JSON object that you want to match against to find matching documents.
             * This obeys the normal MongoDB matching rules.
             *
             * Currently we can only match on task Id. and class and task queue name.
             */
            JsonObject matcher = new JsonObject();
            if (id != null) {
                matcher.putString("_id", id);
            } else {
                matcher.putString("queue", filters.queueName);
                if (taskType != null) {
                    matcher.putString("type", taskType);
                }
            }
            queryMessage.putObject("matcher", matcher);

            if (brief) {
                // The "updates" element shall not be included in brief report
                queryMessage.putObject(
                        "keys",
                        new JsonObject()
                                .putNumber("updates", 0)
                );
            }
            log.info("MongoDB Query Message: " + queryMessage.toString());
            return queryMessage;
        }

        /**
         * Return true if the given task matches all the filters, or false.
         * @param task
         * @return
         */
        boolean match(JsonObject task) {
            JsonElement taskArgElement = task.getElement("args");
            if (taskArgElement == null) {
                log.error("Unable to find \"args\" element in task " + task.toString());
                return false;
            }

            JsonObject taskArgs = null;
            if (taskArgElement.isArray()) {
                //Redis
                taskArgs = task.getArray("args").get(0);
                // Replace the taskArgs array with the external format
                task.removeField("args");
                String taskType = task.getString("class");
                task.removeField("class");
                task.putString("type", taskType);
                task.putObject("args", taskArgs);
                task.putString("_id", taskArgs.getString("_id"));
                taskArgs.removeField("_id");
                task.putString("state", TaskConstants.TASK_STATE_PENDING);
            } else {
                // MongoDB
                taskArgs = taskArgElement.asObject();
            }

            /**
             * TODO: Change to debug
             */
            log.info("Processing task: " + task.toString());
            if (orgId != null && !orgId.equals(taskArgs.getString("orgId"))) {
                return false;
            }
            if (id != null && !id.equals(task.getString("_id"))) {
                return false;
            }
            if (producerHost != null && !producerHost.equals(taskArgs.getString("producerHost"))) {
                return false;
            }
            if (producerApp != null && !producerApp.equals(taskArgs.getString("producerApp"))) {
                return false;
            }
            if (taskType != null && !taskType.equals(task.getString("type"))) {
                return false;
            }
            if (username != null && !username.equals(taskArgs.getString("username"))) {
                return false;
            }
            if (state != null && !state.equals(task.getString("state"))) {
                return false;
            }

            return true;
        }
    }

    /**
     * Performs the query.
     */
    public void run() {
        /**
         * Now let us kick off the query
         */
        if (filters.state == null || filters.state.equals(TaskConstants.TASK_STATE_PENDING)) {
            /**
             * Read all the tasks out of task queue
             */
            TaskMgmtRestWsVertice.redisClient.lrange(
                    TaskUtils.getJesqueKey(QUEUE, filters.queueName),
                    0,
                    Integer.MAX_VALUE,
                    new CommonAsyncHandler(filters, result, Stage.Redis, Stage.MongoDB, eventBus)
            );
        } else {
            /**
             * Skip Redis and query MongoDB directly
             */
            eventBus.send(
                    VertxConstants.VERTX_ADDRESS_MONGODB,
                    filters.getMongoQueryMessage(),
                    new CommonAsyncHandler(filters, result, Stage.MongoDB, Stage.Done, eventBus)
            );
        }
    }

    /**
     * Common Async Handler for the async result
     */
    public class CommonAsyncHandler implements Handler<Message<JsonObject>> {
        /**
         * Original HTTP Request
         */
        public  Filters filters;

        /**
         * The element name of the result ("value" for Redis or "results" for mongo persistor
         */
        public String elementName = null;

        /**
         * The JsonArray to store the result
         */
        JsonArray result = new JsonArray();

        /**
         * Current Stage
         */
        Stage stage;

        /**
         * Next Stage
         */
        Stage nextStage;

        /**
         * Event Bus
         */
        EventBus eventBus;

        /**
         * Constructor.
         *
         * @param filters
         * @param result
         * @param stage
         * @param eventBus
         */
        public CommonAsyncHandler(
                Filters filters,
                JsonArray result,
                Stage stage,
                Stage nextStage,
                EventBus eventBus) {
            this.filters = filters;
            this.result = result;
            this.stage = stage;
            this.nextStage = nextStage;
            if (stage == Stage.Redis) {
                this.elementName = "value";
            } else {
                // MongoDB
                this.elementName = "results";
            }
            this.eventBus = eventBus;
        }

        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            JsonObject jsonObject = jsonObjectMessage.body();
            String error = TaskMgmtWsUtils.checkAsyncResult(filters.request, jsonObject);
            if (error != null) {
                log.error(error);
                return;
            }

            log.info(stage.name() + " Result:\n" + jsonObject.toString());

            /**
             * Extract tasks from the raw result
             */
            JsonElement tasks = jsonObject.getElement(elementName);
            Iterator taskIterator = null;
            if (tasks.isArray()) {
                taskIterator = tasks.asArray().iterator();
            } else if (tasks.isObject()) {
                log.error(tasks.asObject().toString());
            }

            /**
             * Iterate all tasks
             */
            while (taskIterator != null && taskIterator.hasNext()) {
                Object taskObject = taskIterator.next();
                JsonObject task = null;
                if (taskObject instanceof String) {
                    // Redis case
                    try {
                        task = new JsonObject((String)taskObject);
                    } catch (Exception ex) {
                        log.error("Invalid task string " + (String)taskObject + "!" + ex.toString());
                    }
                } else if (taskObject instanceof JsonObject) {
                    // MongoDB case
                    task = (JsonObject)taskObject;
                } else {
                    log.error("Unexpected task class " + taskObject.getClass());
                }

                if (task == null) {
                    log.error("Cannot convert task from task object: " + taskObject.toString());
                    continue;
                }

                if (filters.match(task)) {
                    /**
                     * Found a match.
                     */
                    if (filters.brief) {
                        task.removeField("updates");
                    }

                    /**
                     * Move "producerHost"/"producerApp"/"orgId"/"sn" from args to top level
                     */
                    JsonObject taskArgs = task.getObject("args");
                    TaskUtils.moveJsonStringElement("parentTask", taskArgs, task);
                    TaskUtils.moveJsonStringElement("producerHost", taskArgs, task);
                    TaskUtils.moveJsonStringElement("producerApp", taskArgs, task);
                    TaskUtils.moveJsonStringElement("createTime", taskArgs, task);
                    TaskUtils.moveJsonNumberElement("orgId", taskArgs, task);
                    TaskUtils.moveJsonNumberElement("sn", taskArgs, task);

                    /**
                     * Add task queue name for pending tasks
                     */
                    if (TaskConstants.TASK_STATE_PENDING.equals(task.getString("state"))) {
                        task.putString("queue", filters.queueName);
                    }

                    /**
                     * Are we cancelling/aborting the matching task(s)?
                     */
                    if (filters.bCancelPending == true) {
                        /**
                         * Delete this pending task queue from Redis via "LREM" command
                         */
                        TaskMgmtRestWsVertice.redisClient.lrem(
                                TaskUtils.getJesqueKey(QUEUE, filters.queueName),
                                -1,
                                taskObject.toString()
                        );

                        /**
                         * Update result
                         */
                        task.putString("queue", filters.queueName);
                        task.putString("state", TaskConstants.TASK_STATE_CANCELLED);

                        /**
                         * Store the cancelled task into MongoD
                         */
                        JsonObject mongoMessage = new JsonObject()
                                .putString("action", "save")
                                .putString("collection", TaskConstants.MONGODB_TASK_COLLECTION)
                                .putObject("document", task);
                        eventBus.send(VertxConstants.VERTX_ADDRESS_MONGODB, mongoMessage);
                    }

                    /**
                     * Are we deleting the matching task(s)?
                     */
                    if (filters.bDelete == true) {
                        if (task.getString("state") != null &&
                                task.getString("state").equals(TaskConstants.TASK_STATE_IN_PROGRESS)) {
                            /**
                             * In-progress tasks cannot be deleted
                             */
                            log.error("In-progress tasks cannot be deleted!");

                            /**
                             * Update result
                             */
                            task.putString("state", "in-progress (failed to delete)");
                        } else {
                            if (stage == Stage.Redis) {
                                /**
                                 * Delete this pending task from Redis via "LREM" command
                                 */
                                TaskMgmtRestWsVertice.redisClient.lrem(
                                        TaskUtils.getJesqueKey(QUEUE, filters.queueName),
                                        -1,
                                        taskObject.toString()
                                );
                            } else {
                                /**
                                 * Delete this completed task from MongoDB
                                 */
                                JsonObject mongoMessage = new JsonObject()
                                        .putString("action", "delete")
                                        .putString("collection", TaskConstants.MONGODB_TASK_COLLECTION)
                                        .putObject("matcher",
                                            new JsonObject().putString("_id", task.getString("_id")));
                                eventBus.send(VertxConstants.VERTX_ADDRESS_MONGODB, mongoMessage);
                            }

                            /**
                             * Update result
                             */
                            task.putString("state", "deleted");
                        }
                    }

                    result.add(task);
                }
            }

            /**
             * Perform next step based on the specified next stage
             */
            switch (nextStage) {
                case Redis:
                    break;
                case MongoDB:
                    eventBus.send(
                            VertxConstants.VERTX_ADDRESS_MONGODB,
                            filters.getMongoQueryMessage(),
                            new CommonAsyncHandler(filters, result, Stage.MongoDB, Stage.Done, eventBus)
                    );
                    break;
                case Done:
                    // We are done
                    filters.request.response().putHeader("Content-Type", "application/json");
                    //request.response().write(respPayload, "UTF-8");
                    filters.request.response().end(result.encodePrettily());
                    break;
                case Null:
                    break;
            }
        }
    }
}
