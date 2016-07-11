package vertx2.acs.worker.workflow;

import vertx2.VertxException;
import vertx2.VertxMongoUtils;
import vertx2.VertxRedisUtils;
import vertx2.VertxUtils;
import vertx2.model.*;
import vertx2.util.AcsConstants;
import vertx2.taskmgmt.TaskConstants;
import vertx2.taskmgmt.TaskUtils;
import vertx2.taskmgmt.worker.AbstractSxaTaskImpl;
import vertx2.taskmgmt.worker.WorkerVertice;
import io.vertx.java.redis.RedisClient;
import net.greghaines.jesque.utils.ResqueConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Project:  cwmp ACS Server
 *
 * Workflow Worker utilizing the general purpose task worker framework.
 *
 * New Workflow tasks are stored in Redis as individual tasks with optional delay, and can be picked up by any workflow
 * worker instance.
 *
 * If the workflow has to be executed within a recurring window and has not been completed in the current window, the
 * worker instance must schedule the next task before starting the execution.
 *
 * Worker instances must also register handler to listen/process for workflow suspend requests.
 *
 * @author: ronyang
 */
public class ActiveWorkflowTaskWorker extends AbstractSxaTaskImpl{
    private static final Logger log = LoggerFactory.getLogger(ActiveWorkflowTaskWorker.class.getName());

    /**
     * Remote Request Types
     */
    public static final String REMOTE_REQUEST_SUSPEND = "suspend";
    public static final String REMOTE_REQUEST_QUERY = "query";  // query in-progress count

    /**
     * Static Exception(s)
     */

    /**
     * Jesque Key for the task queue
     */
    public static final String JESQUE_KEY =
            TaskUtils.getJesqueKey(ResqueConstants.QUEUE, TaskConstants.INTERNAL_ACS_API_TASK_NS_WORKFLOW);

    /**
     * Workflow Worker Vertice
     */
    ActiveWorkflowWorkerVertice activeWorkflowWorkerVertice;

    /**
     * Workflow POJO
     */
    Workflow workflow;

    /**
     * CPE query matcher.
     */
    public JsonObject queryMatcher;

    /**
     * A Hash Map holding all the CPEs that we are executing this workflow against.
     */
    public HashMap<String, WorkflowCpeTracker> cpeTrackers;

    /**
     * After the CPE collection query result is in, each record in the query result shall be used to initiate
     * a new WorkflowCpeTracker POJO.
     *
     * But since we want to limit the max # of concurrent CPEs, some CPE records in the query result must
     * stay in the backlog for a while.
     */
    public LinkedList<JsonObject> backlog = null;

    /**
     * Workflow Suspend Request Handler
     */
    public Handler<Message<String>> remoteRequestHandler;

    /**
     * Task Name
     */
    public static final String TASK_NAME = "workflow";

    /**
     * Max # of concurrent outstanding tasks
     */
    public static final int MAX_NBR_OF_OUTSTANDING_TASKS = 100;

    /**
     * Max # of worker vertice instances
     */
    public static final int MAX_NBR_OF_VERTICE_INSTANCES = Math.min(VertxUtils.getNumberOfCpuCores(), 8);

    /**
     * Next Scheduled task
     */
    JsonObject nextScheduledTask = null;

    /**
     * No more CPEs
     */
    boolean bNoMoreCpe = false;

    /**
     * Suspended?
     */
    boolean bSuspended = false;

    /**
     * Static Exceptions
     */
    public final Exception NO_ID_FOUND = new Exception("No ID String found!");

    /**
     * Static Query Keys that define the interesting DB fields when querying CPE devices collection.
     */
    public static JsonObject QUERY_KEYS = new JsonObject()
            .putNumber(AcsConstants.FIELD_NAME_ID, 1)
            .putNumber(AcsConstants.FIELD_NAME_ORG_ID, 1)
            .putNumber(CpeDeviceType.FIELD_NAME_OUI, 1)
            .putNumber(Cpe.DB_FIELD_NAME_SN, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_URL, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_USERNAME, 1)
            .putNumber(Cpe.DB_FIELD_NAME_CONNREQ_PASSWORD, 1);

    /**
     * Empty Constructor
     */
    public ActiveWorkflowTaskWorker() {
        super();
    }

    /**
     * Constructor by JSON Object
     */
    public ActiveWorkflowTaskWorker(JsonObject taskArgs, WorkerVertice workerVertice) throws Exception {
        super((taskArgs), workerVertice);

        log.info("Processing a new " + getTaskName() + "task...");

        // Convert to Workflow Worker Vertice
        activeWorkflowWorkerVertice = (ActiveWorkflowWorkerVertice) workerVertice;

        /**
         * Extract the workflow from task args
         */
        workflow = Workflow.validateJsonObject(taskArgs.getObject(TASK_NAME));
        if (workflow.id == null) {
            throw NO_ID_FOUND;
        }

        /**
         * Register Workflow Query/Suspend Request Handler
         */
        remoteRequestHandler = new Handler<Message<String>>() {
            @Override
            public void handle(final Message<String> reqMessage) {
                log.info("Received " + reqMessage.body() + " request for " + workflow.id);

                switch (reqMessage.body()) {
                    case REMOTE_REQUEST_SUSPEND:
                        bSuspended = true;
                        workflow.state = Workflow.STATE_SUSPENDED;

                        /**
                         * Cleanup backlogs
                         */
                        backlog.clear();

                        /**
                         * Cancel the scheduled task for the next window if any
                         */
                        if (nextScheduledTask != null) {
                            log.info("Cancelling the scheduled task for next window...");
                            activeWorkflowWorkerVertice.redisClient.zrem(JESQUE_KEY, nextScheduledTask.toString());
                        }

                        /**
                         * Cancel the window-close timer
                         */
                        if (windowCloseTimer != null) {
                            log.info("Cancelling the window close timer...");
                            vertx.cancelTimer(windowCloseTimer);
                        }

                        /**
                         * Leave all the CPE tracks alone for now
                         */

                        /**
                         * Send reply
                         */
                        reqMessage.reply("suspended");
                        break;

                    case REMOTE_REQUEST_QUERY:
                        /**
                         * Send reply with # of in-progress CPEs
                         */
                        reqMessage.reply(String.valueOf(cpeTrackers.size()));
                        break;

                    default:
                        /**
                         * Send reply with # of in-progress CPEs
                         */
                        reqMessage.reply("Invalid Request " + reqMessage.body() + "!");
                        break;
                }
            }
        };
        vertx.eventBus().registerHandler(
                AcsConstants.VERTX_ADDRESS_WORKFLOW_SUSPEND + "." + workflow.id,
                remoteRequestHandler
        );

        /**
         * Add new workflow POJO into hash map
         */
        activeWorkflowWorkerVertice.workflowTaskHashMap.put(workflow.id, this);
    }

    /**
     * Return the type/name (i.e. "class" in Resque term) of the task
     *
     * @return
     */
    @Override
    public String getTaskName() {
        return TASK_NAME;
    }

    /**
     * Return the name of the task queue that contains this type of tasks
     *
     * @return
     */
    @Override
    public String getTaskQueueName() {
        return TaskConstants.INTERNAL_ACS_API_TASK_NS_WORKFLOW;
    }

    /**
     * Query result handler
     */
    public VertxMongoUtils.FindHandler queryResultHandler = null;

    /**
     * Window Close Timer
     */
    public Long windowCloseTimer = null;
    public boolean bWindowClosed = false;

    /**
     * Update workflow state to in-progress
     */
    public static final JsonObject START_PROGRESS =
            new JsonObject().putString(Workflow.FIELD_NAME_STATE, Workflow.STATE_IN_PROGRESS);

    /**
     * Update workflow state to in-progress
     */
    public static final JsonObject SCHEDULED =
            new JsonObject().putString(Workflow.FIELD_NAME_STATE, Workflow.STATE_SCHEDULED);

    /**
     * Main Engine.
     */
    @Override
    public void run() {
        /**
         * Schedule the task for next window if applicable
         */
        if (WorkflowTrigger.TriggerTypeEnum.MAINTENANCE_WINDOW.equals(workflow.execPolicy.initialTrigger.triggerType)) {
            if (workflow.execPolicy.maintenanceWindow.recurringInterval > 0) {
                if (workflow.execPolicy.maintenanceWindow.timeTillNextOpen() >= 0) {
                    // Get the index of this task
                    final String thisId = getId();
                    int index = Integer.valueOf(thisId.substring(thisId.indexOf('.') + 1)) + 1;
                    nextScheduledTask = enqueue(
                            workflow,
                            index,
                            workflow.execPolicy.maintenanceWindow.recurringInterval,
                            activeWorkflowWorkerVertice.redisClient,
                            new Handler<Message<JsonObject>>() {
                                @Override
                                public void handle(Message<JsonObject> enqueueResult) {
                                    JsonObject result = enqueueResult.body();
                                    if (result == null ||
                                            !("ok".equals(result.getString("status")))
                                            ) {
                                        log.error("Received unexpected result when scheduling a new workflow task!\n"
                                                + "Workflow:\n" + workflow.rawJsonObject.encodePrettily()
                                                + "\nEnqueue Result:\n"
                                                + (result == null ? "(null)" : result.encodePrettily()));
                                    }
                                    log.info("Scheduled the task for the next window with delay="
                                            + workflow.execPolicy.maintenanceWindow.recurringInterval + " second(s).");
                                }
                            }

                    );
                } else {
                    log.info(workflow.id + ": This is the last window.");
                }
            }

            /**
             * Also start a timer to terminate this task at window close
             */
            windowCloseTimer = vertx.setTimer(
                    workflow.execPolicy.maintenanceWindow.windowLength * 1000,
                    new Handler<Long>() {
                        @Override
                        public void handle(Long aLong) {
                            // Unregister SAR event handler
                            vertx.eventBus().unregisterHandler(
                                    AcsConstants.VERTX_ADDRESS_WORKFLOW_SUSPEND + "." + workflow.id,
                                    remoteRequestHandler
                            );

                            log.info(VertxUtils.highlightWithHashes(workflow.id + ": Window is closed."));
                            bWindowClosed = true;

                            // Clear backlog
                            backlog.clear();

                            // All on-going CPE trackers are left alone till finish

                            // Change workflow state to "scheduled"
                            try {
                                VertxMongoUtils.update(
                                        vertx.eventBus(),
                                        Workflow.DB_COLLECTION_NAME,
                                        workflow.id,
                                        VertxMongoUtils.getUpdatesObject(
                                                SCHEDULED,
                                                null,
                                                null,
                                                null,
                                                null
                                        ),
                                        null
                                );
                            } catch (VertxException e) {
                                e.printStackTrace();
                            }
                        }
                    }
            );
        }

        /**
         * If this is the first task for this workflow, change state to "in-progress"
         */
        try {
            VertxMongoUtils.update(
                    vertx.eventBus(),
                    Workflow.DB_COLLECTION_NAME,
                    workflow.id,
                    VertxMongoUtils.getUpdatesObject(
                            START_PROGRESS,
                            null,
                            VertxMongoUtils.addTimeStamp(null, Workflow.FIELD_NAME_START),
                            null,
                            null
                    ),
                    null
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }

        /**
         * Initialize the linked lists and hash maps
         */
        cpeTrackers = new HashMap<>();
        backlog = new LinkedList<>();

        // Initialize find handler
        queryResultHandler = getCpeQueryResultHandler();

        // Build a query matcher
        queryMatcher = workflow.getMatcher(activeWorkflowWorkerVertice.groupCache, true);

        // Start the query chain
        doQuery();
    }

    /**
     * Run a query with the matcher
     */
    public void doQuery() {
        if (backlog.size() == 0 && cpeTrackers.size() == 0) {
            if (bWindowClosed || bSuspended || bNoMoreCpe) {
                done();
                return;
            }
        }

        log.info("Running Query with matcher " + queryMatcher);
        try {
            VertxMongoUtils.find(
                    vertx.eventBus(),
                    Cpe.CPE_COLLECTION_NAME,
                    queryMatcher,
                    null,
                    0,
                    workflow.execPolicy.maxConcurrentDevices * 2 + 1,
                    queryResultHandler,
                    QUERY_KEYS,
                    workflow.execPolicy.maxConcurrentDevices * 2 + 1
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }

    /**
     * Enqueue a new workflow task directly into Redis.
     *
     * @param workflow
     * @param index
     * @param delay
     * @param redisClient
     * @param handler
     */
    public static JsonObject enqueue(
            Workflow workflow,
            int index,
            long delay,
            RedisClient redisClient,
            Handler<Message<JsonObject>> handler) {
        // Build a raw task
        JsonObject rawTask = buildRawTask(
                TaskConstants.INTERNAL_ACS_API_TASK_NS_WORKFLOW,
                TASK_NAME,
                new JsonObject()
                        .putObject(TASK_NAME, workflow.rawJsonObject)
                        .putString("_id", workflow.id + "." + index)
        );

        // Enqueue to Redis directly
        JsonObject task = null;
        log.info("En-queuing a task with delay " + delay + " second(s)." + "(task id: " + workflow.id + "." + index);
        try {
            task = TaskUtils.convertRawTaskToJesqueTask(rawTask);
            TaskUtils.delayedEnqueueTask(
                    TaskConstants.INTERNAL_ACS_API_TASK_NS_WORKFLOW,
                    TaskUtils.convertRawTaskToJesqueTask(rawTask),
                    delay * 1000 + System.currentTimeMillis(),
                    redisClient,
                    handler);
            return task;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Remove workflow task by workflow id string.
     *
     * To be called by WorkflowService when suspending/deleting workflows.
     *
     * @param workflowId
     * @param redisClient
     * @param handler
     */
    public static void removeTaskByWorkflowId(
            final String workflowId,
            final RedisClient redisClient,
            final Handler<Long> handler) {
        VertxRedisUtils.zrange(
                redisClient,
                JESQUE_KEY,
                0,
                -1,
                new Handler<JsonArray>() {
                    @Override
                    public void handle(JsonArray allWorkflowTasks) {
                        String task = null;
                        if (allWorkflowTasks != null && allWorkflowTasks.size() > 0) {
                            for (int i = 0; i < allWorkflowTasks.size(); i ++) {
                                String aTask = allWorkflowTasks.get(i).toString();
                                if (aTask.contains(workflowId)) {
                                    // Found it
                                    // Delete it
                                    VertxRedisUtils.zrem(
                                            redisClient,
                                            JESQUE_KEY,
                                            aTask,
                                            handler
                                    );
                                    return;
                                }
                            }
                        }

                        // Did not find the target workflow
                        if (handler != null) {
                            handler.handle(null);
                        } else {
                            log.info("Did not find task for workflow " + workflowId);
                        }
                    }
                }
        );
    }

    /**
     * Edit workflow task by file id.
     *
     * The keyword is a internal id string for a config file or SW image.
     *
     * To be called by File Service when editing config file or SW image..
     *
     * @param fileId
     * @param newFileStruct
     * @param redisClient
     * @param handler
     */
    public static void editTaskByFileId(
            final String fileId,
            final JsonObject newFileStruct,
            final RedisClient redisClient,
            final Handler<Integer> handler) {
        // Read all existing tasks from Redis
        VertxRedisUtils.zrange(
                redisClient,
                JESQUE_KEY,
                0,
                -1,
                new Handler<JsonArray>() {
                    @Override
                    public void handle(JsonArray allWorkflowTasks) {
                        if (allWorkflowTasks == null || allWorkflowTasks.size()== 0) {
                            log.info("No existing workflow task found.");
                            handler.handle(0);
                            return;
                        }

                        final ArrayList<Workflow> workflowArrayList = new ArrayList<Workflow>();
                        for (int i = 0; i < allWorkflowTasks.size(); i ++) {
                            String aTask = allWorkflowTasks.get(i).toString();
                            if (aTask.contains("\"" + fileId + "\"")) {
                                // Found one
                                try {
                                    JsonObject taskJsonObj = new JsonObject(aTask);
                                    JsonArray args = taskJsonObj.getArray("args");
                                    JsonObject firstArg = args.get(0);
                                    JsonObject workflowJsonObj = firstArg.getObject("workflow");
                                    JsonArray actions = workflowJsonObj.getArray(Workflow.FIELD_NAME_ACTIONS);
                                    for (JsonObject action : (JsonObject[])actions.toArray()) {
                                        if (fileId.equals(action.getString(WorkflowAction.FIELD_NAME_FILE_ID))) {
                                            action.removeField(WorkflowAction.FIELD_NAME_FILE_STRUCT);
                                            action.putObject(WorkflowAction.FIELD_NAME_FILE_STRUCT, newFileStruct);
                                        }
                                    }
                                    Workflow workflowPojo = Workflow.validateJsonObject(workflowJsonObj);
                                    // Quick-and-dirty way to store the task JSON object
                                    workflowPojo.rawJsonObject = taskJsonObj;
                                    workflowArrayList.add(workflowPojo);
                                } catch (Exception ex) {
                                    log.error("Found an invalid workflow task! " + aTask);
                                }
                                // Delete it either way
                                VertxRedisUtils.zrem(
                                        redisClient,
                                        JESQUE_KEY,
                                        aTask,
                                        null
                                );
                            }
                        }

                        if (workflowArrayList.size() == 0) {
                            log.info("File " + fileId + " is not used by any workflow task.");
                            handler.handle(0);
                            return;
                        }

                        new MultiTaskZaddTracker(redisClient, workflowArrayList, handler).addFirst();
                    }
                }
        );
    }

    /**
     * Private Inner Class required for implementing the above method editTaskByFileId().
     */
    private static class MultiTaskZaddTracker {
        RedisClient redisClient;
        ArrayList<Workflow> workflowArrayList;
        int numberOfTasks;
        Handler<Integer> finalHandler;
        Handler<Message<JsonObject>> zaddHandler;

        /**
         * Constructor.
         * @param redisClient
         * @param workflowArrayList
         * @param finalHandler
         */
        private MultiTaskZaddTracker(
                final RedisClient redisClient,
                final ArrayList<Workflow> workflowArrayList,
                final Handler<Integer> finalHandler) {
            this.workflowArrayList = workflowArrayList;
            numberOfTasks = workflowArrayList.size();
            zaddHandler = new Handler<Message<JsonObject>>() {
                @Override
                public void handle(Message<JsonObject> event) {
                    // Add the next task from list if any
                    workflowArrayList .remove(0);
                    if (workflowArrayList.size() > 0) {
                        addFirst();
                    } else {
                        finalHandler.handle(numberOfTasks);
                    }
                }
            };
        }

        /**
         * Add the first task from list.
         */
        private void addFirst() {
            if (workflowArrayList.size() > 0) {
                Workflow nextTask = workflowArrayList.get(0);
                TaskUtils.delayedEnqueueTask(
                        TaskConstants.INTERNAL_ACS_API_TASK_NS_WORKFLOW,
                        nextTask.rawJsonObject,
                        nextTask.getInitialDelay() * 1000 + System.currentTimeMillis(),
                        redisClient,
                        zaddHandler
                );
            }
        }
    }

    /**
     * Extending/Overwriting the VertxMongoUtils.FindHandler
     */
    public VertxMongoUtils.FindHandler getCpeQueryResultHandler() {
        try {
            return new VertxMongoUtils.FindHandler(new Handler<JsonArray>() {
                @Override
                public void handle(JsonArray queryResults) {
                    if (queryResults != null || queryResults.size() == 0) {
                        // Iterate all CPEs in the result
                        for (int i = 0; i < queryResults.size(); i++) {
                            JsonObject aCpe = queryResults.get(i);

                            /**
                             * First make sure this CPE is not currently in the cpeTrackers
                             */
                            if (cpeTrackers.size() > 0 &&
                                    cpeTrackers.get(aCpe.getString(AcsConstants.FIELD_NAME_ID)) != null) {
                                continue;
                            }

                            if (cpeTrackers.size() < workflow.execPolicy.maxConcurrentDevices) {
                                // still has room for more CPE
                                cpeTrackers.put(
                                        aCpe.getString(AcsConstants.FIELD_NAME_ID),
                                        new WorkflowCpeTracker(
                                                vertx,
                                                aCpe,
                                                workflow,
                                                cpeExecResultHandler
                                        )
                                );
                            } else {
                                // put this CPE into backlog
                                backlog.add(aCpe);
                            }
                        }

                        /**
                         * more exist?
                         */
                        if (queryResults.size() < (workflow.execPolicy.maxConcurrentDevices * 2 + 1)) {
                            bNoMoreCpe = true;
                        }
                    } else {
                        // No more matching CPE. All  done
                        bNoMoreCpe = true;
                    }

                    if (backlog.size() == 0 && cpeTrackers.size() == 0 && bNoMoreCpe) {
                        done();
                    }
                }
            });
        } catch (VertxException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Result Handler after the executing this workflow against a single CPE
     */
    public Handler<JsonObject> cpeExecResultHandler = new Handler<JsonObject>() {
        @Override
        public void handle(JsonObject cpeExecResult) {
            /**
             * Remove this CPE tracker from linked list
             */
            cpeTrackers.remove(cpeExecResult.getString(AcsConstants.FIELD_NAME_ID));

            /**
             * Grab one more CPE from backlog if any
             */
            if (backlog.size() > 0) {
                JsonObject aCpe = backlog.removeFirst();
                // Kick it off for this CPE
                cpeTrackers.put(
                        aCpe.getString(AcsConstants.FIELD_NAME_ID),
                        new WorkflowCpeTracker(
                                vertx,
                                aCpe,
                                workflow,
                                cpeExecResultHandler
                        )
                );

                // Continue executing the next query in the linked list (if any) if backlog is now empty
                if (backlog.size() == 0 && areAllCpeTrackersInProgress()) {
                    // Run the next query if any
                    doQuery();
                }
            } else {
                // Run query again
                doQuery();
            }
        }
    };

    /**
     * Check if all CPE trackers are in-progress (updated the CPE record)
     * @return
     */
    public boolean areAllCpeTrackersInProgress() {
        for (WorkflowCpeTracker tracker : cpeTrackers.values()) {
            if (tracker.inProgress == false) {
                return false;
            }
        }

        return true;
    }

    /**
     * Method to be called when the workflow task is done
     */
    public static final JsonObject ALL_DONE =
            new JsonObject().putString(Workflow.FIELD_NAME_STATE, Workflow.STATE_COMPLETED);
    public void done() {
        String taskState = this.taskJsonObject.getString("state");
        if (TaskConstants.TASK_STATE_SUCCEEDED.equals(taskState)) {
            return;
        }

        log.info(VertxUtils.highlightWithHashes(
                "Workflow Task for " + this.getId() + " is completed."));

        /**
         * Cancel window close timer if any
         */
        if (windowCloseTimer != null) {
            log.info("Cancelling the window close timer...");
            vertx.cancelTimer(windowCloseTimer);
            windowCloseTimer = null;
        }

        // Complete the "task" from task service perspective
        persistSucceededTask();

        // Remove from hash map
        activeWorkflowWorkerVertice.workflowTaskHashMap.remove(workflow.id);

        if (bSuspended) {
            return;
        }

        // Is the workflow completed?
        JsonObject setWorkflowState = SCHEDULED;
        JsonObject endTime = null;
        if (bNoMoreCpe && cpeTrackers.size() == 0) {
            log.info(VertxUtils.highlightWithHashes(
                    "Workflow " + workflow.id + " is competed."));
            /**
             * Cancel the scheduled task for the next window if any
             */
            if (nextScheduledTask != null) {
                log.info("Cancelling the scheduled task for next window...");
                activeWorkflowWorkerVertice.redisClient.zrem(JESQUE_KEY, nextScheduledTask.toString());
                nextScheduledTask = null;
            }

            // Update workflow status
            setWorkflowState = ALL_DONE;
            endTime = VertxMongoUtils.addTimeStamp(null, Workflow.FIELD_NAME_END);
        }

        // Update workflow state
        try {
            VertxMongoUtils.update(
                    vertx.eventBus(),
                    Workflow.DB_COLLECTION_NAME,
                    workflow.id,
                    VertxMongoUtils.getUpdatesObject(
                            setWorkflowState,
                            null,
                            endTime,
                            null,
                            null
                    ),
                    null
            );
        } catch (VertxException e) {
            e.printStackTrace();
        }
    }
}
