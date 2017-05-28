package vertx.taskmgmt.worker;

import vertx.VertxConstants;
import vertx.VertxUtils;
import vertx.taskmgmt.TaskConstants;
import vertx.taskmgmt.TaskUtils;
import vertx.taskmgmt.model.SxaTaskBase;
import io.vertx.java.redis.RedisClient;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Date;

/**
 * Project:  cwmp-parent
 *
 * Base Task Definition/Model for Worker.
 *
 * All tasks shall be defined based on this plus their own parameters.
 *
 * @author: ronang
 */
public abstract class AbstractSxaTaskImpl extends SxaTaskBase implements WorkerAware, Runnable {
    private Logger log = null;

    /**
     * Worker Vertice Instance
     */
    WorkerVertice workerVertice;

    /**
     * Vert.x Instance
     */
    public Vertx vertx;


    /**
     * Jesque Worker Instance
     */
    public SxaTaskWorkerImpl workerInstance;

    /**
     * Start Time in ms
     */
    public long startTime = System.currentTimeMillis();

    /**
     * Default Empty Constructor
     */
    public AbstractSxaTaskImpl() {
        taskJsonObject = new JsonObject();
        log = LoggerFactory.getLogger(this.getClass());
    }

    /**
     * Constructor that takes a JsonObject (called by workers)
     */
    public AbstractSxaTaskImpl(JsonObject taskArgs, WorkerVertice workerVertice) throws Exception {
        super(taskArgs);
        this.workerVertice = workerVertice;
        this.vertx = workerVertice.getVertx();
        log = LoggerFactory.getLogger(this.getClass());

        // Check for parent task
        String parentTask = taskArgs.getString("parentTask");
        if (parentTask != null) {
            /**
             * Notify parent task worker (if any) that this sub task has been picked up by a sub task worker
             */
            JsonObject event = new JsonObject()
                    .putString("subTaskId", getId())
                    .putBoolean("started", true);

            // Send the event to event bus
            vertx.eventBus().send(getSubTaskUpdateAddress(parentTask), event);
        }
    }

    /**
     * Add a new update string to an in-progress task
     * @param update
     */
    public void addUpdate(String update, Boolean persistNow) {
        log.info(this + " : adding update: " + update);

        JsonArray updates = taskJsonObject.getArray("updates");
        if (updates == null) {
            updates = new JsonArray();
            taskJsonObject.putArray("updates", updates);
        }

        /**
         * TODO: only persist the newly added update.
         */
        updates.addString(String.valueOf(updates.size()+1) + " " + new Date() + " " + update);

        /**
         * Persist to MongoDB if needed
         */
        if (persistNow == true) {
            persistTask();
        }
    }

    /**
     * Add and persist a new update string to an in-progress task
     * @param update
     */
    public void addUpdate(String update) {
        addUpdate(update, true);
    }

    /**
     * Async Event Handler for sub task Updates
     */
    public Handler<Message<JsonObject>> subTaskUpdateHandler;

    /**
     * Get all sub tasks.
     * @return
     */
    public JsonObject getSubTasks() {
        JsonObject subTasks = taskJsonObject.getObject("subTasks");
        if (subTasks == null) {
            subTasks = new JsonObject();
            taskJsonObject.putObject("subTasks", subTasks);

            /**
             * Register a handler for receiving sub-task completion events
             */
            subTaskUpdateHandler = new Handler<Message<JsonObject>> () {
                @Override
                public void handle(Message<JsonObject> event) {
                    updateSubTask(event.body());
                }
            };
            vertx.eventBus().registerHandler(getSubTaskUpdateAddress(getId()), subTaskUpdateHandler);
        }

        return subTasks;
    }

    /**
     * Get the 0-based index of the next sub task
     * @return
     */
    public int getNextSubTaskIndex() {
        JsonObject subTasks = taskJsonObject.getObject("subTasks");
        if (subTasks == null) {
            return 0;
        }

        return subTasks.getNumber("totalOutStandingSubTasks", 0).intValue();
    }
    
    /*
     * Default Redis Handler Class for adding a sub task
     */
    public class DefaultAddSubTaskRedisHandler implements Handler<Message<JsonObject>> {
        
    	private String subTaskId;
    	
    	/*
    	 * Constructor
    	 * @param subTaskId
    	 */
    	public DefaultAddSubTaskRedisHandler(String subTaskId){
    		this.subTaskId = subTaskId;
    	}
    	
    	/**
         * The handler method body.
         * @param jsonObjectMessage
         */
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            JsonObject result = jsonObjectMessage.body();
            if (result == null) {
            	log.error("The response message is NULL! " + subTaskId);
            	return;
            } else {            
                String status = result.getString("status");
                if (status  == null) {
                	log.error("The response message is invalid (no status found)! " + subTaskId);
                	return;
                } else if (!status.equals("ok")) {
                	log.error("The response message has unexpected status " + status + "! " + subTaskId);
                	return;
                }
            }
            log.info("Successfully added a new sub task to redis! sub task id: " + subTaskId);
        }
    }

    /**
     * Add a sub task.
     *
     * @param subTask
     * @param bPersistTask
     */
    public void addSubTask(
            final AbstractSxaTaskImpl subTask,
            boolean bPersistTask,
            RedisClient redisClient,
            Handler<Message<JsonObject>> handler
    ) throws Exception {
        /**
         * Get the subTasks object
         */
        JsonObject subTasks = getSubTasks();

        /**
         * Add the new sub task to it
         */
        int totalOutStandingSubTasks = subTasks.getInteger("totalOutStandingSubTasks", 0);;
        subTasks.putObject(subTask.getId(), new JsonObject().putBoolean("completed", false));
        subTasks.putNumber("totalOutStandingSubTasks", totalOutStandingSubTasks+1);

        /**
         * Set "parentTask" field of the sub task
         */
        subTask.taskArgs.putString("parentTask", getId());
        subTask.taskArgs.putString("createTime", new Date().toString());
        subTask.taskArgs.putString("producerHost", VertxUtils.getLocalHostname());
        
        if(handler == null)
        {
        	handler = new DefaultAddSubTaskRedisHandler(subTask.getId());
        }

        /**
         * Enqueue the sub task to Redis
         */
        TaskUtils.enqueueTask(
                subTask.getTaskQueueName(),
                TaskUtils.convertRawTaskToJesqueTask(subTask.taskJsonObject),
                redisClient,
                handler
        );

        /**
         * Need to persist this task?
         */
        if (bPersistTask) {
            persistTask();
        }
    }

    /**
     * Update an existing sub task.
     *
     * @param update
     */
    public void updateSubTask(JsonObject update) {
        String subTaskId = update.getString("subTaskId");
        log.info(this.toString() + ": received update for sub task " + subTaskId + ":\n" + update.encodePrettily());

        JsonObject subTasks = taskJsonObject.getObject("subTasks");
        subTasks.putObject(subTaskId, update);

        /**
         * Update the # of outstanding sub tasks if the sub task is now completed
         */
        Boolean completed = update.getBoolean("completed", false);
        if (completed) {
            /**
             * Update the # of succeeded/failed sub tasks
             */
            int succeededSubTasks = 0;
            int failedSubTasks = 0;
            if (update.getBoolean("succeeded", false) == true) {
                succeededSubTasks = subTasks.getInteger("totalSucceededSubTasks", 0);
                subTasks.putNumber("totalSucceededSubTasks", ++succeededSubTasks);
            } else {
                failedSubTasks = subTasks.getInteger("totalFailedSubTasks", 0);
                subTasks.putNumber("totalFailedSubTasks", ++failedSubTasks);
            }

            int totalOutStandingSubTasks = subTasks.getInteger("totalOutStandingSubTasks", 1);
            if (totalOutStandingSubTasks > 0) {
                totalOutStandingSubTasks --;
            } else {
                log.error("Invalid totalOutStandingSubTasks " + totalOutStandingSubTasks + "!");
                totalOutStandingSubTasks = 0;
            }
            subTasks.putNumber("totalOutStandingSubTasks", totalOutStandingSubTasks);

            if (totalOutStandingSubTasks == 0) {
                /**
                 * All sub tasks are completed
                 */
                // Un-register the sub-task-update handler
                vertx.eventBus().unregisterHandler(getSubTaskUpdateAddress(getId()), subTaskUpdateHandler);

                /**
                 * the top level task is considered succeeded only if all sub tasks are succeeded
                 */
                completeTask(failedSubTasks==0, getTaskResult());
            } else {
                log.info("Outstanding: " + totalOutStandingSubTasks
                        + ", Succeeded: " + succeededSubTasks
                        + ", Failed: " + failedSubTasks);

                /**
                 * Persist the task upon each completed sub task
                 */
                persistTask();
            }
        }
    }

    /**
     * Build a default result Object (String or JsonObject) for tasks with sub tasks
     * @return
     */
    public Object getTaskResult() {
        JsonObject subTasks = taskJsonObject.getObject("subTasks");
        if (subTasks == null) {
            return "Done";
        } else {
            Number succeeded = subTasks.getNumber("totalSucceededSubTasks", 0);
            Number failed = subTasks.getNumber("totalFailedSubTasks", 0);
            int total = succeeded.intValue() + failed.intValue();
            return "All " + total + " sub task(s) are completed with "
                    + succeeded + " succeeded and "
                    + failed + " failed.";
        }
    }

    /**
     * @param worker the Worker executing the job
     */
    @Override
    public void setWorker(Worker worker) {
        workerInstance = (SxaTaskWorkerImpl)worker;
        taskJsonObject.putString("worker", workerInstance.getName());
    }

    /**
     * Get the Vertx Event Bus Address for sub task updates (for this task only)
     */
    public static String getSubTaskUpdateAddress(String parentTaskId) {
        return TaskConstants.VERTX_ADDRESS_SUB_TASK_UPDATES +"." + parentTaskId;
    }

    /**
     * Save Completed Task to MongoDB
     */
    public void persistTask() {
        vertx.eventBus().send(
                VertxConstants.VERTX_ADDRESS_MONGODB,
                new JsonObject()
                        .putString("action", "save")
                        .putString("collection", TaskConstants.MONGODB_TASK_COLLECTION)
                        .putObject("document", taskJsonObject)
        );
    }

    /**
     * Complete the task.
     *
     * @param succeeded
     * @param result
     */
    public void completeTask(boolean succeeded, Object result) {
        // Update Final Task State
        if (succeeded) {
            setState(TaskConstants.TASK_STATE_SUCCEEDED);
        } else {
            setState(TaskConstants.TASK_STATE_FAILED);
        }

        // Add Result
        if (result != null) {
            if (result instanceof String) {
                taskJsonObject.putString("result", (String)result);
            } else if (result instanceof JsonObject) {
                taskJsonObject.putObject("result", (JsonObject) result);
            }
            log.info(this.toString() + ": Completed with result: " + result.toString());
        }

        // Mark the completion time
        taskJsonObject.putString("completeTime", new Date().toString());
        taskJsonObject.putString("timeConsumed", TaskUtils.msToString(System.currentTimeMillis() - startTime));

        // Persist to MongoDB
        persistTask();

        // Notify Jesque poller (to resume polling)
        if (result != null) {
            if (result instanceof String) {
                sendResultToJesquePoller(false, (String) result);
            } else if (result instanceof JsonObject) {
                sendResultToJesquePoller(false, (JsonObject) result);
            }
        } else {
            sendResultToJesquePoller(false, "{}");
        }

        /**
         * Also notify parent task if any
         */
        String parentTask = taskJsonObject.getObject("args").getString("parentTask");
        if (parentTask != null) {
            JsonObject event = new JsonObject()
                    .putString("subTaskId", getId())
                    .putBoolean("completed", true)
                    .putBoolean("succeeded", succeeded);
            if (result != null) {
                if (result instanceof String) {
                    event.putString("result", (String)result);
                } else if (result instanceof JsonObject) {
                    event.putObject("result", (JsonObject)result);
                }
            }

            // Send the event to event bus
            vertx.eventBus().send(getSubTaskUpdateAddress(parentTask), event);
        }
    }

    /**
     * Save Failed Task to MongoDB with an error string
     */
    public void persistFailedTask(String error){
        completeTask(false, error);
    }

    /**
     * Save Successfully Completed Task to MongoDB
     */
    public void persistSucceededTask(){
        persistSucceededTask(null);
    }

    /**
     * Save Successfully Completed Task to MongoDB with a result string
     */
    public void persistSucceededTask(String result){
        completeTask(true, result);
    }

    /**
     * Convert this back to Jesque Job Instance
     * @return
     */
    public JsonObject getJobJsonObject() {
        return new JsonObject()
                .putString("class", getTaskQueueName())
                .putArray("args", new JsonArray().addObject(taskArgs));
    }

    /**
     * Send task execution result to Jesque Poller so it will resume polling.
     * @param succeeded
     * @param error
     */
    public void sendResultToJesquePoller(boolean succeeded, String error) {
        JsonObject result = SxaTaskWorkerImpl.buildJobProcessResult(
                succeeded, error, getJobJsonObject(), getTaskQueueName());
        vertx.eventBus().send(TaskConstants.VERTX_ADDRESS_TASK_RESULTS + "." + getTaskQueueName(), result);
    }

    /**
     * Send task execution result to Jesque Poller so it will resume polling.
     * @param succeeded
     * @param result
     */
    public void sendResultToJesquePoller(boolean succeeded, JsonObject result) {
        vertx.eventBus().send(
                TaskConstants.VERTX_ADDRESS_TASK_RESULTS + "." + getTaskQueueName(),
                SxaTaskWorkerImpl.buildJobProcessResult(succeeded, null, getJobJsonObject(), getTaskQueueName())
                        .putObject("result", result)
        );
    }
}
