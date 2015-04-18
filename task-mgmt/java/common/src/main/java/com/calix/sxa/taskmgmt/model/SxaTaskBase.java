package com.calix.sxa.taskmgmt.model;

import com.calix.sxa.taskmgmt.TaskConstants;
import com.calix.sxa.taskmgmt.TaskUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

import java.util.Date;

/**
 * Project:  sxa-cc-parent
 *
 * Base Task Definition/Model.
 *
 * All tasks shall be defined based on this plus their own parameters.
 *
 * @author: jqin
 */
public abstract class SxaTaskBase {
    private static Logger log = LoggerFactory.getLogger(SxaTaskBase.class);

    /**
     * A JsonObject that represents this task
     */
    public JsonObject taskJsonObject;

    /**
     * Raw JSON Object passed from producer client
     */
    public JsonObject taskArgs;

    /**
     * A temp String (without the "updates") used by the toString() method.
     */
    String taskString = null;

    /**
     * Default Empty Constructor
     */
    public SxaTaskBase() {
        taskJsonObject = new JsonObject();
    }

    /**
     * Static Util to generate id string
     */
    public static String generateTaskId(
            String taskQueue,
            String producerHost,
            String producerApp,
            String createTime,
            Number orgId,
            Number sn
    ) {
        String id = orgId + "-" + taskQueue + "-" + producerHost + "-" + producerApp + "-";
        if (sn == null) {
            id += String.valueOf(System.currentTimeMillis()/1000);
        } else {
            id += sn.toString();
        }

        return id + "-" + TaskUtils.getNextTaskSn();
    }

    /**
     * Validate Task Args.
     *
     * @param taskArgs
     * @param taskQueue
     * @return  The task Id string if passed the validation, or null if failed.
     */
    public static String validateTaskArgs(String taskQueue, JsonObject taskArgs) {
        /**
         * Validate the arguments
         */
        String id = taskArgs.getString("_id");
        if (id == null) {
            /**
             * The following parameters are required to generate an id if the producer didn't provide an id
             */
            String producerHost = taskArgs.getString("producerHost");
            String producerApp = taskArgs.getString("producerApp");
            String createTime = taskArgs.getString("createTime");
            Number sn = taskArgs.getNumber("sn");
            Number orgId = taskArgs.getNumber("orgId");

            if (producerHost == null || producerApp == null || createTime == null || orgId == null) {
                log.error("Failed to construct new task upon these args:\n" + taskArgs.toString());
            } else {
                /**
                 * Generate a new ID
                 */
                id = generateTaskId(taskQueue, producerHost, producerApp, createTime, orgId, sn);
                taskArgs.putString("_id", id);
            }
        }

        return id;
    }

    /**
     * Constructor that takes a JsonObject (called by workers)
     */
    public SxaTaskBase(JsonObject taskArgs) throws Exception {
        /**
         * Validate the arguments
         */
        String id = validateTaskArgs(this.getTaskQueueName(), taskArgs);
        if (id == null) {
            log.error("Failed to construct new task upon these args:\n" + taskArgs.toString());
            throw new Exception("Missing required common task argument(s)!");
        }

        /**
         * Build a JSON Object to represent the task
         */
        taskJsonObject = new JsonObject();
        taskJsonObject.putString("queue", getTaskQueueName());
        taskJsonObject.putString("type", getTaskName());
        taskJsonObject.putObject("args", taskArgs);
        taskJsonObject.putString("startTime", new Date().toString());
        taskJsonObject.putString("state", TaskConstants.TASK_STATE_IN_PROGRESS);
        taskJsonObject.putString("_id", id);

        this.taskArgs = taskArgs;
        taskString = taskJsonObject.toString();
    }

    /**
     * Constructor that takes producer fields/values (usually called by producer)
     */
    public SxaTaskBase(
            String producerHost,
            String producerApp,
            String createTime,
            String username,
            Number orgId,
            Number sn) throws Exception {
        this(new JsonObject()
                .putString("producerHost", producerHost)
                .putString("producerApp", producerApp)
                .putString("createTime", createTime)
                .putNumber("orgId", orgId)
                .putNumber("sn", sn));

        // Set State to Pending
        setState(TaskConstants.TASK_STATE_PENDING);

        // Check for optional username
        if (username != null) {
            taskJsonObject.putString("username", username);
        }
    }

    /**
     * Set the task state
     * @param state
     */
    public void setState(String state) {
        taskJsonObject.putString("state", state);
    }

    /**
     * Get This Task's UUID (universally unique identifier)
     */
    public String getId() {
        return taskJsonObject.getString("_id");
    }

    /**
     * Get the update strings
     */
    public String getUpdatesString() {
        if (taskJsonObject.getArray("updates") != null) {
            return taskJsonObject.getArray("updates").toString();
        } else {
            return "";
        }
    }

    /**
     * Return the type/name (i.e. "class" in Resque term) of the task
     *
     * @return
     */
    public abstract String getTaskName();

    /**
     * Return the name of the task queue that contains this type of tasks
     * @return
     */
    public abstract String getTaskQueueName();

    /**
     * toString.
     *
     * Currently only returns the task id and state.
     */
    @Override
    public String toString() {
        return toShortString();
    }

    /**
     * Returns a short string with the task id and state.
     */
    public String toShortString() {
        return getId() + "(" + taskJsonObject.getString("state") + ")";
    }

    /**
     * Build a raw task (a JsonObject)
     *
     * @param taskArgs
     */
    public static JsonObject buildRawTask(
            String taskQueueName,
            String taskName,
            JsonObject taskArgs){
        /**
         * Build a JSON Object to represent the task
         */
        return new JsonObject()
                .putString("queue", taskQueueName)
                .putString("type", taskName)
                .putObject("args", taskArgs);
    }
}
