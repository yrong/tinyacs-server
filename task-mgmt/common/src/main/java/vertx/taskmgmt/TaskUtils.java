package vertx.taskmgmt;

import vertx.VertxConfigProperties;
import vertx.taskmgmt.model.SxaTaskBase;
import io.vertx.java.redis.RedisClient;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.ConfigBuilder;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.ResqueConstants;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Date;

/**
 * Project:  cwmp-parent
 *
 * Jesque Utils.
 *
 * @author: ronang
 */
public class TaskUtils {
    /**
     * Default Jesque/Redis Config
     */
    public static final Config jesqueConfig = new Config(
            VertxConfigProperties.redisHost,
            VertxConfigProperties.redisPort,
            VertxConfigProperties.redisTimeout,
            VertxConfigProperties.redisPassword,
            ConfigBuilder.DEFAULT_NAMESPACE,
            VertxConfigProperties.redisDbIndex
    );

    /**
     * Get Task Service URL
     */
    public static String getTaskServiceUrl() {
        return ConfigProperties.taskServiceUrl;
    }

    /**
     * Post a new task to the task service via HTTP.
     *
     * To be called by the clients.
     *
     * @param taskQueue
     * @param taskType
     * @param taskArgs
     * @param responseHandler   A Handler that takes the String ID as input.
     *                          If id string is null, that means operation failed.
     * @param timeoutMs
     */
    public void postNewTask(
            String taskQueue,
            String taskType,
            JsonObject taskArgs,
            Handler<String> responseHandler,
            long timeoutMs) {
        /**
         * TODO
         */
    }

    /**
     * Maintain a static sequential task id
     */
    private static Integer taskSn = 0;
    public static Number getNextTaskSn() {
        synchronized (taskSn) {
            return ++ taskSn;
        }
    }

    /**
     * Convert a raw task (a JSON object) into an SXA Jesque Task/Job.
     *
     * @param rawTask
     */
    public static JsonObject convertRawTaskToJesqueTask(JsonObject rawTask) throws Exception {
        String taskQueue = rawTask.getString("queue");
        String taskType = rawTask.getString("type");
        JsonObject taskArgs =  rawTask.getObject("args");

        /**
         * Validate input
         */
        if (taskQueue == null || taskArgs == null || taskType == null) {
            throw new Exception("Illegal Parameter!");
        }

        /**
         * Set "createTime"
         */
        taskArgs.putString("createTime", new Date().toString());

        /**
         * Move "producerHost"/"producerApp"/"orgId"/"sn" to args if found at top level
         */
        TaskUtils.moveJsonStringElement("parentTask", rawTask, taskArgs);
        TaskUtils.moveJsonStringElement("producerHost", rawTask, taskArgs);
        TaskUtils.moveJsonStringElement("producerApp", rawTask, taskArgs);
        TaskUtils.moveJsonNumberElement("orgId", rawTask, taskArgs);
        TaskUtils.moveJsonNumberElement("sn", rawTask, taskArgs);

        /**
         * Get a new taskId
         */
        String taskId = SxaTaskBase.validateTaskArgs(taskQueue, taskArgs);
        if (taskId == null) {
            throw new Exception("Illegal Parameter!");
        }

        /**
         * Build a Jesque Job Json Object
         */
        return new JsonObject()
                .putString("class", taskType)
                .putString("_id", taskId)
                .putArray("args", new JsonArray().add(taskArgs));
    }

    /**
     * Enqueue a new task (a JSON object) into Resque via async Redis Client.
     *
     * @param queueName
     * @param task
     * @param redisClient
     * @param handler
     */
    public static void enqueueTask(
            String queueName,
            JsonObject task,
            RedisClient redisClient,
            Handler<Message<JsonObject>> handler) {
        redisClient.sadd(getJesqueKey(ResqueConstants.QUEUES), queueName);
        redisClient.rpush(
                getJesqueKey(ResqueConstants.QUEUE, queueName),
                task.toString(),
                handler);
    }

    /**
     * Enqueue a new delayed task (a JSON object) into Resque via async Redis Client.
     *
     * @param queueName
     * @param task
     * @param delay         (in # of seconds)
     * @param redisClient
     * @param handler
     */
    public static void delayedEnqueueTask(
            String queueName,
            JsonObject task,
            long delay,
            RedisClient redisClient,
            Handler<Message<JsonObject>> handler) {
        redisClient.sadd(getJesqueKey(ResqueConstants.QUEUES), queueName);
        redisClient.zadd(
                getJesqueKey(ResqueConstants.QUEUE, queueName),
                delay,
                task.toString(),
                handler);
    }

    /**
     * Builds a namespaced Redis key with the given arguments.
     *
     * @param parts
     *            the key parts to be joined
     * @return an assembled String key
     */
    public static String getJesqueKey(final String... parts) {
        return JesqueUtils.createKey(ConfigBuilder.DEFAULT_NAMESPACE, parts);
    }

    /**
     * Move a JSON String element from one JSON Object to another.
     *
     * @param name      Name of the string element
     * @param from
     * @param to
     */
    public static void moveJsonStringElement(String name, JsonObject from, JsonObject to) {
        if (name == null || from == null || to == null) {
            return;
        }

        String value = from.getString(name);
        if (value != null) {
            from.removeField(name);
            to.putString(name, value);
        }
    }

    /**
     * Move a JSON Number element from one JSON Object to another.
     *
     * @param name      Name of the string element
     * @param from
     * @param to
     */
    public static void moveJsonNumberElement(String name, JsonObject from, JsonObject to) {
        if (name == null || from == null || to == null) {
            return;
        }

        Number value = from.getNumber(name);
        if (value != null) {
            from.removeField(name);
            to.putNumber(name, value);
        }
    }

    /**
     * Convert # of milli seconds into a readable string.
     */
    public static String msToString(long ms) {
        String result = "";

        long rest = ms;
        if (ms > TaskConstants.ONE_DAY_IN_MS) {
            result += (ms / TaskConstants.ONE_DAY_IN_MS) + " day(s)";
            rest = ms % TaskConstants.ONE_DAY_IN_MS;
        }
        if (rest > TaskConstants.ONE_HOUR_IN_MS) {
            result += " " + (rest / TaskConstants.ONE_HOUR_IN_MS) + " hour(s)";
            rest = ms % TaskConstants.ONE_HOUR_IN_MS;
        }
        if (rest > TaskConstants.ONE_MINUTE_IN_MS) {
            result += " " + (rest / TaskConstants.ONE_MINUTE_IN_MS) + " minute(s)";
            rest = ms % TaskConstants.ONE_MINUTE_IN_MS;
        }
        if (rest > TaskConstants.ONE_SECOND_IN_MS) {
            result += " " + (rest / TaskConstants.ONE_SECOND_IN_MS) + " second(s)";
            rest = ms % TaskConstants.ONE_SECOND_IN_MS;
        }
        if (rest > 0 || ms == 0) {
            result += " " + rest  + " milli-second(s)";
        }
        return  result;
    }
}
