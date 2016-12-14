package vertx2.taskmgmt.worker;

import vertx2.VertxConstants;
import vertx2.taskmgmt.TaskConstants;
import vertx2.taskmgmt.TaskUtils;
import io.vertx.java.redis.RedisClient;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.Job;
import net.greghaines.jesque.utils.JedisUtils;
import net.greghaines.jesque.utils.ResqueConstants;
import net.greghaines.jesque.worker.JobFactory;
import net.greghaines.jesque.worker.WorkerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static net.greghaines.jesque.utils.ResqueConstants.*;
import static net.greghaines.jesque.worker.JobExecutor.State.RUNNING;
import static net.greghaines.jesque.worker.WorkerEvent.WORKER_STOP;

/**
 * Project:  SXA Task Management
 *
 * This class implements the Jesque Worker Interface by simply polling jobs from Redis and forwarding the jobs to
 * Vertx Event Bus.
 *
 * TODO: Add periodical heartbeats.
 *
 * @author: ronang
 */
public class SxaTaskWorkerImpl extends WorkerImpl{
    private static Logger log = LoggerFactory.getLogger(SxaTaskWorkerImpl.class);

    /**
     * VertX POJO
     */
    public Vertx vertx = null;

    /**
     * Vert.x Event Bus
     */
    public EventBus eventBus = null;

    /**
     * Async Redis Client
     */
    public RedisClient redisClient = null;

    /**
     * List of expected task types
     */
    List<? extends AbstractSxaTaskImpl> tasks;

    /**
     * Max # of outstanding Jobs (default to 1)
     */
    int maxOutstandingJobs = 1;

    /**
     * # of outstanding Jobs
     */
    int outstandingJobs = 0;

    /**
     * Job Result Handler
     */
    public Handler<Message<JsonObject>> jobResultHandler;

    /**
     * Creates a new WorkerImpl, which creates it's own connection to Redis
     * using values from the config. The worker will only listen to the supplied
     * queues and only execute jobs that are in the supplied job types.
     *
     * @param config     used to create a connection to Redis and the package prefix
     *                   for incoming jobs
     * @param queues     the list of queues to poll
     * @param jobFactory the job factory that materializes the jobs
     * @throws IllegalArgumentException if the config is null, if the queues is null, or if the
     *                                            jobTypes is null or empty
     */
    public SxaTaskWorkerImpl(Config config, Collection<String> queues, JobFactory jobFactory) {
        super(config, queues, jobFactory);
    }

    /**
     * Constructor that requires Vert.x Event Bus.
     *
     * @param workerVertice
     * @param tasks         A Collection of instances of all the task classes that can be processed
     *                      by this worker instance (one instance per class)
     * @param maxOutstandingJobs    Max # of outstanding Jobs
     */
    public SxaTaskWorkerImpl(
            List<? extends AbstractSxaTaskImpl> tasks,
            WorkerVertice workerVertice,
            int maxOutstandingJobs) {
        /**
         * Initialize with our Jesque Config
         */
        super(TaskUtils.jesqueConfig,
                buildTaskQueueNameCollection(tasks),
                new SxaTaskMapBasedJobFactory(buildTaskTypes(tasks), workerVertice));

        this.tasks = tasks;
        this.vertx = workerVertice.getVertx();
        this.eventBus = vertx.eventBus();
        this.maxOutstandingJobs = maxOutstandingJobs;
        this.jobResultHandler = new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> jsonObjectMessage) {
                /**
                 * Analyze the result
                 */
                handleResult(jsonObjectMessage.body());

                if (outstandingJobs > 0) {
                    outstandingJobs --;
                }
            }
        };
    }


    /**
     * Build Task Queue Name Collection
     *
     * @param tasks
     * @return
     */
    private static Collection<String> buildTaskQueueNameCollection(List<? extends AbstractSxaTaskImpl> tasks) {
        Collection<String> queues = new ArrayList<String>(tasks.size());
        for (AbstractSxaTaskImpl task : tasks) {
            queues.add(task.getTaskQueueName());
        }
        return  queues;
    }

    /**
     * Build a map for all the task types.
     *
     * @param tasks
     * @return
     */
    private static Map<String, Class<?>> buildTaskTypes(List<? extends AbstractSxaTaskImpl> tasks) {
        Map<String, Class<?>> taskTypes = new HashMap<String, Class<?>>(tasks.size());
        for (AbstractSxaTaskImpl task : tasks) {
            taskTypes.put(task.getTaskName(), task.getClass());
        }

        return taskTypes;
    }

    /**
     * Starts this worker. Registers the worker in Redis and begins polling the
     * queues for jobs.
     */
    public void start() {
        /**
         * The following section is copied from Jesque
         */
        this.state.set(RUNNING);
        renameThread("RUNNING");
        this.jedis.sadd(key(WORKERS), this.getName());
        this.jedis.set(key(WORKER, this.getName(), STARTED), new SimpleDateFormat(DATE_FORMAT).format(new Date()));

        poll();
    }
    
    /**
     * Redis Async Handler
     */
    AsyncResultHandler<String> modRedisDeploymentAsyncHandler = new AsyncResultHandler<String>() {
        public void handle(AsyncResult<String> asyncResult) {
            if (asyncResult.succeeded()) {
                log.info("The Redis verticle has been successfully deployed");

                redisClient =  new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);
            } else {
                log.error("Failed to deploy the Redis verticle !!!");
                asyncResult.cause().printStackTrace();
            }
        }
    };

    /**
     * Initialize Async Redis Client (not the jedis client instance)
     */
    public void initRedisClient(Container container) {
        
    	container.deployModule(
                VertxConstants.MOD_REDIS,
                VertxConstants.MOD_REDIS_CONFIG,
                modRedisDeploymentAsyncHandler);
    }

    /**
     * Polls the queues for jobs and executes them.
     */
    @Override
    protected void poll() {
        /**
         * Do not poll if there are too many outstanding jobs
         */
        if (outstandingJobs >= maxOutstandingJobs) {
            return;
        }

        String curQueue = null;
        try {
            curQueue = this.queueNames.poll(EMPTY_QUEUE_SLEEP_TIME, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (curQueue != null) {
            this.queueNames.add(curQueue); // Rotate the queues
            // checkPaused();
            // Might have been waiting in poll()/checkPaused() for a while
            if (RUNNING.equals(this.state.get())) {
                final String payload = pop(curQueue);
                if (payload != null && payload.length() > 2) {
                    log.info("poll(): Received a new job, payload: " + payload);

                    /**
                     * Convert payload to Vert.X JSON Object
                     */
                    try {
                        final JsonObject jsonObject = new JsonObject(payload);
                        final Job job = getJobFromJsonObject(jsonObject);
                        if (job != null) {
                            /**
                             * Send the new Job to the Vert.x event bus
                             */
                            eventBus.send(TaskConstants.VERTX_ADDRESS_NEW_TASKS + "." + curQueue + "." + job.getClassName(), jsonObject);

                            /**
                             * Update the # of outstandingJobs
                             */
                            outstandingJobs ++;
                        }
                    } catch (Exception ex) {
                        log.error("Caught exception " + ex.toString() + " while processing payload \n" + payload);
                    }
                }
            }
        }
    }

    /**
     * Materializes a job.
     *
     * Caller by worker vertice when converting event bus message to taskWorkerImpl instance.
     *
     * @param queueName
     * @param payload       The raw payload from Redis
     *
     * @return the materialized job which is an instance of AbstractSxaTaskImpl
     * @throws Exception if there was an exception creating the object
     */
    public AbstractSxaTaskImpl materializeJob(
            String queueName,
            JsonObject payload) throws Exception {
        return (AbstractSxaTaskImpl)this.getJobFactory().materializeJob(getJobFromJsonObject(payload));
    }

    /**
     * Convert payload to Vert.X JSON Object
     *
     * @param jsonObject
     *
     * @return A Jesque Job
     */
    public Job getJobFromJsonObject(JsonObject jsonObject) {
        String className = jsonObject.getString("class");
        if (className == null) {
            log.error("No class field found!");
        } else {
            JsonArray args = jsonObject.getArray("args");
            if (args == null || args.size() < 1) {
                log.error("No args found!");
            } else {
                return new Job(className, args.get(0));
            }
        }

        return null;
    }

    /**
     * Process the async job processing result received from event bus
     * @param result
     */
    public void handleResult(JsonObject result) {
        boolean succeeded = result.getBoolean("succeeded", true);
        Job job = getJobFromJsonObject(result.getObject("job"));
        if (succeeded) {
            success(job, null, null, null);
        } else {
            Exception exception = new Exception(result.getString("failure"));
            failure(exception, job, result.getString("queue"));
        }
    }

    /**
     * Build a Job Process result (to be sent over the event bus).
     * @param succeeded
     * @param failure
     * @param jobJsonObject
     * @param queue
     * @return
     */
    public static JsonObject buildJobProcessResult(
        boolean succeeded,
        String failure,
        JsonObject jobJsonObject,
        String queue
    ) {
        JsonObject output = new JsonObject();
        output.putBoolean("succeeded", succeeded);
        if (failure != null) {
            output.putString("failure", failure);
        }
        if (jobJsonObject != null) {
            output.putObject("job", jobJsonObject);
        }
        if (queue != null) {
            output.putString("queue", queue);
        }

        return output;
    }

    /**
     * Shutdown the worker and remove all in-flight jobs
     */
    public void shutdown() {
        try {
            /*
            String redisReply = "";
            for (AbstractSxaTaskImpl task : tasks) {
                while (true) {
                    redisReply = this.jedis.lpop(key(INFLIGHT, this.getName(), task.getTaskQueueName()));
                    if (redisReply.equals("nil")) {
                        break;
                    }
                }
            }
            */
            renameThread("STOPPING");
            this.listenerDelegate.fireEvent(WORKER_STOP, this, null, null, null, null, null);
            log.info("Stopping " + this.getName());
            this.jedis.srem(key(WORKERS), this.getName());
            this.jedis.del(
                    key(WORKER, this.getName()),
                    key(WORKER, this.getName(), STARTED),
                    key(STAT, FAILED, this.getName()),
                    key(STAT, PROCESSED, this.getName())
            );
            this.jedis.quit();
            end(true);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Remove a job from the given queue.
     * @param curQueue the queue to remove a job from
     * @return a JSON string of a job or null if there was nothing to de-queue
     */
    protected String pop(final String curQueue) {
        final String key = key(ResqueConstants.QUEUE, curQueue);
        String payload = null;
        // If a delayed queue, peek and remove from ZSET
        if (JedisUtils.isDelayedQueue(this.jedis, key)) {
            final long now = System.currentTimeMillis();
            // Peek ==> is there any item scheduled to run between -INF and now?
            final Set<String> payloadSet = this.jedis.zrangeByScore(key, -1, now, 0, 1);
            if (payloadSet != null && !payloadSet.isEmpty()) {
                final String tmp = payloadSet.iterator().next();
                // Try to acquire this job
                if (this.jedis.zrem(key, tmp) == 1) {
                    payload = tmp;
                }
            }
        } else { // If not a delayed Queue, then use RPOP
            payload = lpoplpush(key, key(INFLIGHT, this.getName(), curQueue));
        }
        return payload;
    }
}
