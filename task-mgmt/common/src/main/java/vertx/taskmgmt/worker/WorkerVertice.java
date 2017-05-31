package vertx.taskmgmt.worker;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import vertx.taskmgmt.TaskConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * Project:  SXA Task Management
 *
 * The Worker Verticle.
 *
 * @author: ronang
 */
public class WorkerVertice extends AbstractVerticle {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Jesque Worker Instance
     */
    public SxaTaskWorkerImpl worker = null;

    /**
     * Start the ACS Jesque Worker Vertice
     */
    public void start() {

        // Initialize the logger
        log.info("Starting up worker vertice...");

        /**
         * Deploy a Task Poller vertice
         */
        //container.deployVerticle(TaskPollerVertice.class.getName(), container.config());
        DeploymentOptions options = new DeploymentOptions().setConfig(config());
        vertx.deployVerticle(TaskPollerVertice.class.getName(),options);

        try {
            /**
             * Extract Vertice Config which contains the list of task queue name(s)
             */
            List<? extends AbstractSxaTaskImpl> tasks = WorkerUtils.getTaskTypes(config());
            if (tasks == null) {
                try {
                    throw new Exception("Invalid Config Args!");
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }

            /**
             * Register event handler for new jobs/tasks
             */
            for (final AbstractSxaTaskImpl task : tasks) {
                vertx.eventBus().consumer(
                        TaskConstants.VERTX_ADDRESS_NEW_TASKS + "." + task.getTaskQueueName() + "." + task.getTaskName(),
                        new NewJobHandler(task.getTaskQueueName())
                );
            }

            /**
             * Create a worker POJO for materializing jobs
             */
            worker = new SxaTaskWorkerImpl(tasks, this, WorkerUtils.getMaxOutstandingTasks(config()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        log.info("Deployment completed.");
    }

    /**
     * New Job Handler
     */
    public class NewJobHandler implements Handler<Message<JsonObject>> {
        /**
         * Task Queue Name
         */
        String queueName;

        /**
         * Constructor.
         *
         * @param queueName
         */
        public NewJobHandler(String queueName) {
            this.queueName = queueName;
        }

        /**
         * Actual Handler
         *
         * @param jsonObjectMessage
         */
        @Override
        public void handle(Message<JsonObject> jsonObjectMessage) {
            // The body is a Jesque Job
            JsonObject body = jsonObjectMessage.body();

            AbstractSxaTaskImpl taskImpl;
            try {
                taskImpl = worker.materializeJob(queueName, body);
                taskImpl.setWorker(worker);
                taskImpl.persistTask();
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Failed to materialize job due to " + e.getMessage() + ":\n" + body.encodePrettily());
                JsonObject result = SxaTaskWorkerImpl.buildJobProcessResult(
                        false,
                        "Failed to materialize job!",
                        body,
                        queueName);
                vertx.eventBus().send(TaskConstants.VERTX_ADDRESS_TASK_RESULTS + "." + queueName, result);
                return;
            }

            log.info("Materialized a new " + taskImpl.getTaskName());
            taskImpl.run();
        }
    }

    /**
     * Stop all the workers when quitting
     */
    public void stop() {
        log.info("Stopping Jesque Worker " + worker.getName() + " ...");

        /**
         * Register event handler for new jobs/tasks
         */
        for (final AbstractSxaTaskImpl task : worker.tasks) {
            vertx.eventBus().consumer(
                    TaskConstants.VERTX_ADDRESS_NEW_TASKS + "." + task.getTaskQueueName(),
                    new NewJobHandler(task.getTaskQueueName())
            );
        }

        worker.shutdown();
    }
}
