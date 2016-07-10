package vertx2.taskmgmt.worker;

import vertx2.taskmgmt.TaskConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

import java.util.List;

/**
 * Project:  SXA Task Management
 *
 * Task Poller Verticle which polls new tasks from Redis and forwards them to event bus.
 *
 * @author: jqin
 */
public class TaskPollerVertice extends WorkerVertice {
    private Logger log = LoggerFactory.getLogger(TaskPollerVertice.class);

    /**
     * Jesque Worker Instance
     */
    SxaTaskWorkerImpl poller = null;
    
    /**
     *  Poll Timer Id
     */
    Long timerId = null;

    /**
     * Start the ACS Jesque Worker Vertice
     */
    @Override
    public void start() {
        /**
         * Extract Vertice Config which contains the list of task queue name(s)
         */
        List<? extends AbstractSxaTaskImpl> tasks = WorkerUtils.getTaskTypes(container);
        if (tasks == null) {
            try {
                throw new Exception("Invalid Config Args!");
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }

        /**
         * Debug log
         */
        log.info("Starting poller vertice that will process the following task types/queues:");
        for (AbstractSxaTaskImpl task : tasks) {
            log.info("queue: " + task.getTaskQueueName() + ", task type: " + task.getTaskName()
                    + ", class: " + task.getClass().getName());
        }
        log.info("MaxOutstandingTasks: " + WorkerUtils.getMaxOutstandingTasks(container));

        try {
            /**
             * Create a poller POJO (as a "worker")
             */
            poller = new SxaTaskWorkerImpl(tasks, this, WorkerUtils.getMaxOutstandingTasks(container));

            /**
             * Register event bus handlers for jobs/tasks processing results
             */
            for (AbstractSxaTaskImpl task : tasks) {
                log.info("Registering event handler for task queue " + task.getTaskQueueName() + "...");
                vertx.eventBus().registerLocalHandler(
                        TaskConstants.VERTX_ADDRESS_TASK_RESULTS + "." + task.getTaskQueueName(),
                        poller.jobResultHandler
                );
            }

            /**
             * Initialize the poller
             */
            log.info("Starting poller..");
            poller.start();

            /**
             * Start a 500ms timer to do the polling
             */
            log.info("Starting polling timer..");
            timerId = vertx.setPeriodic(500, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    poller.poll();
                }
            });
        } catch (Exception ex) {
            log.error("Caught exception " + ex.getMessage() + "!");
            ex.printStackTrace();
        }

        log.info("Deployment completed.");
    }

    /**
     * Stop all the workers when quitting
     */
    @Override
    public void stop() {
        log.info("Stopping Jesque Task Poller (" + poller.getName() + ") Vertice...");

        /**
         * Un-Register event bus handlers for jobs/tasks processing results
         */
        if (poller != null) {
            if (poller.tasks != null) {
                for (AbstractSxaTaskImpl task : poller.tasks) {
                    vertx.eventBus().unregisterHandler(
                            TaskConstants.VERTX_ADDRESS_TASK_RESULTS + "." + task.getTaskQueueName(),
                            poller.jobResultHandler
                    );
                }
            }

            poller.shutdown();
        }

        if (timerId != null) {
            vertx.cancelTimer(timerId);
        }
    }
}
