package vertx2.acs.worker.workflow;

import vertx2.VertxConstants;
import vertx2.VertxUtils;
import vertx2.cache.GroupCache;
import vertx2.model.CpeGroup;
import vertx2.util.AcsConstants;
import vertx2.taskmgmt.worker.WorkerVertice;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Project:  cwmp
 *
 * Extending the default (Task) Worker Vertice to add custom workflow logic.
 *
 * @author: ronyang
 */
public class ActiveWorkflowWorkerVertice extends WorkerVertice {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * An internal hash map that contains all the workflow tasks that this worker is managing.
     */
    public HashMap<String, ActiveWorkflowTaskWorker> workflowTaskHashMap = new HashMap<>();

    // Async Redis Client Instance
    public RedisClient redisClient;

    // CPE Group Cache
    public GroupCache groupCache;

    /**
     * Override the default start() method to register a handler
     */
    @Override
    public void start() {
        super.start();

        /**
         * Initialize Redis Client
         */
        redisClient = new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);

        /**
         * Initialize Group Cache
         */
        groupCache = new GroupCache(
                vertx,
                AcsConstants.VERTX_ADDRESS_ACS_GROUP_CRUD,
                CpeGroup.DB_COLLECTION_NAME,
                CpeGroup.class.getSimpleName()
        );
    }

    /**
     * Override the default stop() method to unregister a handler
     */
    @Override
    public void stop() {
        super.stop();

        if (workflowTaskHashMap.size() > 0) {
            log.info(VertxUtils.highlightWithHashes(
                    "There are " + workflowTaskHashMap.size() + " in-progress task(s).")
            );

            /**
             * TODO: re-enqueue in-progress tasks back to Redis
             */
            log.info(VertxUtils.highlightWithHashes("TODO: re-enqueue in-progress tasks back to Redis!"));
        }
    }
}
