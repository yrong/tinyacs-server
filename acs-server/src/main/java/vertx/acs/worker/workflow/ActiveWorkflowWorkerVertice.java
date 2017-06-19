package vertx.acs.worker.workflow;

import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import vertx.VertxConfigProperties;
import vertx.VertxConstants;
import vertx.VertxUtils;
import vertx.cache.GroupCache;
import vertx.model.CpeGroup;
import vertx.util.AcsConstants;
import vertx.taskmgmt.worker.WorkerVertice;
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
        RedisOptions options = new RedisOptions().setHost(VertxConfigProperties.redisHost).setPort(VertxConfigProperties.redisPort);
        redisClient = RedisClient.create(vertx,options);


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
