package com.calix.sxa.taskmgmt;

import com.calix.sxa.VertxConfigProperties;
import com.calix.sxa.VertxConstants;
import com.calix.sxa.VertxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.platform.Verticle;

/**
 * Project:  sxa-cc-parent
 *
 * @author: jqin
 */
public class TaskMgmtMainVertice extends Verticle {
    private static final Logger log = LoggerFactory.getLogger(TaskMgmtMainVertice.class.getName());

    /**
     * Start the Vertice
     */
    public void start() {
        log.info("\nTask Mgmt Server Instance " + VertxUtils.getLocalHostname() + " is starting up..\n");

        /**
         * Configure and Deploy VERT.X Mongo Persistor module
         */
        container.deployModule(
                VertxConstants.MOD_MONGO_PERSISTOR,
                VertxConstants.MOD_MONGO_PERSISTOR_CONFIG,
                mongodbDeploymentAsyncHandler
        );
    }

    /**
     * Async Result Handler for deploying MongoDB Persistor
     */
    AsyncResultHandler<String> mongodbDeploymentAsyncHandler = new AsyncResultHandler<String>() {
        public void handle(AsyncResult<String> asyncResult) {
            if (asyncResult.succeeded()) {
                log.info("The MongoDB verticle has been successfully deployed");
                log.info("Deploying Redis Verticle... (redis server is @ " +
                        VertxConfigProperties.redisHost + ":" + VertxConfigProperties.redisPort + ")");

                /**
                 * Create a Redis client and Deploy Redis Module
                 *
                 * The Jesque Worker Verticle(s) are deployed after the Redis Module is successfully deployed.
                 */
                container.deployModule(
                        VertxConstants.MOD_REDIS,
                        VertxConstants.MOD_REDIS_CONFIG,
                        modRedisDeploymentAsyncHandler
                );
            } else {
                log.error("Failed to deploy the MongoDB verticle !!!");
                asyncResult.cause().printStackTrace();

                /**
                 * Publish Server Startup Failure Event
                 */
                vertx.eventBus().publish(VertxConstants.VERTX_ADDRESS_SERVER_EVENTS,
                        "Task Mgmt Server Instance " + VertxUtils.getLocalHostname() + " failed to start up!");
            }
        }
    };

    /**
     * Async Result Handler for deploying Redis Module
     */
    AsyncResultHandler<String> modRedisDeploymentAsyncHandler = new AsyncResultHandler<String>() {
        public void handle(AsyncResult<String> asyncResult) {
            if (asyncResult.succeeded()) {
                log.info("The Redis verticle has been successfully deployed");

                /**
                 * Start Task Queue REST API Service
                 */
                log.info("Deploying Task Mgmt Server Verticle(s)...");
                container.deployVerticle(TaskMgmtRestWsVertice.class.getName());
            } else {
                log.error("Failed to deploy the Redis verticle !!!");
                asyncResult.cause().printStackTrace();

                /**
                 * Publish Server Startup Failure Event
                 */
                vertx.eventBus().publish(VertxConstants.VERTX_ADDRESS_SERVER_EVENTS,
                        "Task Mgmt Server Instance " + VertxUtils.getLocalHostname() + " failed to start up!");
            }
        }
    };

    public void stop() {
        /**
         * Publish Server Departure Event
         */
        vertx.eventBus().publish(VertxConstants.VERTX_ADDRESS_SERVER_EVENTS,
                "Task Mgmt Server Instance " + VertxUtils.getLocalHostname() + " is shutting down...");
   }
}