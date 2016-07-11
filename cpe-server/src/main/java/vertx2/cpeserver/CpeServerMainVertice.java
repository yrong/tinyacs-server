package vertx2.cpeserver;

import vertx2.VertxConstants;
import vertx2.VertxDeployUtils;
import vertx2.VertxUtils;
import vertx2.connreq.ConnectionRequestManagerVertice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * Project:  SXA CC CPE Server
 *
 * @author: ronyang
 */
public class CpeServerMainVertice extends Verticle {
    private static final Logger log = LoggerFactory.getLogger(CpeServerMainVertice.class.getName());

    /**
     * Start the Vertice
     */
    public void start() {
        log.info(VertxUtils.highlightWithHashes("Number of CPU Cores: " + VertxUtils.getNumberOfCpuCores()));

        /**
         * Print JVM Heap Info
         */
        VertxUtils.displayJvmHeapInfo(vertx);

        /**
         * Print Build Info
         */
        VertxUtils.displayBuildInfo(vertx);

        /**
         * Build the list of sub modules/vertices to be deployed
         */
        VertxDeployUtils.Deployments deployments = new VertxDeployUtils.Deployments();
        // Add Mod MongoDB
        deployments.add(VertxConstants.MOD_MONGO_PERSISTOR_DEPLOYMENT);
        // Add Mod MongoDB-GridFS
        deployments.add(VertxConstants.MOD_MONGO_GRIDFS_DEPLOYMENT);
        // Add Mod Redis
        deployments.add(VertxConstants.MOD_REDIS_DEPLOYMENT);
        // Add Connection-Request Worker Vertice
        deployments.add(VertxUtils.buildNewDeployment(ConnectionRequestManagerVertice.class.getName(), null));
        // Add multiple TR-069 Server Vertice Instances
        for (int i = 0; i < CpeServerConstants.NUMBER_OF_SESSION_VERTICES; i ++) {
            deployments.add(
                    VertxUtils.buildNewDeployment(
                            CpeServerTR069SessionVertice.class.getName(),
                            new JsonObject().putNumber(CpeServerConstants.FIELD_NAME_VERTICE_INDEX, i)
                    )
            );
        }
        // Add Multiple HTTP Load Balancer Vertices
        deployments.add(
                VertxUtils.buildNewDeployment(
                        CpeServerHttpLoadBalancerVertice.class.getName(),
                        null,
                        CpeServerConstants.NUMBER_OF_SESSION_VERTICES
                )
        );

        /**
         * Start all CPE server specific items after mongo persistor has been deployed
         */
        deployments.finalHandler = new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> deployResult) {
                if (deployResult.succeeded()) {
                    log.info("All external and sub modules have been successfully installed.");
                }
            }
        };

        /**
         * Start the Deployments
         */
        VertxUtils.deployModsVertices(container, deployments);

        /**
         * Start a 1-hour timer to display JVM Heap Info
         */
        vertx.setPeriodic(3600000, new Handler<Long>() {
            @Override
            public void handle(Long event) {
                VertxUtils.displayJvmHeapInfo(vertx);
            }
        });
    }
}