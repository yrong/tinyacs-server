package vertx.cpeserver;

import io.vertx.core.*;
import vertx.VertxConstants;
import vertx.VertxUtils;
import vertx.connreq.ConnectionRequestManagerVertice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

/**
 * Project:  CPE Server
 *
 * @author: ronyang
 */
public class CpeServerMainVertice extends AbstractVerticle {
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

        vertx.deployVerticle(ConnectionRequestManagerVertice.class.getName());
        // Add multiple TR-069 Server Vertice Instances
        for (int i = 0; i < CpeServerConstants.NUMBER_OF_SESSION_VERTICES; i ++) {
            DeploymentOptions options = new DeploymentOptions();
            options.setConfig(new JsonObject().put(CpeServerConstants.FIELD_NAME_VERTICE_INDEX, i));
            vertx.deployVerticle(CpeServerTR069SessionVertice.class.getName(),options);
        }
        // Add Multiple HTTP Load Balancer Vertices
//        DeploymentOptions options = new DeploymentOptions().setInstances(CpeServerConstants.NUMBER_OF_SESSION_VERTICES);
        vertx.deployVerticle(CpeServerHttpLoadBalancerVertice.class.getName());


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