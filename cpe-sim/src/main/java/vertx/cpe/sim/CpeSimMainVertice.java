package vertx.cpe.sim;

import vertx.VertxConstants;
import vertx.VertxDeployUtils;
import vertx.VertxUtils;
import vertx.util.CpeDataModelMgmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;
import io.vertx.platform.Verticle;

/**
 * Project:  CPE Simulator
 *
 * @author: ronyang
 */
public class CpeSimMainVertice extends Verticle {
    private final Logger log = LoggerFactory.getLogger(CpeSimMainVertice.class.getName());

    /**
     * Event Bus
     */
    EventBus eventBus;

    /**
     * Vertice Start Time
     */
    public static long startTime = System.currentTimeMillis();

    /**
     * Start the Vertice
     */
    public void start() {
        log.info("CPE Simulator is starting up on port " + CpeSimConstants.HTTP_SERVICE_REQ_PORT + "\n");

        log.info(VertxUtils.highlightWithHashes("ACS URL: " + CpeSimConstants.ACS_URL));

        /**
         * Save event bus
         */
        eventBus = vertx.eventBus();


        /**
         * Initialize Data Models
         */
        CpeDataModelMgmt.init(vertx, "tr_data_models/");

        /**
         * Read the default CPE data
         */
        CpeSimUtils.initDefaultCpeData(vertx);

        /**
         * Deploy Mongo Persistor
         */
        /**
         * Build the list of sub modules/vertices to be deployed
         */
        VertxDeployUtils.Deployments deployments = new VertxDeployUtils.Deployments();
        deployments.add(VertxConstants.MOD_MONGO_PERSISTOR_DEPLOYMENT);
        VertxUtils.deployModsVertices(container, deployments);

        /**
         * Deploy multiple Session Vertice based on the # of CPE Cores
         */
        int numberOfVertices = Runtime.getRuntime().availableProcessors();
        if (numberOfVertices > 32) {
            numberOfVertices = 32;
        }
        log.info("Starting " + numberOfVertices + " session vertices.");
        container.deployVerticle(CpeSimSessionVertice.class.getName(), numberOfVertices);

        /**
         * Deploy multiple Diag worker vertices
         */
        log.info("Starting " + numberOfVertices + " diag worker vertices.");
        container.deployVerticle(CpeDiagWorkerVertice.class.getName(), numberOfVertices);

        /**
         * Start the server
         */
        HttpServer server = vertx.createHttpServer();
        server.requestHandler(new HttpRequestHandler(vertx, container));
        server.listen(CpeSimConstants.HTTP_SERVICE_REQ_PORT);
    }
}