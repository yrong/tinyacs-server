package vertx.cpe.sim;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.ext.mongo.MongoClient;
import vertx.VertxMongoUtils;
import vertx.VertxUtils;
import vertx.util.CpeDataModelMgmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpServer;

/**
 * Project:  CPE Simulator
 *
 * @author: ronyang
 */
public class CpeSimMainVertice extends AbstractVerticle {
    private final Logger log = LoggerFactory.getLogger(CpeSimMainVertice.class.getName());

    /**
     * Event Bus
     */
    EventBus eventBus;

    /**
     * Vertice Start Time
     */
    public static long startTime = System.currentTimeMillis();

    public MongoClient mongoClient;

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

        mongoClient = MongoClient.createShared(vertx, VertxMongoUtils.getModMongoPersistorConfig());


        /**
         * Initialize Data Models
         */
        CpeDataModelMgmt.init(vertx, "tr_data_models/");

        /**
         * Read the default CPE data
         */
        CpeSimUtils.initDefaultCpeData(vertx);

        /**
         * Deploy multiple Session Vertice based on the # of CPE Cores
         */
        int numberOfVertices = 1;//Runtime.getRuntime().availableProcessors();
        if (numberOfVertices > 32) {
            numberOfVertices = 32;
        }
        log.info("Starting " + numberOfVertices + " session vertices.");
        DeploymentOptions options = new DeploymentOptions().setInstances(numberOfVertices);
        vertx.deployVerticle(CpeSimSessionVertice.class.getName(),options);

        /**
         * Deploy multiple Diag worker vertices
         */
        log.info("Starting " + numberOfVertices + " diag worker vertices.");
        vertx.deployVerticle(CpeDiagWorkerVertice.class.getName(), options);

        /**
         * Start the server
         */
        HttpServer server = vertx.createHttpServer();
        server.requestHandler(new HttpRequestHandler(vertx,mongoClient));
        server.listen(CpeSimConstants.HTTP_SERVICE_REQ_PORT);
    }
}