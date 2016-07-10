package vertx2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

/**
 * Vertx Module Deployment Utils.
 *
 * @author: ronyang
 */
public class VertxDeployUtils {
    private static Logger log = LoggerFactory.getLogger(VertxDeployUtils.class);

    // String Constants
    private static final String FIELD_NAME_DEPLOYMENT_NAME = "name";
    private static final String FIELD_NAME_DEPLOYMENT_CONFIG = "config";
    private static final String FIELD_NAME_DEPLOYMENT_NBR_OF_INSTANCES = "numberOfInstances";

    /**
     * Inner Class to track all deployments
     */
    public static class Deployments extends JsonArray {
        public AsyncResultHandler<String>  finalHandler = null;
    }

    /**
     * Build a single new deployment with name and config (single instance).
     * @param name
     * @param config
     * @return
     */
    public static JsonObject buildNewDeployment(String name, JsonObject config) {
        return buildNewDeployment(name, config, 1);
    }

    /**
     * Build a single new deployment with name and config (single instance).
     * @param name
     * @param config
     * @param nbrOfInstances
     * @return
     */
    public static JsonObject buildNewDeployment(String name, JsonObject config, int nbrOfInstances) {
        return new JsonObject()
                .putString(FIELD_NAME_DEPLOYMENT_NAME, name)
                .putObject(FIELD_NAME_DEPLOYMENT_CONFIG, config)
                .putNumber(FIELD_NAME_DEPLOYMENT_NBR_OF_INSTANCES, nbrOfInstances);
    }

    /**
     * Deploy multiple modules/vertices one by one in the given order.
     *
     * @param deployments
     */
    public static void deployModsVertices(Container container, Deployments deployments) {
        log.info("Deploying the following modules/vertices:\n" + deployments.encodePrettily());
        singleDeploy(container, deployments);
    }

    /**
     * Deploy the first module/vertice in the array
     * @param container
     * @param deployments
     */
    public static void singleDeploy(Container container, Deployments deployments) {
        if (deployments.size() > 0) {
            // Need to deploy first one
            JsonObject firstDeployment = deployments.get(0);
            String name = firstDeployment.getString(FIELD_NAME_DEPLOYMENT_NAME);
            if (name != null) {
                // Build another async handler ahead
                DeploymentAsyncHandler nextHandler = new DeploymentAsyncHandler(container, deployments);

                // Extract Config
                JsonObject config = firstDeployment.getObject(FIELD_NAME_DEPLOYMENT_CONFIG);

                // Extract # of Instances
                int nbrOfInstances = firstDeployment.getInteger(FIELD_NAME_DEPLOYMENT_NBR_OF_INSTANCES, 1);

                if (firstDeployment.getString(FIELD_NAME_DEPLOYMENT_NAME).contains("~")) {
                    log.info("Deploying Module " + name + "... (nbrOfInstances = " + nbrOfInstances + ")");
                    if (nbrOfInstances > 1) {
                        container.deployModule(name, config, nbrOfInstances, nextHandler);
                    } else {
                        container.deployModule(name, config, nextHandler);
                    }
                } else {
                    log.info("Deploying Vertice " + name + "... (nbrOfInstances = " + nbrOfInstances + ")");
                    if (nbrOfInstances > 1) {
                        container.deployVerticle(name, config, nbrOfInstances, nextHandler);
                    } else {
                        container.deployVerticle(name, config, nextHandler);
                    }
                }
            } else {
                log.error("No module/vertice name found!\n" + firstDeployment.encodePrettily());
            }
        } else {
            log.info("No more deployment to be done.");
        }
    }

    /**
     * Async Result Handler for deployment
     */
    public static class DeploymentAsyncHandler implements AsyncResultHandler<String> {

        /**
         * Vertx Container
         */
        Container container;

        /**
         * The List of all the modules/vertices that need to be deployed
         * with field name being the module/vertice name and value being the deployment config
         */
        Deployments allDeployments;

        /**
         * Constructor.
         */
        public DeploymentAsyncHandler(
                Container container,
                Deployments allDeployments) {
            this.container = container;
            this.allDeployments = allDeployments;
        }

        @Override
        public void handle(AsyncResult<String> asyncResult) {
            // This deployment
            JsonObject deployment = allDeployments.get(0);

            // Rest of the deployment(s)
            Deployments rest = null;
            if (allDeployments.size() > 1) {
                // Build a new array without this module/vertice
                rest = new Deployments();
                for (int i = 1; i < allDeployments.size(); i ++) {
                    rest.add(allDeployments.get(i));
                }
                if (allDeployments.finalHandler != null) {
                    rest.finalHandler = allDeployments.finalHandler;
                }
            }

            // Check deploy result
            if (asyncResult.succeeded()) {
                log.info(deployment.getString(FIELD_NAME_DEPLOYMENT_NAME) + " has been successfully deployed.");

                if (rest != null) {
                    // Deploy the next module/vertice
                    singleDeploy(container, rest);
                } else {
                    // Call the final handler if any
                    if (allDeployments.finalHandler != null) {
                        allDeployments.finalHandler.handle(asyncResult);
                    }
                }
            } else {
                log.error("Failed to deploy " + deployment.getString(FIELD_NAME_DEPLOYMENT_NAME) + "!!!");
                if (rest != null) {
                    log.error("the following module(s)/vertice(s) will NOT be deployed:\n" + rest.encodePrettily());
                }
                // Call the final handler if any
                if (allDeployments.finalHandler != null) {
                    allDeployments.finalHandler.handle(asyncResult);
                }
            }
        }
    }

    /**
     * Deploy Mongo Persistor.
     *
     * @param container
     */
    public static void deployMongoPersistor(Container container) {
        Deployments deployments = new Deployments();
        deployments.add(VertxConstants.MOD_MONGO_PERSISTOR_DEPLOYMENT);
        deployModsVertices(container, deployments);
    }
}
