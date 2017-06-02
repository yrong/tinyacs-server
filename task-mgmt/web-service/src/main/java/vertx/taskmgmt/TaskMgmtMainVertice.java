package vertx.taskmgmt;

import io.vertx.core.AbstractVerticle;
import vertx.VertxConstants;
import vertx.VertxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  cwmp-parent
 *
 * @author: ronang
 */
public class TaskMgmtMainVertice extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(TaskMgmtMainVertice.class.getName());

    /**
     * Start the Vertice
     */
    public void start() {
        log.info("\nTask Mgmt Server Instance " + VertxUtils.getLocalHostname() + " is starting up..\n");
        vertx.deployVerticle(TaskMgmtRestWsVertice.class.getName());
    }


    public void stop() {
        /**
         * Publish Server Departure Event
         */
        vertx.eventBus().publish(VertxConstants.VERTX_ADDRESS_SERVER_EVENTS,
                "Task Mgmt Server Instance " + VertxUtils.getLocalHostname() + " is shutting down...");
   }
}