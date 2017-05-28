package vertx.taskmgmt;

import vertx.VertxConfigProperties;
import vertx.VertxConstants;
import vertx.VertxUtils;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.platform.Verticle;

/**
 * Project:  cwmp-parent
 *
 * REST API Verticle for Task CRUD Service
 *
 * @author: ronang
 */
public class TaskMgmtRestWsVertice extends Verticle {
    private static final Logger log = LoggerFactory.getLogger(TaskMgmtRestWsVertice.class.getName());

    /**
     * Server Port
     *
     * TODO: Shall we move this configuration to mod.json?
     */
    public static final int servicePort = 8090;

    /**
     * Saved raw request
     */
    HttpServerRequest savedRequest;

    /**
     * Async Redis Client
     */
    public static RedisClient redisClient = null;
    
    
    /**
     * Start the Vertice
     */
    public void start() {
               
        log.info("Redis Server is at " + VertxConfigProperties.redisHost + ":" + VertxConfigProperties.redisPort);
        /**
         * Init Async Redis Client
         */
        redisClient = new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);

        log.info("\nStarting SXA Task Mgmt REST Web Service Verticle on port # " + servicePort + "...\n");
        /**
         * Start the server
         */
        HttpServer server = vertx.createHttpServer();
        server.requestHandler(new Handler<HttpServerRequest>() {
            public void handle(HttpServerRequest request) {
                savedRequest = request;
                //savedRequest.response().setChunked(true);
                log.info("Request path: " + request.path());
                log.info("Request method: " + request.method());

                /**
                 * Dispatch the request by method and path
                 */
                if (request.method().equals(HttpMethod.POST.name())) {
                    /**
                     * Create new task
                     */
                    request.bodyHandler(new CreateTaskHandler(request));
                } else {
                    /**
                     * Retrieve Tasks or Cancel/Abort/Delete Existing Task.
                     */
                    try {
                        QueryTaskHandler queryTaskHandler = new QueryTaskHandler(request, vertx.eventBus());
                        queryTaskHandler.run();
                    } catch (Exception e) {
                        VertxUtils.setResponseStatus(request, HttpResponseStatus.BAD_REQUEST);
                        VertxUtils.writeToResponse(request, e.getMessage());
                    }
                }
           }
        }).listen(servicePort);
    }
}