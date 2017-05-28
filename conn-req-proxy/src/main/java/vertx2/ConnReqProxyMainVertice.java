package vertx;

import vertx.VertxConstants;
import vertx.VertxDeployUtils;
import vertx.VertxUtils;
import vertx.connreq.ConnectionRequestManagerVertice;
import vertx.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.platform.Verticle;

/**
 * Project:  cwmp
 *
 * Connection-Request Proxy.
 *
 * The requests are carried as HTTP requests with all the information in the payload.
 *
 * @author: ronyang
 */
public class ConnReqProxyMainVertice extends Verticle {
    private final Logger log = LoggerFactory.getLogger(ConnReqProxyMainVertice.class.getName());

    /**
     * Start the Vertice
     */
    public void start() {
        log.info("Connection-Request Proxy is starting up on port " + ConnReqProxyConstants.HTTP_SERVICE_PORT + "\n");

        /**
         * Build the list of sub modules/vertices to be deployed
         */
        VertxDeployUtils.Deployments deployments = new VertxDeployUtils.Deployments();
        // Add Mod Redis
        deployments.add(VertxConstants.MOD_REDIS_DEPLOYMENT);
        // Add Connection-Request Worker Vertice
        deployments.add(VertxUtils.buildNewDeployment(ConnectionRequestManagerVertice.class.getName(), null));


        /**
         * Start all CPE server specific items after mongo persistor has been deployed
         */
        deployments.finalHandler = new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> deployResult) {
                if (deployResult.succeeded()) {
                    log.info("All external and sub modules have been successfully installed.");

                    /**
                     * Start the HTTP server which simply forward the request/response between requester and the
                     * ConnReqManager Vertice.
                     */
                    HttpServer server = vertx.createHttpServer();
                    server.requestHandler(new Handler<HttpServerRequest>() {
                        @Override
                        public void handle(final HttpServerRequest request) {
                            /**
                             * Body Handler
                             */
                            request.bodyHandler(new Handler<Buffer>() {
                                @Override
                                public void handle(Buffer rawBodyBuffer) {
                                    JsonObject reqBody = null;
                                    try {
                                        reqBody = new JsonObject(rawBodyBuffer.toString());
                                    } catch (Exception ex) {
                                        VertxUtils.badHttpRequest(request, "Invalid JSON Payload!");
                                        return;
                                    }

                                    vertx.eventBus().send(
                                            AcsConstants.VERTX_ADDRESS_ACS_CONNECTION_REQUEST,
                                            reqBody,
                                            new Handler<Message<JsonObject>>() {
                                                @Override
                                                public void handle(Message<JsonObject> result) {
                                                    VertxUtils.responseSuccess(request, result.body().toString());
                                                }
                                            }
                                    );
                                }
                            });
                        }
                    });
                    server.listen(ConnReqProxyConstants.HTTP_SERVICE_PORT);
                }
            }
        };

        /**
         * Start the Deployments
         */
        VertxUtils.deployModsVertices(container, deployments);
    }
}
