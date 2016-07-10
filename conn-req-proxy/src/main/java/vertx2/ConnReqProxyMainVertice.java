package vertx2;

import com.calix.sxa.VertxConstants;
import com.calix.sxa.VertxDeployUtils;
import com.calix.sxa.VertxUtils;
import vertx2.connreq.ConnectionRequestManagerVertice;
import vertx2.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * Project:  SXA-CC
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
