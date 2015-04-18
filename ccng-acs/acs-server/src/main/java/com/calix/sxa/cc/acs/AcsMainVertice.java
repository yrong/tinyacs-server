package com.calix.sxa.cc.acs;

import com.calix.sxa.VertxConstants;
import com.calix.sxa.VertxDeployUtils;
import com.calix.sxa.VertxUtils;
import com.calix.sxa.cc.acs.cache.PassiveWorkflowCache;
import com.calix.sxa.cc.acs.nbi.AbstractAcNbiCrudService;
import com.calix.sxa.cc.acs.nbi.AcsApiService;
import com.calix.sxa.cc.acs.nbi.BuildInfo.BuildInfoService;
import com.calix.sxa.cc.acs.nbi.configurationcategory.ConfigurationCategoryService;
import com.calix.sxa.cc.acs.nbi.cwmplog.CwmpLogService;
import com.calix.sxa.cc.acs.nbi.devicedata.DeviceDataService;
import com.calix.sxa.cc.acs.nbi.deviceop.DeviceOpService;
import com.calix.sxa.cc.acs.nbi.devicetype.DeviceTypeService;
import com.calix.sxa.cc.acs.nbi.dialplan.DialPlanService;
import com.calix.sxa.cc.acs.nbi.event.EventService;
import com.calix.sxa.cc.acs.nbi.file.FileService;
import com.calix.sxa.cc.acs.nbi.group.GroupService;
import com.calix.sxa.cc.acs.nbi.maintenanceschedule.MaintenanceScheduleService;
import com.calix.sxa.cc.acs.nbi.model.AcsNbiRequest;
import com.calix.sxa.cc.acs.nbi.organization.OrganizationService;
import com.calix.sxa.cc.acs.nbi.organization.PerOrgNbiAuthenticator;
import com.calix.sxa.cc.acs.nbi.organization.PerOrgNbiAuthenticatorCache;
import com.calix.sxa.cc.acs.nbi.profile.ConfigurationProfileService;
import com.calix.sxa.cc.acs.nbi.serviceplan.ServicePlanService;
import com.calix.sxa.cc.acs.nbi.subscriber.SubscriberService;
import com.calix.sxa.cc.acs.nbi.workflow.WorkflowExecLogsService;
import com.calix.sxa.cc.acs.nbi.workflow.WorkflowService;
import com.calix.sxa.cc.acs.worker.autobackup.AutoBackupWorkerVertice;
import com.calix.sxa.cc.acs.worker.workflow.ActiveWorkflowTaskWorker;
import com.calix.sxa.cc.acs.worker.workflow.ActiveWorkflowWorkerVertice;
import com.calix.sxa.cc.acs.worker.workflow.PassiveWorkflowWorkerVertice;
import com.calix.sxa.cc.cache.ConfigurationProfileCache;
import com.calix.sxa.cc.cache.DialPlanCache;
import com.calix.sxa.cc.cache.GroupCache;
import com.calix.sxa.cc.cache.OrganizationCache;
import com.calix.sxa.cc.model.ConfigurationProfile;
import com.calix.sxa.cc.model.CpeGroup;
import com.calix.sxa.cc.model.Organization;
import com.calix.sxa.cc.model.Workflow;
import com.calix.sxa.cc.util.AcsApiUtils;
import com.calix.sxa.cc.util.AcsConfigProperties;
import com.calix.sxa.cc.util.AcsConstants;
import com.calix.sxa.taskmgmt.worker.TaskPollerVertice;
import com.calix.sxa.taskmgmt.worker.WorkerUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.internal.StringUtil;
import io.vertx.java.redis.RedisClient;
import org.apache.http.auth.AUTH;
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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Project:  SXA CC - ACS API
 *
 * @author: jqin
 */
public class AcsMainVertice extends Verticle {
    private static final Logger log = LoggerFactory.getLogger(AcsMainVertice.class.getName());

    /**
     * Server name (determined by mode and hostname and API service port #)
     */
    private static final String serverName =
            " ACS Server " + VertxUtils.getLocalHostname() + ":" + AcsConfigProperties.ACS_INTERNAL_API_PORT;

    /**
     * All Regular SXA-CC API Services
     */
    AcsApiService[] allSxaCcApiServices = new AcsApiService[] {
            new ConfigurationProfileService(),
            new DeviceOpService(),
            new DeviceTypeService(),
            new DeviceDataService(),
            new EventService(),
            new FileService(),
            new GroupService(),
            new WorkflowService(),
            new WorkflowExecLogsService(),
            new CwmpLogService(),
            new SubscriberService(),
            new ServicePlanService(),
            new DialPlanService(),
            new MaintenanceScheduleService(),
            new ConfigurationCategoryService(),
            new BuildInfoService(),
            new OrganizationService()
    };

    /**
     * SXA-CC Service Map <URL Path String --> API Service>
     */
    Map<String, AcsApiService> serviceMap = new HashMap<> ();

    /**
     * Per-Org Authenticator
     */
    PerOrgNbiAuthenticatorCache perOrgNbiAuthenticatorCache;

    /**
     * Start the Vertice
     */
    public void start() {
        log.info(serverName + " is starting up..\n");

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
        // Add Workflow Worker/Poller Vertice
        JsonObject workerConfig = WorkerUtils.buildConfig(
                new String[]{ActiveWorkflowTaskWorker.class.getName()},
                ActiveWorkflowTaskWorker.MAX_NBR_OF_OUTSTANDING_TASKS
        );
        deployments.add(VertxUtils.buildNewDeployment(ActiveWorkflowWorkerVertice.class.getName(), workerConfig));
        deployments.add(VertxUtils.buildNewDeployment(TaskPollerVertice.class.getName(), workerConfig));
        // Add Passive Workflow Worker Vertice(s)
        deployments.add(
                VertxUtils.buildNewDeployment(
                        PassiveWorkflowWorkerVertice.class.getName(),
                        null,
                        AcsConfigProperties.NBR_OF_PASSIVE_WORKFLOW_WORKER_VERTICES)
        );
        // Add Auto Backup Worker Vertice(s)
        deployments.add(
                VertxUtils.buildNewDeployment(AutoBackupWorkerVertice.class.getName(),null)
        );

        /**
         * Start all ACS server specific items after all sub modules have been deployed
         */
        deployments.finalHandler = new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> deployResult) {
                if (deployResult.succeeded()) {
                    log.info("All external and sub modules have been successfully installed.");

                    /**
                     * Create a local cache of all organization for authentication
                     */
                    perOrgNbiAuthenticatorCache = new PerOrgNbiAuthenticatorCache(vertx);

                    /**
                     * Create common objects to be shared by all services
                     */
                    OrganizationCache organizationCache = new OrganizationCache(
                            vertx,
                            AcsConstants.VERTX_ADDRESS_ACS_ORGANIZATION_CRUD,
                            Organization.DB_COLLECTION_NAME,
                            Organization.class.getSimpleName()
                    );
                    PassiveWorkflowCache passiveWorkflowCache = new PassiveWorkflowCache(
                            vertx,
                            AcsConstants.VERTX_ADDRESS_WORKFLOW_CRUD,
                            Workflow.DB_COLLECTION_NAME,
                            "passive-workflow"
                    );
                    ConfigurationProfileCache configurationProfileCache = new ConfigurationProfileCache(
                            vertx,
                            AcsConstants.VERTX_ADDRESS_ACS_CONFIG_PROFILE_CRUD,
                            ConfigurationProfile.DB_COLLECTION_NAME,
                            ConfigurationProfile.class.getSimpleName()
                    );
                    DialPlanCache dialPlanCache = new DialPlanCache(vertx);
                    GroupCache groupCache = new GroupCache(
                            vertx,
                            AcsConstants.VERTX_ADDRESS_ACS_GROUP_CRUD,
                            CpeGroup.DB_COLLECTION_NAME,
                            CpeGroup.class.getSimpleName()
                    );
                    RedisClient redisClient = new RedisClient(vertx.eventBus(), VertxConstants.VERTX_ADDRESS_REDIS);

                    /**
                     * Initialize service map
                     *
                     * TODO: Add release version to the URL path.
                     */
                    for (AcsApiService service : allSxaCcApiServices) {
                        log.info("Installing " + service.getServiceName() + " service...");
                        // Setup HTTP URL path for this service
                        serviceMap.put(service.getServiceName(), service);
                        // Install event bus handler for this service
                        vertx.eventBus().registerHandler(
                                AcsApiUtils.getServiceVertxAddress(service.getServiceName()),
                                new VertxRequestHandler(service));
                        // Set the Cache Objects
                        service.setOrganizationCache(organizationCache);
                        service.setPassiveWorkflowCache(passiveWorkflowCache);
                        service.setDialPlanCache(dialPlanCache);
                        service.setConfigurationProfileCache(configurationProfileCache);
                        service.setGroupCache(groupCache);
                        service.setRedisClient(redisClient);
                        // Start this service
                        service.start(vertx);
                    }

                    /**
                     * Start the SXA-CC Internal API HTTP server
                     */
                    HttpServer sxaCcApiServer = vertx.createHttpServer();
                    sxaCcApiServer.requestHandler(internalApiHttpRequestHandler);
                    sxaCcApiServer.listen(AcsConfigProperties.ACS_INTERNAL_API_PORT);
                    log.info(VertxUtils.highlightWithHashes(
                            "Internal API Port #: " + AcsConfigProperties.ACS_INTERNAL_API_PORT));

                    /**
                     * Start the External API HTTP server
                     */
                    HttpServer externalApiServer = vertx.createHttpServer();
                    externalApiServer.requestHandler(externalApiHttpRequestHandler);
                    externalApiServer.listen(AcsConfigProperties.ACS_EXTERNAL_API_PORT);
                    log.info(VertxUtils.highlightWithHashes(
                            "External API Port #: " + AcsConfigProperties.ACS_EXTERNAL_API_PORT));

                    log.info(VertxUtils.highlightWithHashes(
                            "File Server Base URL: " + AcsConfigProperties.BASE_FILE_SERVER_URL));

                    /**
                     * We are now up and running
                     */
                    log.info(VertxUtils.highlightWithHashes("ACS Server is now up and running."));

                    // Print Build Info
                    VertxUtils.displayBuildInfo(vertx);
                }
            }
        };

        /**
         * Start the Deployments
         */
        VertxUtils.deployModsVertices(container, deployments);
    }

    /**
     * Stop this Vertice
     */
    public void stop() {
        /**
         * Publish Server Departure Event
         */
        vertx.eventBus().publish(VertxConstants.VERTX_ADDRESS_SERVER_EVENTS,
                serverName + " is shutting down...");
        for (AcsApiService service : allSxaCcApiServices) {
            log.info("Shutting down " + service.getServiceName() + " service...");
            service.stop(vertx);
        }
    }

    /**
     * SXA-CC Internal API HTTP Request Handler
     */
    Handler<HttpServerRequest> internalApiHttpRequestHandler = new Handler<HttpServerRequest>() {
        /**
         * Handler Body
         *
         * @param request
         */
        @Override
        public void handle(final HttpServerRequest request) {
            /**
             * Determine OrgId
             *
             * "orgId", if any, is carried via a query parameter
             */
            final String orgId = request.params().get("orgId");

            // Call the common handler
            commonHttpRequestHandler(request, true, AcsConfigProperties.ACS_INTERNAL_API_CONTEXT_ROOT, orgId);
        }
    };

    /**
     * External API HTTP Request Handler
     */
    Handler<HttpServerRequest> externalApiHttpRequestHandler = new Handler<HttpServerRequest>() {
        /**
         * Handler Body
         *
         * @param request
         */
        @Override
        public void handle(final HttpServerRequest request) {
            log.debug("Received a new request from Remote host: " + request.remoteAddress().getHostString()
                    + ", URL path: " + request.path());

            /**
             * Authenticate the request
             */
            String authString = request.headers().get(AUTH.WWW_AUTH_RESP);
            if (authString == null) {
                /**
                 * TODO: Do we want to log this incident?
                 */

                log.error("Received an external request without " + AUTH.WWW_AUTH_RESP + " header! URL path: "
                        + request.path() + ", remote host: " + request.remoteAddress().getHostString());
                request.response().putHeader("Content-Type", "text/html");
                VertxUtils.setResponseStatus(request, HttpResponseStatus.UNAUTHORIZED);

                if (request.path().equals("/") || request.path().equals("/api")) {
                    request.response().end(
                            "<h2>Welcome to the Calix Consumer Connect API Server!</h2>\n" +
                            "\n" +
                            "\n" +
                            "<p>Every API request must carry a valid HTTP Basic Authorization Header.</p>\n" +
                            "\n" +
                            "<p>Please Consult with Calix Support Team to get the API Documents and Credentials.</p>\n"
                    );
                } else {
                    request.response().end();
                }
                return;
            }

            PerOrgNbiAuthenticator authenticator =
                    perOrgNbiAuthenticatorCache.getAuthenticatorByAuthString(authString);
            if (authenticator == null) {
                /**
                 * TODO: Do we want to log this incident?
                 */

                log.error("Received an NBI request with invalid " + AUTH.WWW_AUTH_RESP + " header! URL path: "
                        + request.path() + ", remote host: " + request.remoteAddress().getHostString());
                VertxUtils.setResponseStatus(request, HttpResponseStatus.UNAUTHORIZED);
                request.response().end();
                return;
            }

            // Get Org ID
            String orgId = authenticator.id;

            // Call the common handler
            commonHttpRequestHandler(request, false, AcsConfigProperties.ACS_EXTERNAL_API_CONTEXT_ROOT, orgId);
        }
    };

    /**
     * Common HTTP Request Handler Method.
     *
     * @param request
     * @param contextRoot
     * @param orgId
     */
    public void commonHttpRequestHandler(
            final HttpServerRequest request,
            final boolean bIsInternalRequest,
            String contextRoot,
            final String orgId) {
        /**
         * Lookup service instance by URL path which must be in the format of
         * "/[context root]/[service name]{/[]optional path parameters]}"
         */
        final String[] pathSegments = StringUtil.split(request.path(), '/');
        if (pathSegments.length < 3 || !pathSegments[1].equals(contextRoot)) {
            /**
             * Invalid Request Path
             */
            if (request.path().equals("/favicon.ico")) {
                //VertxUtils.serveHttpFileDownloadRequest(vertx, request, "favicon.ico");
                request.response().end();
            } else if (bIsInternalRequest && request.path().equals("/buildInfo")) {
                VertxUtils.serveHttpFileDownloadRequest(vertx, request, "build-info.txt");
            } else {
                log.error("Invalid URL Path " + request.path() + "! HTTP Method: " + request.method());
                if (pathSegments.length > 1) {
                    log.error("Invalid context root " + pathSegments[1] + ", expecting " + contextRoot);
                } else {
                    log.error("Missing context root.");
                }

                VertxUtils.badHttpRequest(request, "Invalid URL path!");
            }
            return;
        }

        final AcsApiService service = serviceMap.get(pathSegments[2]);

        if (service != null) {
            /**
             * A regular ACS API CRUD Service Request
             * The process starts after the entire body has been received
             */
            request.bodyHandler(new Handler<Buffer>() {
                @Override
                public void handle(Buffer rawBodyBuffer) {
                    JsonObject body = null;
                    try {
                        if (rawBodyBuffer != null && rawBodyBuffer.length() > 0) {
                            body = new JsonObject(rawBodyBuffer.toString());
                        } else {
                            body = new JsonObject();
                        }
                    } catch (Exception ex) {
                        VertxUtils.badHttpRequest(request, "Invalid JSON Payload!");
                        return;
                    }

                    /**
                     * Print Debug Log for all external requests
                     */
                    if (bIsInternalRequest == false) {
                        if (body.size() > 0) {
                            log.debug("Request body:\n" + body.encodePrettily());
                        } else {
                            log.debug("Request body is empty.");
                        }
                    }

                    if (orgId != null) {
                        body.putString(AcsConstants.FIELD_NAME_ORG_ID, orgId);
                    }

                    // Create a new AcsNbiRequest POJO and Call the service's handler
                    try {
                        service.handle(new AcsNbiRequest(request, body, bIsInternalRequest), pathSegments);
                    } catch (Exception ex) {
                        String errorDetails = VertxUtils.getLocalIpAddress() + "~" + VertxUtils.getPid()
                                + "~" + new Date().toString() + "~" + System.currentTimeMillis();
                        VertxUtils.responseWithStatusCode(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                request,
                                AbstractAcNbiCrudService.INTERNAL_SERVER_ERROR_CONTACT_CALIX
                                        + " (error detail: " + errorDetails
                        );
                        ex.printStackTrace();
                    }
                }
            });
        } else {
            /**
             * Invalid Request Path
             */
            log.error("No service found for " + pathSegments[2] + "! Invalid URL Path "
                    + request.path() + "! HTTP Method: " + request.method());
            VertxUtils.badHttpRequest(request, "Invalid URL path!");
        }
    }

    /**
     * Inner Class for Internal Vert.x Request Handler.
     *
     * Dedicated instance per service.
     */
    private class VertxRequestHandler implements Handler<Message<JsonObject>> {
        AcsApiService service;

        /**
         * Constructor.
         *
         * @param service
         */
        public VertxRequestHandler(AcsApiService service) {
            this.service = service;
        }

        @Override
        public void handle(Message<JsonObject> message) {
            // Call the service's handler
            service.handle(new AcsNbiRequest(message, message.body()), null);
        }
    };
}