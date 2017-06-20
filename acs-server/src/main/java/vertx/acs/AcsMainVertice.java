package vertx.acs;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import vertx.VertxConfigProperties;
import vertx.VertxConstants;
import vertx.VertxUtils;
import vertx.acs.cache.PassiveWorkflowCache;
import vertx.acs.nbi.AbstractAcNbiCrudService;
import vertx.acs.nbi.AcsApiService;
import vertx.acs.nbi.BuildInfo.BuildInfoService;
import vertx.acs.nbi.configurationcategory.ConfigurationCategoryService;
import vertx.acs.nbi.cwmplog.CwmpLogService;
import vertx.acs.nbi.devicedata.DeviceDataService;
import vertx.acs.nbi.deviceop.DeviceOpService;
import vertx.acs.nbi.devicetype.DeviceTypeService;
import vertx.acs.nbi.dialplan.DialPlanService;
import vertx.acs.nbi.event.EventService;
import vertx.acs.nbi.file.FileService;
import vertx.acs.nbi.group.GroupService;
import vertx.acs.nbi.maintenanceschedule.MaintenanceScheduleService;
import vertx.acs.nbi.model.AcsNbiRequest;
import vertx.acs.nbi.organization.OrganizationService;
import vertx.acs.nbi.organization.PerOrgNbiAuthenticator;
import vertx.acs.nbi.organization.PerOrgNbiAuthenticatorCache;
import vertx.acs.nbi.profile.ConfigurationProfileService;
import vertx.acs.nbi.serviceplan.ServicePlanService;
import vertx.acs.nbi.subscriber.SubscriberService;
import vertx.acs.nbi.workflow.WorkflowExecLogsService;
import vertx.acs.nbi.workflow.WorkflowService;
import vertx.acs.worker.autobackup.AutoBackupWorkerVertice;
import vertx.acs.worker.workflow.ActiveWorkflowTaskWorker;
import vertx.acs.worker.workflow.ActiveWorkflowWorkerVertice;
import vertx.acs.worker.workflow.PassiveWorkflowWorkerVertice;
import vertx.cache.ConfigurationProfileCache;
import vertx.cache.DialPlanCache;
import vertx.cache.GroupCache;
import vertx.cache.OrganizationCache;
import vertx.model.ConfigurationProfile;
import vertx.model.CpeGroup;
import vertx.model.Organization;
import vertx.model.Workflow;
import vertx.util.AcsApiUtils;
import vertx.util.AcsConfigProperties;
import vertx.util.AcsConstants;
import vertx.taskmgmt.worker.TaskPollerVertice;
import vertx.taskmgmt.worker.WorkerUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.internal.StringUtil;
import org.apache.http.auth.AUTH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Project:  SXA CC - ACS API
 *
 * @author: ronyang
 */
public class AcsMainVertice extends AbstractVerticle {
    private static final Logger log = LoggerFactory.getLogger(AcsMainVertice.class.getName());

    /**
     * Server name (determined by mode and hostname and API service port #)
     */
    private static final String serverName =
            " ACS Server " + VertxUtils.getLocalHostname() + ":" + AcsConfigProperties.ACS_INTERNAL_API_PORT;

    /**
     * All Regular cwmp API Services
     */
    AcsApiService[] allCWMPApiServices = new AcsApiService[] {
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
     * cwmp Service Map <URL Path String --> API Service>
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
        JsonObject workerConfig = WorkerUtils.buildConfig(
                new String[]{ActiveWorkflowTaskWorker.class.getName()},
                ActiveWorkflowTaskWorker.MAX_NBR_OF_OUTSTANDING_TASKS
        );
        DeploymentOptions options = new DeploymentOptions().setConfig(workerConfig);
        vertx.deployVerticle(ActiveWorkflowWorkerVertice.class.getName(),options);
        vertx.deployVerticle(TaskPollerVertice.class.getName(),options);
        vertx.deployVerticle(PassiveWorkflowWorkerVertice.class.getName(), options, res -> {
            if (res.succeeded()) {
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
                RedisOptions redis_config = new RedisOptions()
                        .setHost(VertxConfigProperties.redisHost).setPort(VertxConfigProperties.redisPort).setSelect(VertxConfigProperties.redisDbIndex);
                RedisClient redisClient = RedisClient.create(vertx,redis_config);

                /**
                 * Initialize service map
                 *
                 * TODO: Add release version to the URL path.
                 */
                for (AcsApiService service : allCWMPApiServices) {
                    log.info("Installing " + service.getServiceName() + " service...");
                    // Setup HTTP URL path for this service
                    serviceMap.put(service.getServiceName(), service);
                    // Install event bus handler for this service
                    vertx.eventBus().consumer(
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
                 * Start the cwmp Internal API HTTP server
                 */
                HttpServer CWMPApiServer = vertx.createHttpServer();
                CWMPApiServer.requestHandler(internalApiHttpRequestHandler);
                CWMPApiServer.listen(AcsConfigProperties.ACS_INTERNAL_API_PORT);
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
        });
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
        for (AcsApiService service : allCWMPApiServices) {
            log.info("Shutting down " + service.getServiceName() + " service...");
            service.stop(vertx);
        }
    }

    /**
     * cwmp Internal API HTTP Request Handler
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
            log.debug("Received a new request from Remote host: " + request.remoteAddress().host()
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
                        + request.path() + ", remote host: " + request.remoteAddress().host());
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
                        + request.path() + ", remote host: " + request.remoteAddress().host());
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
        final String[] pathSegments = request.path().split("/");
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
                        body.put(AcsConstants.FIELD_NAME_ORG_ID, orgId);
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