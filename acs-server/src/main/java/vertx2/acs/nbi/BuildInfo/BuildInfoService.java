package vertx2.acs.nbi.BuildInfo;

import vertx2.VertxUtils;
import vertx2.acs.cache.PassiveWorkflowCache;
import vertx2.acs.nbi.AcsApiService;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.cache.ConfigurationProfileCache;
import vertx2.cache.DialPlanCache;
import vertx2.cache.GroupCache;
import vertx2.cache.OrganizationCache;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.java.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;

/**
 * Project:  cwmp Master Branch
 *
 * @author: ronyang
 */
public class BuildInfoService implements AcsApiService{
    /**
     * Logger Instance
     */
    public Logger log = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * Build Info String
     */
    public String buildInfoString = "Build Info Not Available.";

    /**
     * Start the service
     *
     * @param vertx
     */
    @Override
    public void start(Vertx vertx) {
        /**
         * Initialize Build Info String
         */
        final String filePath = "build-info.txt";
        vertx.fileSystem().readFile(
                filePath,
                new AsyncResultHandler<Buffer>() {
                    public void handle(AsyncResult<Buffer> ar) {
                        if (ar.succeeded()) {
                            if (ar.result() != null) {
                                log.info(VertxUtils.highlightWithHashes("Build Info:\n" + ar.result()));
                                buildInfoString = ar.result().toString() + "\n";
                            } else {
                                log.error(VertxUtils.highlightWithHashes("File " + filePath + " is empty!"));
                            }
                        } else {
                            log.error(VertxUtils.highlightWithHashes(
                                    "File " + filePath + " does not exist!" + " (" + ar.cause() + ")"));
                        }
                    }
                });
    }

    /**
     * Stop the service
     *
     * @param vertx
     */
    @Override
    public void stop(Vertx vertx) {

    }

    /**
     * Set Organization Cache
     */
    public void setOrganizationCache(OrganizationCache organizationCache) {
    }

    /**
     * Set Passive Workflow Cache
     *
     * @param passiveWorkflowCache
     */
    @Override
    public void setPassiveWorkflowCache(PassiveWorkflowCache passiveWorkflowCache) {

    }

    /**
     * Set Dial Plan Cache
     *
     * @param dialPlanCache
     */
    @Override
    public void setDialPlanCache(DialPlanCache dialPlanCache) {

    }

    /**
     * Set Configuration Profile Cache
     *
     * @param configurationProfileCache
     */
    @Override
    public void setConfigurationProfileCache(ConfigurationProfileCache configurationProfileCache) {

    }

    /**
     * Set Group Cache
     *
     * @param groupCache
     */
    @Override
    public void setGroupCache(GroupCache groupCache) {

    }

    /**
     * Set Redis Client
     *
     * @param redisClient
     */
    @Override
    public void setRedisClient(RedisClient redisClient) {

    }

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     * <p/>
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return "build-info";
    }

    /**
     * Handle a new request received via either Vert.x Event Bus or HTTP.
     *
     * @param nbiRequest
     * @param urlPathParams An array of strings that holds all the URL path parameters if any. The first 3 array
     */
    @Override
    public void handle(AcsNbiRequest nbiRequest, String[] urlPathParams) {
        if (nbiRequest.httpServerRequest != null) {
            nbiRequest.sendResponse(HttpResponseStatus.OK, buildInfoString);
        } else {
            nbiRequest.sendResponse(HttpResponseStatus.BAD_REQUEST);
        }
    }
}
