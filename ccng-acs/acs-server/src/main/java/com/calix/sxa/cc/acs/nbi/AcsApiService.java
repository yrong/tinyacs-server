package com.calix.sxa.cc.acs.nbi;

import com.calix.sxa.cc.acs.cache.PassiveWorkflowCache;
import com.calix.sxa.cc.acs.nbi.model.AcsNbiRequest;
import com.calix.sxa.cc.cache.ConfigurationProfileCache;
import com.calix.sxa.cc.cache.DialPlanCache;
import com.calix.sxa.cc.cache.GroupCache;
import com.calix.sxa.cc.cache.OrganizationCache;
import io.vertx.java.redis.RedisClient;
import org.vertx.java.core.Vertx;

/**
 * Project:  SXA CC - ACS API
 *
 * ACS API Service Interface Definition.
 *
 * To be implemented by actual services such as "Device Operation" or "Group" or "Event" services.
 *
 * @author: jqin
 */
public interface AcsApiService {
    /**
     * Start the service
     */
    public void start(Vertx vertx);

    /**
     * Stop the service
     */
    public void stop(Vertx vertx);

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    public String getServiceName();

    /**
     * Handle a new request received via either Vert.x Event Bus or HTTP.
     *
     * @param nbiRequest
     * @param urlPathParams An array of strings that holds all the URL path parameters if any. The first 3 array
     *                      element are the API context root ("cc") and the service name.
     */
    public void handle(AcsNbiRequest nbiRequest, String[] urlPathParams);

    /**
     * Set Organization Cache
     */
    public void setOrganizationCache(OrganizationCache organizationCache);

    /**
     * Set Passive Workflow Cache
     */
    public void setPassiveWorkflowCache(PassiveWorkflowCache passiveWorkflowCache);

    /**
     * Set Dial Plan Cache
     */
    public void setDialPlanCache(DialPlanCache dialPlanCache);

    /**
     * Set Configuration Profile Cache
     */
    public void setConfigurationProfileCache(ConfigurationProfileCache configurationProfileCache);

    /**
     * Set Group Cache
     */
    public void setGroupCache(GroupCache groupCache);

    /**
     * Set Redis Client
     */
    public void setRedisClient(RedisClient redisClient);

    /**
     * TODO: Add Method to get WADL for this service
     */
}
