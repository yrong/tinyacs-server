package vertx.util;

import vertx.model.AcsApiCrudTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp
 *
 * ACS API Client Utils
 *
 * @author: ronyang
 */
public class AcsApiUtils {
    private static final Logger log = LoggerFactory.getLogger(AcsApiUtils.class);

    /**
     * Send an ACS API Request via Vert.x Event Bus.
     *
     * @param eventBus
     * @param serviceName
     * @param orgId
     * @param crudType
     * @param requestBody
     * @param timeout         timeout in # of seconds
     * @param resultHandler
     */
    public static void sendApiRequest(
            EventBus eventBus,
            String serviceName,
            String orgId,
            AcsApiCrudTypeEnum crudType,
            JsonObject requestBody,
            long timeout,
            Handler<AsyncResult<Message<JsonObject>>> resultHandler) {
        eventBus.sendWithTimeout(
                getServiceVertxAddress(serviceName),
                requestBody
                        .putString(AcsConstants.FIELD_NAME_METHOD, crudType.httpMethodString)
                        .putString(AcsConstants.FIELD_NAME_ORG_ID, orgId),
                timeout * 1000,
                resultHandler
        );
    }

    /**
     * Get the Service HTTP URL Path for a given service name.
     * @param serviceName
     */
    public static String getServiceUrlPath(String serviceName) {
        return AcsConfigProperties.ACS_INTERNAL_API_CONTEXT_ROOT + "/" + serviceName;
    }

    /**
     * Get the Vertx Event Bus Address for a given service name.
     * @param serviceName
     */
    public static String getServiceVertxAddress(String serviceName) {
        return AcsConstants.VERTX_ADDRESS_ACS_API + "." + serviceName;
    }

    /**
     * Get the Document Type by MongoDB Collection Name.
     *
     * For example, "CWMP-devices" will produce "device"
     *
     * @param dbCollectionName
     * @return
     */
    public static String getDocumentTypeByCollectionName(String dbCollectionName) {
        // Remove the "CWMP-" prefix
        String withoutPrefix = dbCollectionName.substring(dbCollectionName.indexOf("-") + 1);
        // Remove the trailing "s"
        return withoutPrefix.substring(0, withoutPrefix.length() - 1);
    }
}
