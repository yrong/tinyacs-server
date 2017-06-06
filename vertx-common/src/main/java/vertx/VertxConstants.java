package vertx;

import io.vertx.core.json.JsonObject;

/**
 * SXA Vert.x Constants
 *
 * @author: ronyang
 */
public class VertxConstants {
    /**
     * Vert.x Addresses
     */
    // MongoDB Persistor
    public static final String VERTX_ADDRESS_MONGODB = VertxUtils.getHostnameAndPid() + ".mongodb";

    // MongoDB GridFS
    public static final String VERTX_ADDRESS_MONGODB_GRID_FS = VertxUtils.getHostnameAndPid() + ".gridfs";
    public static final String VERTX_ADDRESS_MONGODB_GRID_FS_SAVE_CHUNK = VERTX_ADDRESS_MONGODB_GRID_FS + "/saveChunk";

    // General Purpose Redis Address
    public static final String VERTX_ADDRESS_REDIS = VertxUtils.getHostnameAndPid() + ".redis";

    // Redis Pub Address
    public static final String VERTX_ADDRESS_REDIS_PUB = VertxUtils.getHostnameAndPid() + ".redis.pub";

    // Redis Sub Address
    public static final String VERTX_ADDRESS_REDIS_SUB = VertxUtils.getHostnameAndPid() + ".redis.sub";

    // Server Instance Arrival/Departures
    public static final String VERTX_ADDRESS_SERVER_EVENTS = "server.events";

    // URL Protocol String for Vert.x Event Bus Address Based URLs
    public static final String URL_PROTOCOL_VERTX_EVENT_BUS = "vertx";

    /**
     * Mod-mongo-persistor
     */
    public static final String MOD_MONGO_PERSISTOR = "io.vertx~mod-mongo-persistor~2.1.0";
    public static final JsonObject MOD_MONGO_PERSISTOR_CONFIG = VertxMongoUtils.getModMongoPersistorConfig();

    

    /**
     * Mod-redis
     */
    public static final String MOD_REDIS = "io.vertx~mod-redis~1.1.4";
    public static final JsonObject MOD_REDIS_CONFIG = new JsonObject()
            .put("address", VertxConstants.VERTX_ADDRESS_REDIS)
            .put("host", VertxConfigProperties.redisHost)
            .put("port",VertxConfigProperties.redisPort);

}
