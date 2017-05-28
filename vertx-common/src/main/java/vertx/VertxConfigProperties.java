package vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SXA Vertx Configuration Property Utils.
 *
 * For now, the Configuration Properties are read from System environment.
 *
 * Some defaults are used if failed to read from System environment.
 *
 * @author: ronyang
 */
public class VertxConfigProperties {
    private static final Logger log = LoggerFactory.getLogger(VertxConfigProperties.class.getName());

    /**
     * Default MongoDb Server Host/Port/Instance
     */
    public static final String defaultMongoHost = "localhost";
    public static final int defaultMongoPort = 27017;
    public static final String defaultMongoDbName = "sxa";
    public static final String defaultMongoSeeds = null;

    /**
     * System Environment Variable Names for MongoDB
     */
    public static final String mongoHostSysEnvVar = "CWMP_MONGO_HOST";
    public static final String mongoPortSysEnvVar = "CWMP_MONGO_PORT";
    public static final String mongoDbNameSysEnvVar = "CWMP_MONGO_DB_NAME";
    public static final String mongoSeedsSysEnvVar = "CWMP_MONGO_SEEDS";

    /**
     * Actual MongoDb Server Host
     */
    public static final String mongoHost = VertxUtils.initStringProp(mongoHostSysEnvVar, defaultMongoHost);
    public static final int mongoPort = VertxUtils.initIntegerProp(mongoPortSysEnvVar, defaultMongoPort);
    public static final String mongoDbName = VertxUtils.initStringProp(mongoDbNameSysEnvVar, defaultMongoDbName);
    public static final String mongoSeeds = VertxUtils.initStringProp(mongoSeedsSysEnvVar, defaultMongoSeeds);

    /**
     * Default REDIS Server Hostname/Port/Timeout/DB_Index
     */
    public static final String defaultRedisHost = "localhost";
    public static final int defaultRedisPort = 6379;
    public static final int defaultRedisTimeout = 2000;
    public static final int defaultRedisDbIndex = 0;
    public static final String defaultRedisPassword = null;
    public static final String defaultRedisNamespace = "resque";

    /**
     * System Environment Variable Names for REDIS Server Hostname/Port/Timeout/DB_Index
     */
    public static final String redisHostSysEnvVar = "CWMP_REDIS_HOST";
    public static final String redisPortSysEnvVar = "CWMP_REDIS_PORT";
    public static final String redisTimeoutSysEnvVar = "CWMP_REDIS_TIMEOUT";
    public static final String redisDbIndexSysEnvVar = "CWMP_REDIS_DB_INDEX";
    public static final String redisPasswordSysEnvVar = "CWMP_REDIS_PASSWORD";

    /**
     * Actual REDIS Server Hostname/Port/Timeout/DB_Index Values
     */
    public static final String redisHost = VertxUtils.initStringProp(redisHostSysEnvVar, defaultRedisHost);
    public static final int redisPort = VertxUtils.initIntegerProp(redisPortSysEnvVar, defaultRedisPort);
    public static final int redisTimeout = VertxUtils.initIntegerProp(redisTimeoutSysEnvVar, defaultRedisTimeout);
    public static final int redisDbIndex = VertxUtils.initIntegerProp(redisDbIndexSysEnvVar, defaultRedisDbIndex);
    public static final String redisPassword = VertxUtils.initStringProp(redisPasswordSysEnvVar, defaultRedisPassword);

    /**
     * Local Hostname
     */
    public static final String LOCAL_HOSTNAME_SYS_ENV_VAR = "CWMP_LOCAL_HOSTNAME";
    public static final String LOCAL_HOSTNAME = VertxUtils.initStringProp(LOCAL_HOSTNAME_SYS_ENV_VAR,
            VertxUtils.getLocalHostname());
}
