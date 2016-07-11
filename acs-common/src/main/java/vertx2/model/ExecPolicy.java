package vertx2.model;

import vertx2.VertxException;
import vertx2.VertxJsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  cwmp-parent
 *
 * @author: ronyang
 */
public class ExecPolicy {
    private static final Logger log = LoggerFactory.getLogger(ExecPolicy.class.getName());

    /**
     * Default Values
     */
    public static final int DEFAULT_DEVICE_OP_TIMEOUT = 30;         // 30 seconds
    public static final int DEFAULT_DOWNLOAD_TIMEOUT = 300;         // 5 minutes
    public static final int DEFAULT_DEVICE_OP_MAX_RETRIES = 0;      // no retry by default
    public static final int DEFAULT_DEVICE_OP_RETRY_INTERVAL = 60; // 1 min
    public static final int DEFAULT_MAX_CONCURRENT_DEVICE = 100;

    /**
     * JSON Field Names
     */
    public static final String FIELD_NAME_MAX_CONCURRENT_DEVICES = "maxConcurrentDevices";
    public static final String FIELD_NAME_INITIAL_TRIGGER = "initialTrigger";
    public static final String FIELD_NAME_MAINTENANCE_SCHEDULE = "maintenanceWindow";
    public static final String FIELD_NAME_WINDOW = "window";
    public static final String FIELD_NAME_INITIAL_DELAY = "initialDelay";
    public static final String FIELD_NAME_RETRY_POLICY = "retryPolicy";
    public static final String FIELD_NAME_MAX_RETRIES = "maxRetries";
    public static final String FIELD_NAME_RETRY_INTERVAL = "retryInterval";
    public static final String FIELD_NAME_TIMEOUT = "timeout";

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator mandatoryFields =
            new VertxJsonUtils.JsonFieldValidator();

    public static final VertxJsonUtils.JsonFieldValidator optionalFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_TIMEOUT, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_INITIAL_TRIGGER, VertxJsonUtils.JsonFieldType.JsonObject)
                    .append(FIELD_NAME_INITIAL_DELAY, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_MAX_CONCURRENT_DEVICES, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_MAX_RETRIES, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_RETRY_INTERVAL, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_WINDOW, VertxJsonUtils.JsonFieldType.JsonObject);

    /**
     * Static Constants
     */
    public static final JsonObject EXEC_POLICY_WITH_DEFAULT_TIMEOUT = new JsonObject()
            .putNumber(FIELD_NAME_TIMEOUT, DEFAULT_DEVICE_OP_TIMEOUT);
    public static final JsonObject EXEC_POLICY_WITH_DOWNLOAD_TIMEOUT = new JsonObject()
            .putNumber(FIELD_NAME_TIMEOUT, DEFAULT_DOWNLOAD_TIMEOUT);

    /**
     * Local Variables
     */
    public JsonObject rawJsonObject;
    public MaintenanceWindow maintenanceWindow = null;
    public WorkflowTrigger initialTrigger = null;
    public int timeout = DEFAULT_DEVICE_OP_TIMEOUT;
    public int maxConcurrentDevices = DEFAULT_MAX_CONCURRENT_DEVICE;
    public int initialDelay = 0;
    public int maxRetries = DEFAULT_DEVICE_OP_MAX_RETRIES;
    public int retryInterval = DEFAULT_DEVICE_OP_RETRY_INTERVAL;

    /**
     * Constructor by a Exec Policy JSON Object.
     *
     * @param execPolicyJsonObject    A JSON Object

     * @throws vertx2.VertxException
     */
    public ExecPolicy(JsonObject execPolicyJsonObject) throws VertxException{
        this.rawJsonObject = execPolicyJsonObject;
        /**
         * Call the common validation method first
         */
        VertxJsonUtils.validateFields(execPolicyJsonObject, mandatoryFields, optionalFields);

        initialDelay = execPolicyJsonObject.getInteger(FIELD_NAME_INITIAL_DELAY, 0);
        timeout = execPolicyJsonObject.getInteger(FIELD_NAME_TIMEOUT, DEFAULT_DEVICE_OP_TIMEOUT);
        maxRetries = execPolicyJsonObject.getInteger(FIELD_NAME_MAX_RETRIES, DEFAULT_DEVICE_OP_MAX_RETRIES);
        retryInterval = execPolicyJsonObject.getInteger(FIELD_NAME_RETRY_INTERVAL, DEFAULT_DEVICE_OP_RETRY_INTERVAL);
        timeout = execPolicyJsonObject.getInteger(FIELD_NAME_TIMEOUT, DEFAULT_DEVICE_OP_TIMEOUT);
        maxConcurrentDevices = execPolicyJsonObject.getInteger(FIELD_NAME_MAX_CONCURRENT_DEVICES, DEFAULT_MAX_CONCURRENT_DEVICE);

        if (execPolicyJsonObject.containsField(FIELD_NAME_WINDOW)) {
            maintenanceWindow = new MaintenanceWindow(execPolicyJsonObject.getObject(FIELD_NAME_WINDOW));
        }

        if (execPolicyJsonObject.containsField(FIELD_NAME_INITIAL_TRIGGER)) {
            initialTrigger = new WorkflowTrigger(execPolicyJsonObject.getObject(FIELD_NAME_INITIAL_TRIGGER));
        }
    }
}