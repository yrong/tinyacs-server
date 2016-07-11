package vertx2.util;

/**
 * Project:  cwmp
 *
 * ACS NBI Constants.
 *
 * @author: ronyang
 */
public class AcsConstants {
    // cwmp Group ID
    public static final String CWMP_GROUP_ID = "vertx2";

    /**
     * API Service Names
     */
    public static final String ACS_API_SERVICE_DEVICE_OP = "device-op";
    public static final String ACS_API_SERVICE_DEVICE_DATA = "device";
    public static final String ACS_API_SERVICE_SUBSCRIBER = "subscriber";
    public static final String ACS_API_SERVICE_GROUP = "group";
    public static final String ACS_API_SERVICE_FILE = "file";
    public static final String ACS_API_SERVICE_CWMP_LOG = "device-cwmp-logs";
    public static final String ACS_API_SERVICE_CHANGE_LOG = "device-change-logs";
    public static final String ACS_API_SERVICE_WORKFLOW = "workflow";
    public static final String ACS_API_SERVICE_WORKFLOW_EXEC_LOG = "workflow-exec-logs";
    public static final String ACS_API_SERVICE_MAINTENANCE_SCHEDULE = "maintenance-schedule";
    public static final String ACS_API_SERVICE_EVENT = "event";
    public static final String ACS_API_SERVICE_ORGANIZATION = "organization";
    public static final String ACS_API_SERVICE_CONFIGURATION_PROFILE = "configuration-profile";
    public static final String ACS_API_SERVICE_CONFIGURATION_CATEGORY = "configuration-category";
    public static final String ACS_API_SERVICE_SERVICE_PROFILE = "service-profile";
    public static final String ACS_API_SERVICE_NOTIFICATION_PROFILE = "notification-profile";
    public static final String ACS_API_SERVICE_DEVICE_TYPE = "device-type";
    public static final String ACS_API_SERVICE_DEVICE_DATA_MODEL = "device-data-model";
    public static final String ACS_API_SERVICE_SERVICE_PLAN = "service-plan";
    public static final String ACS_API_SERVICE_DIAL_PLAN = "dial-plan";

    /**
     * Vert.x Event Bus Addresses
     */
    // ACS API "External" Event Address
    public static final String VERTX_ADDRESS_ACS_API = "acs.api";
    // ACS Device Op Request Address Prefix (followed by CPE server's Hostname+PID)
    public static final String VERTX_ADDRESS_ACS_DEVICE_OP_REQUEST_PREFIX = "acs.device.op.request~";
    // ACS Connection Request Address
    public static final String VERTX_ADDRESS_ACS_CONNECTION_REQUEST= "acs.connection.request";
    // ACS API Callback Event Address
    public static final String VERTX_ADDRESS_ACS_API_CALLBACK = "acs.api.callback";
    // ACS Organization CRUD Notifications
    public static final String VERTX_ADDRESS_ACS_ORGANIZATION_CRUD = "acs.organization.crud";
    // ACS Group CRUD Notifications
    public static final String VERTX_ADDRESS_ACS_GROUP_CRUD = "acs.group.crud";
    // ACS Configuration Category CRUD Notifications
    public static final String VERTX_ADDRESS_ACS_CONFIG_CATEGORY_CRUD = "acs.config.category.crud";
    // ACS Configuration Profile CRUD Notifications
    public static final String VERTX_ADDRESS_ACS_CONFIG_PROFILE_CRUD = "acs.config.profile.crud";
    // ACS Workflow CRUD Notifications
    public static final String VERTX_ADDRESS_WORKFLOW_CRUD = "acs.config.workflow.crud";
    // ACS Workflow Suspend Requests
    public static final String VERTX_ADDRESS_WORKFLOW_SUSPEND = "acs.config.workflow.suspend";
    // ACS Dial-Plan CRUD Notifications
    public static final String VERTX_ADDRESS_ACS_DIAL_PLAN_CRUD = "acs.dial.plan.crud";
    // ACS Maintenance Schedule Notifications
    public static final String VERTX_ADDRESS_MAINTENANCE_SCHEDULE = "acs.config.maintenance.schedule";

    /**
     * Redis Keys
     */
    /**
     * Per-CPE Redis Sets that stores device-operation requests
     */
    // actual Redis keys will be built by adding the following prefix to CPE id string
    public static final String REDIS_KEY_DEVICE_OP_PREFIX = "acs.device.op~";
    // Connection-Request Related Redis Key Prefix
    public static final String REDIS_KEY_CONN_REQ_PREFIX = "acs.conn.req~";
    /**
     * CPE Discovery Queue
     */
    public static final String REDIS_KEY_CPE_DISCOVERY_QUEUE = "acs.cpe.discovery.queue";

    /**
     * Auto Backup Task Queue
     */
    public static final String REDIS_KEY_AUTO_BACKUP_QUEUE = "acs.auto.backup.queue";

    /**
     * Field Names
     */
    public static final String FIELD_NAME_ID = "_id";
    public static final String FIELD_NAME_ORG_ID = "orgId";
    public static final String FIELD_NAME_METHOD = "method";
    public static final String FIELD_NAME_NAME = "name";
    public static final String FIELD_NAME_DESCRIPTION = "description";
    public static final String FIELD_NAME_CPE_ID = "cpeIdentifier";
    public static final String FIELD_NAME_CPE_FILTER = "cpeFilter";
    public static final String FIELD_NAME_ERROR = "error";
    public static final String FIELD_NAME_STATUS_CODE = "status";
    public static final String FIELD_NAME_MORE_EXIST = "moreExist";
    public static final String FIELD_NAME_RESULT = "result";
    public static final String FIELD_NAME_PARAM_VALUES = "parameterValues";
    public static final String FIELD_NAME_INCLUDES = "includes";
    public static final String FIELD_NAME_NBR_OF_RECORDS = "numberOfRecords";
    public static final String FIELD_NAME_CREATE_TIME = "createTime";
    public static final String FIELD_NAME_USERNAME = "username";
    public static final String FIELD_NAME_PASSWORD = "password";

    // ACS CRUD Notifications
    public static final String FIELD_NAME_ACS_CRUD_TYPE = "crudType";

    // HTTP Digest Auth Realm
    public static final String CWMP_AUTH_REALM = "CalixGcs";
    // HTTP Basic Auth Realm
    public static final String HTTP_BASIC_AUTH_CHALLENGE = "Basic realm=\"" + CWMP_AUTH_REALM + "\"";
}
