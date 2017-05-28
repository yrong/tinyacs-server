package vertx.cwmp;

/**
 * Project:  ccng-acs
 * 
 * CWMP Fault Code Enums.
 *
 * @author: ronyang
 */
public class CwmpFaultCodes {
    public static final long REQUEST_DENIED = 9001;
    public static final long INTERNAL = 9002;
    public static final long INVALID_ARGS = 9003;
    public static final long RESOURCE_EXCEEDED = 9004;
    public static final long INVALID_PARAMETER_NAME = 9005;
    public static final long INVALID_PARAMETER_TYPE = 9006;
    public static final long INVALID_PARAMETER_VALUE = 9007;
    public static final long PARAMETER_READONLY = 9008;
    public static final long NOTIFICATION_REJECTED = 9009;
    public static final long DOWNLOAD_FAILURE = 9010;
    public static final long UPLOAD_FAILURE = 9011;
    public static final long FILE_TRANSFER_AUTHENTICATION_FAILURE = 9012;
    public static final long PROTOCOL_NOT_SUPPORTED = 9013;
    public static final long DLF_MULTICAST = 9014;
    public static final long DLF_NO_CONTACT = 9015;
    public static final long DLF_FILE_ACCESS = 9016;
    public static final long DLF_UNABLE_TO_COMPLETE = 9017;
    public static final long DLF_FILE_CORRUPTED = 9018;
    public static final long DLF_FILE_AUTHENTICATION = 9019;
    public static final long ACS_METHOD_NOT_SUPPORTED = 8000;
    public static final long ACS_REQUEST_DENIED = 8001;
    public static final long ACS_INTERNAL_ERROR = 8002;
    public static final long ACS_INVALID_ARGS = 8003;
    public static final long ACS_RESOURCE_EXCEEDED = 8004;
    public static final long ACS_RETRY = 8005;
}
