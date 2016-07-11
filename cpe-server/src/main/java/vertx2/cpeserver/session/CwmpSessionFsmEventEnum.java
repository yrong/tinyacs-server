package vertx2.cpeserver.session;

/**
 * Project:  cwmp
 *
 * CWMP Session FSM Event Enum.
 *
 * @author: ronyang
 */
public enum CwmpSessionFsmEventEnum {
    InformReceived,
    NonInformRpcReceived,
    DbQueryResultReceived,
    InternalDbFailure,
    CpeMessageReceived,
    Timeout,
    NbiInactiveTimedOut,
    NbiCallbackComplete,
    NewNbiRequest,
    ReceivedInProgressDeviceOp
}
