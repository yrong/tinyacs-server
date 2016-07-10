package vertx2.cpeserver.session;

/**
 * Project:  SXA-CC
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
