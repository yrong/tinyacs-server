package com.calix.sxa.cc.cpeserver.session;

/**
 * Project:  SXA-CC
 *
 * CWMP Session FSM Event Enum.
 *
 * @author: jqin
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
