package com.calix.sxa.cc.cpeserver.session;

/**
 * Project:  SXA-CC
 *
 * CWMP Session FSM State Enum.
 *
 * @author: jqin
 */
public enum CwmpSessionFsmStateEnum {
    // Inform Received
    Start,
    // Successfully authenticated, Querying the CPE Device DB after received the Inform
    QueryingCpeDeviceDb,
    // DB Query is done, "InformResponse" was sent, waiting for CPE requests (such as TransferComplete) or Empty POST
    Server,
    // After sent an RPC request message to CPE, we need to wait for the CPE response before the next step
    // i.e. already reversed to client role after receiving an Empty POST
    PendingCpeResponse,
    // All internal requests have been processed, and now let us wait for NBI requests from both Redis and
    // Vert.x Event Bus (for up to 10 seconds)
    WaitingForNewNbiRequest,
    // After sending result for an NBI request to the callback URL, we need to make sure the callback is properly
    // received by the client before polling/serving the next NBI request
    PendingNbiCallback,
    // Reading In-Progress Device Op From Redis
    ReadingInProgressDeviceOp,
    // All done.
    Terminated,
    // Authentication Failure
    AuthFailure
}
