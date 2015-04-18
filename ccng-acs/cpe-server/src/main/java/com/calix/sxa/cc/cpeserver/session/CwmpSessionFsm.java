package com.calix.sxa.cc.cpeserver.session;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxMongoUtils;
import com.calix.sxa.cc.cpeserver.CpeServerConstants;
import com.calix.sxa.cc.cwmp.*;
import com.calix.sxa.cc.model.Cpe;
import com.calix.sxa.cc.model.CpeDeviceOpTypeEnum;
import dslforumOrgCwmp12.FaultDocument;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA-CC
 *
 * CWMP Session Finite State Machine (all static util methods)
 *
 * State Machine Transits:
 *
 * --------------------------------------------------------------------------------------------------------------------
 * Current State            Event               Action(s)                           New State
 * --------------------------------------------------------------------------------------------------------------------
 * Start                    Inform as a valid   Start Querying CPE Db               QueryingCpeDeviceDb
 *                          HTTP Basic auth
 *                          header
 * --------------------------------------------------------------------------------------------------------------------
 * Start                    Auth Failed         Send NOT_AUTHORIZED Response        AuthFailure
 * --------------------------------------------------------------------------------------------------------------------
 * QueryingCpeDeviceDb      Result Received     Send InformResponse                 Server
 * --------------------------------------------------------------------------------------------------------------------
 * Server                   RPC Message         Send proper RPC Response            Server
 * --------------------------------------------------------------------------------------------------------------------
 * Server                   Empty POST          1.Send next RPC request in queue    PendingCpeResponse
 *                                              2.Query Redis                       or
 *                                              3.Update Session info in Redis      WaitingForNewNbiRequest          
 *                                              4.Start CPE Inactive Timer
 * --------------------------------------------------------------------------------------------------------------------
 * Server                   CPE Resp Timed out  Terminate Session                   Terminated
 * --------------------------------------------------------------------------------------------------------------------
 * PendingCpeResponse       Received CPE Resp   Send result to north bound via      PendingNbiCallback
 *                                              via callback
 * --------------------------------------------------------------------------------------------------------------------
 * PendingCpeResponse       CPE Resp Timed out  1. Terminate Session                Terminated
 *                                              2. Call timeout handler of all
 *                                                 requests in queue as well as
 *                                                 the outstanding request
 *                                              3. Remove session info from Redis
 * --------------------------------------------------------------------------------------------------------------------
 * PendingNbiCallback       Callback confirmed  1.Enqueue the next device-op
 *                                                received from callback reply
 *                                              2.If local queue is empty, start    WaitingForNewNbiRequest
 *                                                NBI inactive timer
 *                                              3.If local queue is NOT empty,      PendingCpeResponse
 *                                                send the next request
 * --------------------------------------------------------------------------------------------------------------------
 * PendingNbiCallback       Callback Timed Out  1.If local queue is empty, start    WaitingForNewNbiRequest
 *                                                NBI inactive timer
 *                                              3.If local queue is NOT empty,      PendingCpeResponse
 *                                                send the next request
 * --------------------------------------------------------------------------------------------------------------------
 * WaitingForNewNbiRequest  Received NBI request 1.Enqueue and send the newly       PendingCpeResponse
 *                                                 received request
 *                                              2. Start CPE Inactive Timer
 * --------------------------------------------------------------------------------------------------------------------
 * WaitingForNewNbiRequest  NBI Inactive Timeout Terminate Session                  Terminated
 * --------------------------------------------------------------------------------------------------------------------
 * ReadingInProgressDeviceOp  In-Prog Device    Send result to NBI client via       Server
 *                            Op received       callback                            or
 *                                                                                  Terminated
 * --------------------------------------------------------------------------------------------------------------------
 * @author: jqin
 */
public class CwmpSessionFsm {
    private static final Logger log = LoggerFactory.getLogger(CwmpSessionFsm.class.getName());

    /**
     * Main FSM State Transit Method.
     * 
     * @param session
     * @param event
     */
    public static void transit(
            CwmpSession session, 
            CwmpSessionFsmEventEnum event) {
        CwmpSessionFsmStateEnum oldState = session.state;

        if (event.equals(CwmpSessionFsmEventEnum.InternalDbFailure)) {
            // Send CWMP Fault
            session.sendCwmpMessageToCpe(
                    CwmpUtils.getFaultMessage(
                        session.cwmpVersion,
                        CwmpFaultCodes.ACS_INTERNAL_ERROR,
                        "Internal DB Failure!")
            );

            // Terminate Session
            session.terminate();
        }
        switch (session.state) {
            case Start:
                transitFromStart(session, event);
                break;

            case QueryingCpeDeviceDb:
                transitFromQueryingCpeDeviceDb(session, event);
                break;

            case Server:
                transitFromServer(session, event);
                break;

            case PendingCpeResponse:
                transitFromPendingCpeResponse(session, event);
                break;

            case WaitingForNewNbiRequest:
                transitFromWaitingForNewNbiRequest(session, event);
                break;

            case PendingNbiCallback:
                transitFromPendingNbiCallback(session, event);
                break;

            case Terminated:
                transitFromTerminated(session, event);
                break;

            case AuthFailure:
                transitFromAuthFailure(session, event);
                break;

            case ReadingInProgressDeviceOp:
                transitFromReadingInProgressDeviceOp(session, event);
                break;

            default:
                log.error(session.cpeKey + ": Invalid State " + session.state.name());
                break;
        }

        if (!oldState.equals(session.state)) {
            // Print state transition
            log.debug(session.cpeKey + ": " + oldState.name() + " ----> " + session.state.name()
                    + " upon event " + event.name());
        }

        // Restart timer on any event is session is not terminated
        if (!session.state.equals(CwmpSessionFsmStateEnum.Terminated)) {
            session.startInactiveTimer(CpeServerConstants.CWMP_SESSION_TIMEOUT);

            if (session.state.equals(CwmpSessionFsmStateEnum.WaitingForNewNbiRequest)) {
                session.startInactiveTimer(CpeServerConstants.CWMP_SESSION_NBI_INACTIVE_TIMEOUT);
            } else {
                session.startInactiveTimer(CpeServerConstants.CWMP_SESSION_TIMEOUT);
            }
        }
    }

    /**
     * ----------------------------------------------------------------------------------------------------------------
     * Current State            Event               Action(s)                           New State
     * ----------------------------------------------------------------------------------------------------------------
     * Start                    Inform as a valid   Start Querying CPE Db               QueryingCpeDeviceDb
     *                          HTTP Basic auth
     *                          header
     * ----------------------------------------------------------------------------------------------------------------
     *
     * @param event
     * @param session
     */
    public static void transitFromStart(CwmpSession session, CwmpSessionFsmEventEnum event) {
        switch (event) {
            case CpeMessageReceived:
                // Query DB using the CPE info extracted from the "Inform" request
                try {
                    session.state = CwmpSessionFsmStateEnum.QueryingCpeDeviceDb;
                    VertxMongoUtils.findOne(
                            session.eventBus,
                            Cpe.CPE_COLLECTION_NAME,
                            // Matcher which contains the CPE Key as the id
                            new JsonObject().putString(VertxMongoUtils.MOD_MONGO_FIELD_NAME_ID, session.cpeKey),
                            // Async FindOne Result Handler
                            new VertxMongoUtils.FindOneHandler(
                                    new CwmpSession.DbQueryHandler(
                                            session,
                                            session.orgId,
                                            session.informRequest.getDeviceId())),
                            // Keys
                            null
                    );
                } catch (SxaVertxException e) {
                    // This should never happen though
                    e.printStackTrace();
                    session.state = CwmpSessionFsmStateEnum.Terminated;
                    CwmpHttpRequestHandler.reply(
                            session.httpRequestMessage,
                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            null,
                            null
                    );
                }
                break;

            default:
                unexpectedEvent(session, event);
                break;
        }
    }

    /**
     * ----------------------------------------------------------------------------------------------------------------
     * Current State            Event               Action(s)                           New State
     * ----------------------------------------------------------------------------------------------------------------
     * QueryingCpeDeviceDb      Result Received     Send InformResponse                 Server
     * ----------------------------------------------------------------------------------------------------------------
     *
     * @param event
     * @param session
     */
    public static void transitFromQueryingCpeDeviceDb(CwmpSession session, CwmpSessionFsmEventEnum event) {
        switch (event) {
            case DbQueryResultReceived:
                session.state = CwmpSessionFsmStateEnum.Server;

                /**
                 * Generate Cookie
                 */
                String cookie = CwmpSessionCookieUtils.getNewCookie(
                        session.cpe.getCpeKey(),
                        session.sessionVertice.verticeIndex
                );

                /**
                 * Process Inform and Send InformResponse
                 */
                session.sendCwmpMessageToCpe(session.handleInform(), cookie);

                break;

            case NewNbiRequest:
            case Timeout:
                break;

            default:
                unexpectedEvent(session, event);
                break;
        }
    }

    /**
     * ----------------------------------------------------------------------------------------------------------------
     * Current State            Event               Action(s)                           New State
     * ----------------------------------------------------------------------------------------------------------------
     * Server                   RPC Request         Send proper RPC Response            Server
     *                                              Restart Inactive Timer
     * ----------------------------------------------------------------------------------------------------------------
     * Server                   RPC Response        Send the next request if any        Server
     *                                              Restart Inactive Timer
     * ----------------------------------------------------------------------------------------------------------------
     * Server                   Empty POST          1.Send next RPC request in queue    PendingCpeResponse
     *                                              2.Query Redis                       or
     *                                              3.Update Session info in Redis      WaitingForNewNbiRequest          
     *                                              4.Start CPE Inactive Timer
     * ----------------------------------------------------------------------------------------------------------------
     * Server                   CPE Resp Timed out  Terminate Session                   Terminated
     * ----------------------------------------------------------------------------------------------------------------
     * 
     * We will get here from session.processCpeMessage(), which means the RPC message has already been processed.
     *
     * @param event
     * @param session
     */
    public static void transitFromServer(
            CwmpSession session,
            CwmpSessionFsmEventEnum event) {
        switch (event) {
            case CpeMessageReceived:
                if (session.receivedCpeMessage == null) {
                    // Received Empty POST
                    if (session.bTriggeredByConnReq) {
                        // Start waiting for NBI requests
                        session.state = CwmpSessionFsmStateEnum.WaitingForNewNbiRequest;

                        // Store session info in Redis
                        //session.storeSessionInfo();

                        // Check for any pending device-op in Redis queue
                        session.popDeviceOpFromRedis();
                    }

                    if (session.multiSessionOpType != CpeDeviceOpTypeEnum.Invalid) {
                        /**
                         * Read the Conn-Req/Session Info to get the in-progress diag/download device-op
                         */
                        session.state = CwmpSessionFsmStateEnum.ReadingInProgressDeviceOp;
                        session.readCurrentInProgressDeviceOp();
                    } else {
                        // De-queue the next request from local queue if any
                        deQueueNextRequest(session);
                    }
                } else {
                    // Received a RPC Request from CPE
                    CwmpMessage cwmpMessage = null;
                    switch (session.receivedCpeMessage.rpcMessageName) {
                        case "TransferComplete":
                            /**
                             * TransferComplete
                             */
                            log.info(session.cpeKey + ": Handling TransferComplete Request...");
                            cwmpMessage = session.handleTransferComplete(session.receivedCpeMessage);
                            break;

                        case "GetRPCMethods":
                            /**
                             * GetRPCMethods
                             */
                            log.info(session.cpeKey + ": Handling GetRPCMethods Request...");
                            cwmpMessage = session.handleGetRPCMethods(session.receivedCpeMessage);
                            break;

                        default:
                            /**
                             * Unexpected message
                             */
                            log.error(session.cpeKey + ": Unsupported ACS method "
                                    + session.receivedCpeMessage.rpcMessageName + "!");
                            cwmpMessage = CwmpUtils.getFaultMessage(
                                    session.receivedCpeMessage.cwmpVersion,
                                    CwmpFaultCodes.ACS_REQUEST_DENIED,
                                    "Unsupported ACS method"
                            );
                    }

                    // Send the response
                    session.sendCwmpMessageToCpe(cwmpMessage);
                }
                break;

            case Timeout:
                // terminate the session
                session.terminate();
                break;

            case NewNbiRequest:
                // Do nothing till the role is reversed. The requests are stored in the local queue anyway.
                break;

            default:
                unexpectedEvent(session, event);
                break;
        }
    }

    /**
     * ----------------------------------------------------------------------------------------------------------------
     * Current State            Event               Action(s)                           New State
     * ----------------------------------------------------------------------------------------------------------------
     * PendingCpeResponse       Received CPE Resp   Send result to north bound via      PendingNbiCallback
     *                                              via callback
     * ----------------------------------------------------------------------------------------------------------------
     * PendingCpeResponse       CPE Resp Timed out  1. Terminate Session                Terminated
     *                                              2. Call timeout handler of all
     *                                                 requests in queue as well as
     *                                                 the outstanding request
     *                                              3. Remove session info from Redis
     * ----------------------------------------------------------------------------------------------------------------
     *
     * @param event
     * @param session
     */
    public static void transitFromPendingCpeResponse(
            CwmpSession session,
            CwmpSessionFsmEventEnum event) {
        switch (event) {
            case CpeMessageReceived:
                /**
                 * After role is reversed, we only except response messages for the current outstanding request
                 */
                CwmpMessage message = session.receivedCpeMessage;
                CwmpMessage responseMessage = null;

                /**
                 * Is it a Fault message?
                 */
                if (message != null && message.soapEnv.getBody().isSetFault()) {
                    /**
                     * Extract the CWMP Fault from SOAP Fault
                     */
                    FaultDocument.Fault cwmpFault = null;
                    try {
                        cwmpFault = message.soapEnv.getBody().getFault().getDetail().getFault();
                    } catch (Exception e) {
                        log.error(session.cpeKey + ": Invalid SOAP Fault! (No CWMP Fault found.)\n"
                                + message.toPrettyXmlText());
                    }

                    try {
                        /**
                         * Call the outstanding request's fault handler
                         */
                        session.outstandingRequest.handlers.faultHandler(
                                session,
                                session.outstandingRequest,
                                cwmpFault
                        );
                    } catch (Exception ex) {
                        log.error(session.cpeKey + ": Fault Handler threw exception " + ex.getMessage());
                        ex.printStackTrace();
                    }
                    session.outstandingRequest = null;
                } else {
                    /**
                     * Check the response message type
                     */
                    if (!message.rpcMessageName.equals(session.outstandingRequest.methodName + "Response")) {
                        log.error(session.cpeKey + ": Unexpected " + message.rpcMessageName + " message!" +
                                "The current outstanding request is " + session.outstandingRequest.methodName);
                        if (!message.rpcMessageName.endsWith("Response")) {
                            responseMessage = CwmpUtils.getFaultMessage(
                                    message.cwmpVersion,
                                    CwmpFaultCodes.ACS_REQUEST_DENIED,
                                    session.cpeKey + ": Unexpected " + message.rpcMessageName + " message"
                            );

                            // Send the response
                            CwmpHttpRequestHandler.reply(
                                    session.httpRequestMessage,
                                    HttpResponseStatus.OK,
                                    responseMessage.toXmlText(),
                                    null
                            );
                        }
                    } else {
                        /**
                         * Check Message ID
                         */
                        if (message.id == null || !message.id.equals(session.outstandingRequest.rpcMessage.id)) {
                            log.error(session.cpeKey + ": Invalid/Unexpected message ID " + message.id + " in "+ message.rpcMessageName + "!" +
                                    "The current outstanding request's ID is "
                                    + session.outstandingRequest.rpcMessage.id);
                        } else {
                            /**
                             * Call the outstanding request's response handler
                             */
                            try {
                                log.debug(session.cpeKey + ": Calling handler for " + message.rpcMessageName);
                                session.outstandingRequest.handlers.responseHandler(
                                        session,
                                        session.outstandingRequest,
                                        message
                                );
                            } catch (CwmpException e) {
                                log.error(session.cpeKey + ": Caught CWMP Exception " + e.getMessage()
                                        + " while processing" + message.toPrettyXmlText());
                            }
                            session.outstandingRequest = null;

                            // Do we need to perform an auto backup?
                            if (session.cpe.deviceId.changeCounter != null) {
                                switch (message.rpcMessageName) {
                                    case "DeleteObject":
                                    case "SetParameterValuesResponse":
                                        /**
                                         * Set cpeChangeCounterChanged to true so an auto backup will be performed later
                                         */
                                        session.cpeChangeCounterChanged = true;
                                        break;
                                }
                            }
                        }
                    }
                }

                // Continue de-queuing next request from local queue (if any) only if the current request is done
                if (session.state != CwmpSessionFsmStateEnum.PendingNbiCallback) {
                    deQueueNextRequest(session);
                }

                if (session.bTriggeredByConnReq) {
                    /**
                     * Refresh Session Info In Redis
                     */
                    session.refreshSessionInfoInRedis();
                }
                break;

            case NewNbiRequest:
                break;

            case Timeout:
                session.terminate();
                break;

            default:
                unexpectedEvent(session, event);
                break;
        }
    }

    /**
     * ----------------------------------------------------------------------------------------------------------------
     * Current State            Event               Action(s)                           New State
     * ----------------------------------------------------------------------------------------------------------------
     * WaitingForNewNbiRequest  Received NBI request 1.Enqueue and send the newly       PendingCpeResponse
     *                                                 received request
     *                                              2. Start CPE Inactive Timer
     * ----------------------------------------------------------------------------------------------------------------
     * WaitingForNewNbiRequest  NBI Inactive Timeout Terminate Session                  Terminated
     * ----------------------------------------------------------------------------------------------------------------
     *
     * @param event
     * @param session
     */
    public static void transitFromWaitingForNewNbiRequest(
            CwmpSession session,
            CwmpSessionFsmEventEnum event) {
        switch (event) {
            case NewNbiRequest:
                deQueueNextRequest(session);
                break;

            case Timeout:
                session.terminate();
                break;

            default:
                unexpectedEvent(session, event);
                break;
        }
    }

    /**
     * ----------------------------------------------------------------------------------------------------------------
     * Current State            Event               Action(s)                           New State
     * ----------------------------------------------------------------------------------------------------------------
     * PendingNbiCallback       Callback confirmed  1.Enqueue the next device-op
     *                                                received from callback reply
     *                                              2.If local queue is empty, start    WaitingForNewNbiRequest
     *                                                NBI inactive timer
     *                                              3.If local queue is NOT empty,      PendingCpeResponse
     *                                                send the next request
     * ----------------------------------------------------------------------------------------------------------------
     * PendingNbiCallback       Callback Timed Out  1.If local queue is empty, start    WaitingForNewNbiRequest
     *                                                NBI inactive timer
     *                                              3.If local queue is NOT empty,      PendingCpeResponse
     *                                                send the next request
     * ----------------------------------------------------------------------------------------------------------------
     *
     * @param event
     * @param session
     */
    public static void transitFromPendingNbiCallback(
            CwmpSession session,
            CwmpSessionFsmEventEnum event) {
        switch (event) {
            case NbiCallbackComplete:
                session.state = CwmpSessionFsmStateEnum.WaitingForNewNbiRequest;
                deQueueNextRequest(session);
                break;

            case Timeout:
                session.terminate();
                break;

            case NewNbiRequest:
                break;

            default:
                unexpectedEvent(session, event);
                break;
        }
    }

    /**
     * ----------------------------------------------------------------------------------------------------------------
     * Current State              Event             Action(s)                           New State
     * ----------------------------------------------------------------------------------------------------------------
     * ReadingInProgressDeviceOp  In-Progress       Send result to NBI client via       PendingCpeResponse
     *                            Device-Op         callback                            or
     *                            Received                                              Terminated
     * ----------------------------------------------------------------------------------------------------------------
     *
     * @param session
     * @param event
     */
    public static void transitFromReadingInProgressDeviceOp(
            CwmpSession session,
            CwmpSessionFsmEventEnum event) {
        switch (event) {
            case ReceivedInProgressDeviceOp:
                deQueueNextRequest(session);
                break;

            case Timeout:
                // Unable to read from Redis within the timeout period
                // or timed out waiting for "Empty POST"
                // Terminate session
                session.terminate();
                break;

            default:
                unexpectedEvent(session, event);
                break;
        }
    }

    /**
     *
     * @param event
     * @param session
     */
    public static void transitFromTerminated(
            CwmpSession session,
            CwmpSessionFsmEventEnum event) {
        unexpectedEvent(session, event);
    }

    /**
     *
     * @param event
     * @param session
     */
    public static void transitFromAuthFailure(
            CwmpSession session,
            CwmpSessionFsmEventEnum event) {
        unexpectedEvent(session, event);
    }

    /**
     * Print an unexpected event error log.
     * @param session
     * @param event
     */
    public static void unexpectedEvent(CwmpSession session, CwmpSessionFsmEventEnum event) {
        log.error(session.cpeKey + ": Unexpected CPE Event " + event.name()
                + "! current state: " + session.state.name());
    }

    /**
     * De-queue the next request from session's local queue
     * @param session
     */
    public static void deQueueNextRequest(CwmpSession session) {
        if (session.outstandingRequest != null) {
            log.debug(session.cpeKey + ": " + session.state + ": Skipping de-queue due to outstanding request "
                    + session.outstandingRequest.methodName);
            // Do not de-queue next request while there is still outstanding request
            return;
        }

        CwmpRequest nextRequest = session.cwmpRequestQueue.poll();
        if (nextRequest != null) {
            session.outstandingRequest = nextRequest;
            session.state = CwmpSessionFsmStateEnum.PendingCpeResponse;

            // Send it
            session.sendCwmpMessageToCpe(nextRequest.rpcMessage);
        } else {
            /**
             * request queue is empty for now.
             */
            session.outstandingRequest = null;
            if (session.bTriggeredByConnReq) {
                // Wait for more NBI requests
                session.state = CwmpSessionFsmStateEnum.WaitingForNewNbiRequest;
                //session.startInactiveTimer(CpeServerConstants.CWMP_SESSION_NBI_INACTIVE_TIMEOUT);
            } else {
                // Terminate session
                session.terminate();
            }
        }
    }
}
