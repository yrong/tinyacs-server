package vertx2.cpeserver.session;

import vertx2.VertxUtils;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import dslforumOrgCwmp12.FaultDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Project:  ccng-acs
 *
 * This class defines the requests that can be sent to an active CMWP session.
 *
 * By the current design, the request could come from the following 2 sources:
 *
 *  1. GUI frontend (via REST interface), which requires async callback handling.
 *  2. ACS engine after comparing the CPE's current configurations against the desired configurations, which may not
 *     require callback handling.
 *
 *  Either way, timeout shall be enforced via a centralized scheduler.
 *
 * @author: ronyang
 */
public class CwmpRequest {
    private static final Logger log = LoggerFactory.getLogger(CwmpRequest.class.getName());

    // Requester Name Constants
    public static final String CWMP_REQUESTER_LOCAL = "acs-core";    // local requests
    public static final String CWMP_REQUESTER_ACS = "acs";              // external requests

    // Requester Identifier String
    public String requester = "";

    // The CWMP RPC message (the request to be or have been sent to the CPE)
    public CwmpMessage rpcMessage = null;

    // The Callback response handler
    Handlers handlers;

    // Request RPC Method Name
    public String methodName;

    // TODO: add Async Callback context info

    // Timestamps
    long createTime = 0;    // time when the request was added to the internal request queue
    long execTime = 0;      // time when the request was sent to the CPE (after consumed out of the queue)

    /**
     * Constructor Method.
     *
     * @param requester
     * @param msg
     */
    public CwmpRequest(
            String requester,
            CwmpMessage msg,
            String methodName,
            Handlers handlers) {
        this.requester = requester;
        rpcMessage = msg;
        this.methodName = methodName;
        createTime = System.currentTimeMillis();
        this.handlers = handlers;
    }

    /**
     * Interface Definition for Various Handlers that must be extended by actual requests
     */
    public interface Handlers {
        /**
         * Abstract Response Handler Class that must be extended by actual requests
         */
        public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
                throws CwmpException;

        /**
         * Abstract Fault Response Handler Class that must be extended by actual requests
         */
        public void faultHandler(CwmpSession session, CwmpRequest request, FaultDocument.Fault cwmpFault)
                throws CwmpException;

        /**
         * Abstract Timeout Handler Class that must be extended by actual requests
         *
         * Currently the servlet is configured to timeout inactive HTTP sessions after 3 minutes.
         *
         * If the request has already been sent to the CPE but no response has been received when the session is being
         * timed out, this handler shall be called.
         */
        public void timeoutHandler(CwmpSession session, CwmpRequest request)
                throws CwmpException;

        /**
         * Abstract Drain Handler Class that must be extended by actual requests
         *
         * When a session is being timed out, all the requests that are still in the queue (i.e. have not been sent to
         * the CPE) will be drained out of the queue and this handler will be called for those requests being drained.
         */
        public void drainHandler(CwmpSession session, CwmpRequest request)
                throws CwmpException;

    }

    @Override
    public String toString() {
        String str = "methodName: " + methodName +
                ", requested by " + requester +
                " at " + VertxUtils.msToDateString(createTime);
        if (execTime > 0) {
            str += ", sent to CPE at " + VertxUtils.msToDateString(execTime);
        }

        return str;
    }
}
