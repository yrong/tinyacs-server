package vertx.cpe.sim;

import vertx.cwmp.CwmpInformEventCodes;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.platform.Verticle;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Project:  CPE Simulator
 *
 * CWMP Diagnostics Worker Vertice.
 *
 * @author: ronyang
 */
public class CpeDiagWorkerVertice extends Verticle {
    private static final Logger log = LoggerFactory.getLogger(CpeDiagWorkerVertice.class.getName());

    /**
     * Event Bus
     */
    EventBus eventBus;

    /**
     * Start the Vertice
     */
    public void start() {
        log.info("Starting a CPE Diag Worker Verticle...");

        /**
         * Save event bus
         */
        eventBus = vertx.eventBus();

        /**
         * Register Handler for new diag requests
         */
        eventBus.registerLocalHandler(
                CpeSimConstants.VERTX_ADDRESS_DIAG_REQUEST,
                new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> event) {
                        JsonObject cpe = event.body().getObject("cpe");
                        String diagType = event.body().getString("diagType");
                        JsonObject diagArgs = event.body().getObject("diagArgs");

                        log.info("CPE " + cpe.getString("_id") + ": Starting " + diagType + ".\n"
                                + diagArgs.encodePrettily());

                        switch (diagType) {
                            case "InternetGatewayDevice.IPPingDiagnostics":
                                ipPing(cpe, diagType, diagArgs);
                                break;

                            default:
                                break;
                        }
                    }
                }
        );
    }

    /**
     * Perform an IP-Ping Diagnostics
     *
     * @param cpe
     * @param diagType
     * @param args
     */
    public void ipPing(JsonObject cpe, String diagType, JsonObject args) {
        JsonObject results = new JsonObject();
        StringBuffer output = new StringBuffer();

        // Build command line string
        String host = args.getString("Host");
        String dataBlockSize = args.getString("DataBlockSize");
        String timeout = args.getString("Timeout");
        String numberOfRepetitions = args.getString("NumberOfRepetitions");
        String command = "ping";
        int total;
        if (dataBlockSize != null)
            command = command + " -s " + dataBlockSize;
        if (timeout != null)
            command = command + " -W " + (Integer.valueOf(timeout) / 1000);
        if (numberOfRepetitions != null) {
            command = command + " -c " + numberOfRepetitions;
            total = Integer.valueOf(numberOfRepetitions);
        } else {
            total = 4;
            command = command + " -c 4";
        }
        command = command + " " + host;
        log.info("Executing command line \"" + command + "\"...");

        // Run an external ping process and wait for result
        Process p;
        try {
            p = Runtime.getRuntime().exec(command);
            p.waitFor();
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = reader.readLine())!= null) {
                log.info(line);
                if (line.contains("unknown host")) {
                    results.putString(diagType + ".DiagnosticsState", "Error_CannotResolveHostName");
                    break;
                } else if (line.contains("packets transmitted")) {
                    // For example "4 packets transmitted, 4 received, 0% packet loss, time 3030ms"
                    String[] segments = StringUtil.split(line, ' ');
                    int successCount = Integer.valueOf(segments[3]);
                    int failCount = total - successCount;
                    results.putString(diagType + ".SuccessCount", String.valueOf(successCount));
                    results.putString(diagType + ".FailureCount", String.valueOf(failCount));
                } else if (line.contains("min/avg/max/")) {
                    // for example "rtt min/avg/max/mdev = 25.412/25.621/25.815/0.183 ms"
                    String temp = line.substring(line.indexOf("=") + 2);
                    String[] segments = StringUtil.split(temp, '/');
                    results.putString(diagType + ".MinimumResponseTime", segments[0]);
                    results.putString(diagType + ".AverageResponseTime", segments[1]);
                    results.putString(diagType + ".MaximumResponseTime", segments[2]);
                    results.putString(diagType + ".DiagnosticsState", "Complete");
                }
                output.append(line + "\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!results.containsField(diagType + ".DiagnosticsState")) {
            results.putString(diagType + ".DiagnosticsState", "Error_CannotResolveHostName");
            results.putString(diagType + ".SuccessCount", "0");
            results.putString(diagType + ".FailureCount", "0");
            results.putString(diagType + ".MinimumResponseTime", "0");
            results.putString(diagType + ".AverageResponseTime", "0");
            results.putString(diagType + ".MaximumResponseTime", "0");
        }

        log.info("IP Ping Diag Completed with results:\n" + results.encodePrettily());

        // Start a new DIAG Complete Session
        startDiagCompleteSession(cpe, results);
    }

    /**
     * Start a Diag-Complete Session.
     *
     * @param cpe
     * @param results
     */
    public void startDiagCompleteSession(JsonObject cpe, JsonObject results) {
        JsonObject message = new JsonObject()
                .putString("orgId", cpe.getString("orgId"))
                .putNumber("sn", Long.decode("0x" + cpe.getString("serialNumber")))
                .putString("eventCode", CwmpInformEventCodes.DIAGNOSTICS_COMPLETE)
                .putObject("newValues", results);

        // Start a new session by sending an event to one of the session vertices
        vertx.eventBus().send(CpeSimConstants.VERTX_ADDRESS_NEW_SESSION, message);
    }
}