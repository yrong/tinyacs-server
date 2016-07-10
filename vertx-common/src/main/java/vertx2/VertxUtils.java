package vertx2;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VoidHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.file.AsyncFile;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.streams.Pump;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Date;
import java.util.Enumeration;

/**
 * Vert.x Misc Utils.
 *
 * @author: ronyang
 */
public class VertxUtils extends VertxDeployUtils{
    private static Logger log = LoggerFactory.getLogger(VertxUtils.class);

    /**
     * Constants
     */
    public static final long ONE_DAY = 1000 * 3600 * 24;
    public static final int MEGA_BYTES = 1024*1024;
    public static final int MAX_LINE_LENGTH = 80;
    public static final String LINE_OF_HASHES = buildStringWithSameChar('#', MAX_LINE_LENGTH) + "\n";
    public static final String LINE_OF_SPACES = buildStringWithSameChar(' ', MAX_LINE_LENGTH) + "\n";

    /**
     * Convert milliseconds (a long value) to Date string
     * @param milliseconds
     * @return
     */
    public static String msToDateString(long milliseconds) {
        return new Date(milliseconds).toString();
    }

    /**
     * Get local hostname.
     */
    private static String localHostname = null;
    public static String getLocalHostname() {
        /*
        if (localHostname == null) {
            try {
                localHostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                e.printStackTrace();
                localHostname = "localhost";
            }
        }
        */
        /*
        String all = "Interfaces: ";
        try
        {
            String comma = "";
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements())
            {
                NetworkInterface iface = interfaces.nextElement();
                if (iface.isLoopback() || !iface.isUp()) { continue; }
                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while(addresses.hasMoreElements())
                {
                    InetAddress addr = addresses.nextElement();
                    all += comma + iface.getDisplayName() + " " + addr.getHostAddress();
                    comma = ", ";
                }
            }
        }
        catch (Exception e) { log.error(e.getMessage()); }
        localHostname = all;
        */
        if (localHostname == null) {
            String hostName = VertxUtils.initStringProp(VertxConfigProperties.LOCAL_HOSTNAME_SYS_ENV_VAR, null);
            if (hostName == null) {
                try {
                    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                    {
                        while (interfaces.hasMoreElements()) {
                            NetworkInterface nic = interfaces.nextElement();
                            if (nic.isLoopback() || !nic.isUp()) {
                                continue;
                            }
                            Enumeration<InetAddress> addresses = nic.getInetAddresses();
                            while (hostName == null && addresses.hasMoreElements()) {
                                InetAddress address = addresses.nextElement();
                                if (!address.isLoopbackAddress() && (address instanceof Inet4Address)) {
                                    hostName = address.getCanonicalHostName();
                                    break;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
            localHostname = hostName;
        }
        return localHostname;
    }

    /**
     * Get local IP address.
     */
    private static String localIpAddress = null;
    public static String getLocalIpAddress() {
        if (localIpAddress == null) {
            String ipAddress = null;
            try {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                {
                    while (interfaces.hasMoreElements()) {
                        NetworkInterface nic = interfaces.nextElement();
                        if (nic.isLoopback() || !nic.isUp()) {
                            continue;
                        }
                        Enumeration<InetAddress> addresses = nic.getInetAddresses();
                        while (ipAddress == null && addresses.hasMoreElements()) {
                            InetAddress address = addresses.nextElement();
                            if (!address.isLoopbackAddress() && (address instanceof Inet4Address)) {
                                ipAddress = address.getHostAddress();
                                break;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
            localIpAddress = ipAddress;
        }
        return localIpAddress;
    }

    /**
     * Get the # of CPU Cores
     */
    public static int getNumberOfCpuCores() {
        return Runtime.getRuntime().availableProcessors();
    }
    /**
     * Get System Process of this process
     */
    private static String pid = null;
    public static String getPid() {
        if (pid == null) {
            pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        }
        return pid;
    }

    /**
     * Get a String to uniquely identify this process ([hostname].[pid])
     */
    public static String getHostnameAndPid() {
        return getLocalHostname() + "." + getPid();
    }

    /**
     * Get a String to uniquely identify this process ([hostname].[pid])
     */
    public static String getThreadName() {
        return Thread.currentThread().getName();
    }

    /**
     * Initialize a String property.
     * @return
     */
    public static String initStringProp(String env, String defaultValue) {
        String propValue = System.getenv(env);
        if(propValue == null) {
            propValue = System.getProperty(env);
        }
        if(propValue == null) {
            return defaultValue;
        } else {
            return propValue;
        }
    }

    /**
     * Initialize an int property.
     * @return
     */
    public static int initIntegerProp(String env, int defaultValue) {
        String propValue = System.getenv(env);
        if(propValue == null) {
            propValue = System.getProperty(env);
        }
        if(propValue == null) {
            return defaultValue;
        } else {
            return Integer.valueOf(propValue);
        }
    }

    /**
     * Initialize a Boolean property.
     * @return
     */
    public static boolean initBooleanProp(String env, boolean defaultValue) {
        String propValue = System.getenv(env);
        if(propValue == null) {
            propValue = System.getProperty(env);
        }
        if(propValue == null) {
            return defaultValue;
        } else {
            return Boolean.valueOf(propValue);
        }
    }

    /**
     * Write the payload to the response body.
     *
     * @param request
     * @param respPayload
     */
    public static void writeToResponse(HttpServerRequest request, String respPayload) {
        if (respPayload.startsWith("[") || respPayload.startsWith("{")) {
            request.response().putHeader("Content-Type", "application/json");
        } else {
            request.response().putHeader("Content-Type", "text/plain");
        }
        request.response().putHeader("Content-Length", String.valueOf(respPayload.length()));
        request.response().end(respPayload);
    }

    /**
     * Write the payload to the response body.
     *
     * @param request
     * @param respPayload
     * @param firstChunk Boolean Indicator shows whether this is the first chunk of a multi-chunk response
     * @param lastChunk  Boolean Indicator shows whether this is the last portion of a multi-chunk response
     */
    public static void writeToResponse(
            HttpServerRequest request,
            String respPayload,
            boolean firstChunk,
            boolean lastChunk) {
        // Setup Header if first chunk
        if (firstChunk) {
            request.response().setChunked(true);
            request.response().putHeader("Content-Type", "application/json");
        }

        // Write the chunk into response payload
        if (respPayload != null && respPayload.length() > 0) {
            request.response().write(respPayload, "UTF-8");
        }

        // End the response if last chunk
        if (lastChunk) {
            request.response().end();
        }
    }

    /**
     * Set the HTTP Status Code
     * @param request
     * @param status
     */
    public static void setResponseStatus(HttpServerRequest request, HttpResponseStatus status) {
        request.response().setStatusCode(status.code()); // Bad Request
        request.response().setStatusMessage(status.reasonPhrase());
    }

    /**
     * Respond to a bad HTTP Request.
     *
     * @param request
     * @param error
     */
    public static void badHttpRequest(HttpServerRequest request, String error) {
        responseWithStatusCode(HttpResponseStatus.BAD_REQUEST, request, error);
    }

    /**
     * Respond to a bad HTTP Request.
     *
     * @param request
     */
    public static void badHttpRequest(HttpServerRequest request) {
        responseWithStatusCode(HttpResponseStatus.BAD_REQUEST, request);
    }

    /**
     * Respond with a non-200-OK status code plus a error string.
     *
     * @param request
     * @param error
     */
    public static void responseWithStatusCode(HttpResponseStatus status, HttpServerRequest request, String error) {
        JsonObject errorJson = new JsonObject().putString("error", error);
        log.error(errorJson.encode());
        setResponseStatus(request, status);
        writeToResponse(request, errorJson.encodePrettily() + "\n");
    }

    /**
     * Respond with a non-200-OK status code.
     *
     * @param request
     */
    public static void responseWithStatusCode(HttpResponseStatus status, HttpServerRequest request) {
        VertxUtils.setResponseStatus(request, status);
        request.response().end();
    }

    /**
     * Respond with a 200-OK status code.
     *
     * @param request
     */
    public static void responseSuccess(HttpServerRequest request,String payload) {
        VertxUtils.setResponseStatus(request, HttpResponseStatus.OK);
        VertxUtils.writeToResponse(request,payload);
    }

    /**
     * Get Module Version by Artifact Id
     *
     * This is achieved by looking up all the modules within the current working directory.
     *
     * @param groupName
     * @param artifactId
     */
    public static String getModuleVersion(String groupName, String artifactId) {
        String prefix = groupName + "~" + artifactId + "~";

        /**
         * List all sub-directories under "mods"
         */
        File modsDir = new File("mods");
        File[] filesList = modsDir.listFiles();
        for (File file : filesList) {
            if (file.isDirectory()) {
                String modName = file.getName();
                if (modName.startsWith(prefix)) {
                    return modName.substring(prefix.length(), modName.length());
                }
            }
        }

        log.error("Unable to find module " + artifactId + " in mods!");
        return null;
    }

    /**
     * Get the full Vert.X Module Name.
     *
     * @param groupName
     * @param artifactId
     * @return
     */
    public static String getFullModName(String groupName, String artifactId) {
        return groupName + "~" + artifactId + "~" + getModuleVersion(groupName, artifactId);
    }

    /**
     * Display the JAVA Runtime Heap Info and the # of CPU Cores/Speed
     * @param vertx
     */
    public static void displayJvmHeapInfo(Vertx vertx) {
        //Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();

        log.info("\n"
            + LINE_OF_HASHES
            + LINE_OF_HASHES
            + encloseStrWithHashes(" ")
            + encloseStrWithHashes("JVM Heap Info:")
            + encloseStrWithHashes(" ")
            + encloseStrWithHashes("JVM Max Memory:      " + runtime.maxMemory() / MEGA_BYTES + "MB")
            + encloseStrWithHashes("JVM Total Memory:    " + runtime.totalMemory() / MEGA_BYTES + "MB")
            + encloseStrWithHashes("JVM Free Memory:     " + runtime.freeMemory() / MEGA_BYTES + "MB")
            + encloseStrWithHashes("JVM Used Memory:     " + (runtime.totalMemory() - runtime.freeMemory()) / MEGA_BYTES + "MB")
            + encloseStrWithHashes(" ")
            + LINE_OF_HASHES
            + LINE_OF_HASHES
        );
    }

    /**
     * Display the content of build-info.txt
     * @param vertx
     */
    public static void displayBuildInfo(Vertx vertx) {
        // try reading "build-info.txt"
        final String filePath = "build-info.txt";
        vertx.fileSystem().readFile(
                filePath,
                new AsyncResultHandler<Buffer>() {
                    public void handle(AsyncResult<Buffer> ar) {
                        if (ar.succeeded()) {
                            if (ar.result() != null) {
                                log.info(highlightWithHashes("Build Info:\n" + ar.result()));

                                if (!ar.result().toString().contains("buildUser=\"toolssw\"")) {
                                    log.info(highlightWithHashes("Attention: You are running a private build!"));
                                }
                            } else {
                                log.error(highlightWithHashes("File " + filePath + " is empty!"));
                            }
                        } else {
                            log.error(highlightWithHashes(
                                    "File " + filePath + " does not exist!" + " (" + ar.cause() + ")"));
                        }
                    }
        });
    }

    /**
     * Build a External/Public Vert.x Event Bus Address in the format of "[hostname]/[pid]/[address]"
     *
     * @param address       Event Bus Address
     */
    public static String getPublicVertxEventBusAddress(String address) {
        return getHostnameAndPid() + "." + address;
    }

    /**
     * Build an URL with an Vert.x Event Bus Address
     *
     * "vertx://[hostname]/[pid]/[address]
     *
     * @param address       Event Bus Address
     */
    public static String getVertxEventBusUrl(String address) {
        return VertxConstants.URL_PROTOCOL_VERTX_EVENT_BUS + "://" + getPublicVertxEventBusAddress(address);
    }

    /**
     * Extract Vert.x event bus address from a Vert.X based URL String.
     *
     * @param url
     */
    public static String getVertxEventBusAddressFromUrl(String url) {
        return url.substring(VertxConstants.URL_PROTOCOL_VERTX_EVENT_BUS.length() + 3);
    }

    /**
     * Highlight a string with hashes and spaces via multi lines.
     * @param str
     */
    public static String highlightWithHashes(String str) {
        int len = str.length();
        if (len > 80 || str.contains("\n")) {
            return "\n" + LINE_OF_HASHES + LINE_OF_HASHES + "\n" + str + "\n" + LINE_OF_HASHES + LINE_OF_HASHES;
        } else {
            String spaces = buildStringWithSameChar(' ', len);
            String hashes = buildStringWithSameChar('#', len);

            return "\n" +
                    "##########" + hashes + "##########\n" +
                    "########  " + spaces + "  ########\n" +
                    "########  " + str +    "  ########\n" +
                    "########  " + spaces + "  ########\n" +
                    "##########" + hashes + "##########\n";
        }
    }

    /**
     * Enclose a string with hashes.
     */
    public static String encloseStrWithHashes(String original) {
        if (original.length() < (MAX_LINE_LENGTH -20)) {
            return "########  "
                    + original + buildStringWithSameChar(' ', MAX_LINE_LENGTH - 20 - original.length())
                    + "  ########\n";
        } else {
            return "########  " + original + "  ########\n";
        }
    }

    public static String buildStringWithSameChar(char aChar, int count) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < count; i ++) {
            stringBuilder.append(aChar);
        }

        return stringBuilder.toString();
    }

    /**
     * Serve an HTTP File Download Request.
     *
     * @param vertx
     * @param request
     * @param filename
     */
    public static void serveHttpFileDownloadRequest(Vertx vertx, final HttpServerRequest request, final String filename) {
        log.debug("Downloading " + filename + "...");

        request.response().setChunked(true);

        vertx.fileSystem().open(filename, new AsyncResultHandler<AsyncFile>() {
            public void handle(AsyncResult<AsyncFile> ar) {
                if (ar.failed()) {
                    request.resume();
                    VertxUtils.badHttpRequest(request, filename +" not found!");
                    return;
                }

                /**
                 * Setup a pump from file to HTTP request
                 */
                final AsyncFile asyncFile = ar.result();
                final Pump pump = Pump.createPump(asyncFile, request.response());

                /**
                 * Setup Async end handler
                 */
                asyncFile.endHandler(new VoidHandler() {
                    public void handle() {
                        request.response().end();
                    }
                });

                /**
                 * Start the pump
                 */
                pump.start();
            }
        });

    }
}
