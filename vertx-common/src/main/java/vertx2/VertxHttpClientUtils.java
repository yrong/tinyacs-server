package vertx2;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.http.auth.AUTH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.impl.Base64;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Vert.x HTTP Client Utils.
 *
 * @author: ronyang
 */
public class VertxHttpClientUtils {
    private static final Logger log = LoggerFactory.getLogger(VertxHttpClientUtils.class.getName());

    /**
     * Static Vert.x Instance
     *
     * Must be initialized by main vertice.
     */
    public static Vertx vertx;

    /**
     * Initialize the Vert.X Instance
     *
     * @param vertxInstance
     */
    public static void init(Vertx vertxInstance) {
        vertx = vertxInstance;
    }

    /**
     * A Pool of Vert.x HttpClient Instances
     */
    public static Map<String, HttpClient> httpClientMap = new HashMap<>();

    /**
     * Lookup HTTP Client Instance (from Hash Map)by URL String.
     *
     * If not found, create a new one.
     *
     * TODO: When do we delete client instances?
     *
     * @param urlString
     * @return  An existing HTTP Client instance, or a new one.
     * @throws java.net.MalformedURLException
     */
    private static HttpClient lookupHttpClient(String urlString) throws MalformedURLException {
        /**
         * Convert to URL by String
         */
        URL url = new URL(urlString);
        String baseUrlString = url.getHost() + ":" + url.getPort();

        /**
         * Look up existing clients for the same [host:port] the hash map
         */
        HttpClient client = httpClientMap.get(baseUrlString);
        if (client == null) {
            log.info("Creating new HTTP Client for " + baseUrlString);
            client = vertx.createHttpClient()
                    .setHost(url.getHost())
                    .setPort(url.getPort());
            if (url.getProtocol().equalsIgnoreCase("https")) {
                client.setSSL(true);
                client.setVerifyHost(false);
            }
            httpClientMap.put(baseUrlString, client);
        }

        return client;
    }

    /**
     * Create a new HTTP Client by URL String.
     *
     * @param urlString
     * @param vertxInstance
     *
     * @return
     * @throws java.net.MalformedURLException
     */
    public static HttpClient createHttpClient(Vertx vertxInstance, String urlString) throws MalformedURLException {
        /**
         * Convert to URL by String
         */
        URL url = new URL(urlString);
        return vertxInstance.createHttpClient()
                .setHost(url.getHost())
                .setPort(url.getPort());
    }

    /**
     * A friendly wrapper to send async HTTP request.
     *
     * @param url           The URL String without the hostname and port #
     * @param httpMethod    HTTP Method (GET/PUT/POST/DELETE)
     * @param username
     * @param password
     * @param payload
     * @param responseHandler
     * @param exceptionHandler
     * @param timeoutMs
     * @throws java.net.MalformedURLException
     */
    public static void sendHttpRequest(
            String  url,
            HttpMethod httpMethod,
            String username,
            String password,
            String payload,
            Handler<HttpClientResponse> responseHandler,
            Handler<Throwable> exceptionHandler,
            long timeoutMs) {
        // Get HTTP Client Instance
        HttpClient httpClient = null;
        try {
            httpClient = lookupHttpClient(url);
        } catch (MalformedURLException e) {
            exceptionHandler.handle(e);
        }

        sendHttpRequest(url, httpClient, httpMethod, username, password, payload, responseHandler, exceptionHandler, timeoutMs);
    }


    /**
     * A friendly wrapper to send async HTTP request.
     *
     * @param url           The URL String without the hostname and port #
     * @param httpMethod    HTTP Method (GET/PUT/POST/DELETE)
     * @param username
     * @param password
     * @param payload
     * @param responseHandler
     * @param exceptionHandler
     * @param timeoutMs
     * @throws java.net.MalformedURLException
     */
    public static void sendHttpRequest(
            String  url,
            HttpClient httpClient,
            HttpMethod httpMethod,
            String username,
            String password,
            String payload,
            Handler<HttpClientResponse> responseHandler,
            Handler<Throwable> exceptionHandler,
            long timeoutMs) {
        log.info("Sending payload to " + url + ":\n" + payload);

        // Build the request
        HttpClientRequest request = httpClient.request(httpMethod.name(), url, responseHandler);

        // Optional Exception Handler
        if (exceptionHandler != null) {
            request.exceptionHandler(exceptionHandler);
        }

        // Set timeout (in ms)
        request.setTimeout(timeoutMs);

        // Basic Auth Header
        if (username != null) {
            String credentials = username + ":" + password;
            request.headers().set(
                    AUTH.WWW_AUTH_RESP,
                    "Basic " + Base64.encodeBytes(credentials.getBytes())
            );
        }

        // Payload
        if (payload != null && payload.length() > 0) {
            // Content Type/Length
            request.headers().set("Content-Type", "application/json");
            request.headers().set("Content-Length", String.valueOf(payload.length()));
            request.write(payload);
        } else {
            request.headers().set("Content-Length", "0");
        }

        request.end();
    }

    /**
     * Get the [host:port] from HTTP Client.
     *
     * @param client
     * @return
     */
    public static String getUrlFromHttpClient(HttpClient client) {
        return client.getHost() + ":" + client.getPort();
    }
}
