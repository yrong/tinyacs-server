package vertx;


import io.vertx.core.http.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.auth.AUTH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

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
            HttpClientOptions options = new HttpClientOptions().setDefaultHost("url.getHost()").setDefaultPort(80);
            if (url.getProtocol().equalsIgnoreCase("https")) {
                options.setSsl(true);
                options.setVerifyHost(false);
            }
            client = vertx.createHttpClient(options);
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
        HttpClientOptions options = new HttpClientOptions().setDefaultHost(url.getHost()).setDefaultPort(url.getPort());
        return vertxInstance.createHttpClient(options);
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
        HttpClientRequest request = httpClient.request(httpMethod, url, responseHandler);

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
                    "Basic " + Base64.encodeBase64String(credentials.getBytes())
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

}
