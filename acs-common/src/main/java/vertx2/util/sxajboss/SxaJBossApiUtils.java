package vertx2.util.sxajboss;

import vertx2.VertxException;
import vertx2.VertxHttpClientUtils;
import vertx2.VertxUtils;
import vertx2.util.AcsConfigProperties;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientResponse;

/**
 * Project:  cwmp
 *
 * SXA JBoss API Utils
 *
 * @author: ronyang
 */
public class SxaJBossApiUtils {
    private static final Logger log = LoggerFactory.getLogger(SxaJBossApiUtils.class);

    /**
     * Constants
     */
    public static String CWMP_BROKER_BASE_URL;
    public static final int DEFAULT_JBOSS_TIMEOUT = 10000;

    /**
     * Static Exception(s)
     */
    public static final VertxException CWMP_JBOSS_UNDEFINED =
            new VertxException("SXA JBOSS Server Hostname Not Defined!");
    public static final VertxException CWMP_JBOSS_UNREACHABLE =
            new VertxException("Unable to reach SXA JBOSS Server!");
    public static final VertxException UNABLE_TO_GET_BASE_URL_FROM_BROKER =
            new VertxException("Unable to get the base URL from SXA Broker!");

    /**
     * Static HTTP Client (to be initialized)
     */
    public static HttpClient jbossApiHttpClient = null;

    /**
     * Initialize the HTTP Client
     * @param vertx
     */
    public static void initHttpClient(Vertx vertx) {
        if (jbossApiHttpClient != null) {
            return;
        }

        if (AcsConfigProperties.CWMP_JBOSS_API_HOST == null) {
            log.error(
                    VertxUtils.highlightWithHashes(
                    "Please initialize the " + AcsConfigProperties.CWMP_JBOSS_API_HOST_SYS_ENV_VAR + " environment var!")
            );
            return;
        }
        log.info(VertxUtils.highlightWithHashes("SXA JBoss Host/Port: "
                        + AcsConfigProperties.CWMP_JBOSS_API_HOST + ":" + AcsConfigProperties.CWMP_JBOSS_API_PORT));

        // Create a new HTTP Client
        jbossApiHttpClient = vertx.createHttpClient()
                .setHost(AcsConfigProperties.CWMP_JBOSS_API_HOST)
                .setPort(AcsConfigProperties.CWMP_JBOSS_API_PORT);

        // Init Broker Base URL
        CWMP_BROKER_BASE_URL = "http://" + AcsConfigProperties.CWMP_JBOSS_API_HOST + ":"
                + AcsConfigProperties.CWMP_JBOSS_API_PORT + "/sxa-broker/rest/module-registry/url/";
    }

    /**
     * Make an SXA JBOSS API Call.
     *
     * @param moduleName
     * @return
     */
    public static void sendRequest(
            final String moduleName,
            final HttpMethod method,
            final String urlPath, // the partial URL path with optional query parameters but without the module base URL
                                  // starting with a "/"
            final String username,
            final String password,
            final String payload,
            final Handler<HttpClientResponse> responseHandler,
            final Handler<Throwable> exceptionHandler
    ) {
        if (jbossApiHttpClient == null) {
            if (exceptionHandler != null) {
                exceptionHandler.handle(CWMP_JBOSS_UNDEFINED);
            }  else {
                log.error("SXA JBoss Server Hostname is undefined!");
            }
            return;
        }

        /**
         * Define a handler to be called after getting the module base URL
         */
        Handler<HttpClientResponse> moduleBaseUrlHandler = new Handler<HttpClientResponse> () {
            @Override
            public void handle(final HttpClientResponse jbossResponse) {
                jbossResponse.bodyHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer body) {
                        if (HttpResponseStatus.OK.code() == jbossResponse.statusCode()) {
                            String moduleBaseUrl = body.toString();
                            log.debug("Base URL for " + moduleName + " is " + moduleBaseUrl);

                            /**
                             * Now make the actual API call
                             *
                             * For example "http://vmlnx-sxa06.calix.local:8080/plugin-cc-0.0.21.DASH-SNAPSHOT/rest"
                             * for "plugin-cc".
                             */
                            log.debug("Making an HTTP " + method.name() + " request to " + moduleBaseUrl + urlPath
                                    + ", Credentials: " + username + ":" + password + ", Payload: " + payload);

                            VertxHttpClientUtils.sendHttpRequest(
                                    moduleBaseUrl + urlPath,
                                    jbossApiHttpClient,
                                    method,
                                    username,
                                    password,
                                    payload,
                                    // Response Handler
                                    responseHandler,
                                    exceptionHandler,
                                    DEFAULT_JBOSS_TIMEOUT
                            );
                        } else {
                            log.error(VertxUtils.highlightWithHashes(
                                "Unable to get the module base URL from SXA broker for " + moduleName + "!"
                                + " HTTP Status: " + jbossResponse.statusCode() + " " + jbossResponse.statusMessage()
                            ));
                            exceptionHandler.handle(UNABLE_TO_GET_BASE_URL_FROM_BROKER);
                        }
                    }
                });
            }
        };

        /**
         * Get the base URL of a given module by sending an HTTP GET to the SXA Broker.
         *
         * For example "http://vmlnx-sxa06.calix.local:8080/plugin-cc-0.0.21.DASH-SNAPSHOT/rest" for "plugin-cc".
         */
        VertxHttpClientUtils.sendHttpRequest(
                CWMP_BROKER_BASE_URL + moduleName,
                jbossApiHttpClient,
                HttpMethod.GET,
                null,
                null,
                null,
                // Response Handler
                moduleBaseUrlHandler,
                exceptionHandler,
                DEFAULT_JBOSS_TIMEOUT
        );
    }
}
