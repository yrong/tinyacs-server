package vertx.acs.nbi.model;

import vertx.VertxUtils;
import vertx.acs.nbi.AbstractAcNbiCrudService;
import vertx.util.AcsConstants;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Project:  cwmp CCNG ACS API
 *
 * The ACS API supports both HTTP transport as well as Vert.x Event Bus transport for internal clients.
 *
 * This class defines a super-set of both transport protocols so the actual service implementations can then focus
 * on the real business logic.
 *
 * @author: ronyang
 */
public class AcsNbiRequest {
    public HttpServerRequest   httpServerRequest = null;
    public Message<JsonObject> vertxRequestMessage = null;

    // Internal vs. external
    public boolean bInternalRequest = true;

    // The request body
    public JsonObject body;

    /**
     * Save teh URL Path Params for later uses
     */
    public String[] urlPathParams = null;

    /**
     * Query Parameters
     */
    public JsonObject queryParameters = null;

    // Custom Service Specific Data
    public Object serviceData = null;

    /**
     * Constructor for HTTP request
     *
     * @param httpServerRequest
     * @param body
     */
    public AcsNbiRequest (HttpServerRequest   httpServerRequest, JsonObject body, boolean bInternalRequest) {
        this.httpServerRequest = httpServerRequest;
        this.body = body;
        this.bInternalRequest = bInternalRequest;
    }

    /**
     * Constructor for Vert.x Event Bus Request
     *
     * @param vertxRequestMessage
     * @param body
     */
    public AcsNbiRequest (Message<JsonObject> vertxRequestMessage, JsonObject body) {
        this.vertxRequestMessage = vertxRequestMessage;
        this.body = body;
    }

    /**
     * Get HTTP Response Status (POJO) by a string.
     *
     * @param statusString
     */
    public static HttpResponseStatus getHttpResponseStatusByString(String statusString) {
        // extract the integer code
        int code = Integer.valueOf(statusString.substring(0, statusString.indexOf(" ")));
        return HttpResponseStatus.valueOf(code);
    }

    /**
     * Send a JsonObject Response.
     *
     * @param status     (HTTP) Status Code
     *                   The HTTP Response Status Code is now the common status code for both transport protocols.
     * @param result     Optional Result
     * @param firstChunk Boolean Indicator shows whether this is the first chunk of a multi-chunk response
     * @param lastChunk  Boolean Indicator shows whether this is the last portion of a multi-chunk response
     */
    public void sendResponse(
            HttpResponseStatus status,
            JsonObject result,
            boolean firstChunk,
            boolean lastChunk) {
        if (httpServerRequest != null) {
            // Send HTTP Response
            VertxUtils.setResponseStatus(httpServerRequest, status);

            VertxUtils.writeToResponse(httpServerRequest, result.encodePrettily() + "\n", firstChunk, lastChunk);
        } else {
            // Build Vert.x Response
            JsonObject response;
            if (result != null) {
                result.putString("status", status.toString());
                response = result;
            } else {
                response = new JsonObject().putString("status", status.toString());
            }

            /**
             * TODO: Add code to handle multi chunk mode.
             */

            // Send Response via Vert.x event bus
            vertxRequestMessage.reply(response);
        }
    }

    /**
     * Send a JsonObject Response.
     *
     * @param status    (HTTP) Status Code
     *                  The HTTP Response Status Code is now the common status code for both transport protocols.
     * @param result    Optional Result
     */
    public void sendResponse(HttpResponseStatus status, JsonObject result) {
        if (httpServerRequest != null) {
            // Send HTTP Response
            VertxUtils.setResponseStatus(httpServerRequest, status);

            if (result != null) {
                VertxUtils.writeToResponse(httpServerRequest, result.encodePrettily() + "\n");
            } else {
                httpServerRequest.response().end();
            }
        } else {
            // Build Vert.x Response
            JsonObject response;
            if (result != null) {
                result.putString("status", status.toString());
                response = result;
            } else {
                response = new JsonObject().putString("status", status.toString());
            }

            // Send Response via Vert.x event bus
            vertxRequestMessage.reply(response);
        }
    }

    /**
     * Send a String Response.
     *
     * Usually used when performing a bulk query.
     *
     * @param status    (HTTP) Status Code
     *                  The HTTP Response Status Code is now the common status code for both transport protocols.
     * @param resultString
     */
    public void sendResponse(HttpResponseStatus status, String resultString) {
        if (httpServerRequest != null) {
            // Send HTTP Response
            VertxUtils.setResponseStatus(httpServerRequest, status);

            if (resultString != null) {
                VertxUtils.writeToResponse(httpServerRequest, resultString);
            } else {
                httpServerRequest.response().end();
            }
        } else {
            // Build Vert.x Response
            JsonObject response = new JsonObject().putString("status", status.toString());
            if (resultString != null) {
                response.putString("result", resultString);
            }

            // Send Response via Vert.x event bus
            vertxRequestMessage.reply(response);
        }
    }

    /**
     * Send Empty Response.
     *
     * @param status    (HTTP) Status Code
     *                  The HTTP Response Status Code is now the common status code for both transport protocols.
     */
    public void sendResponse(HttpResponseStatus status) {
        if (httpServerRequest != null) {
            // Send HTTP Response
            VertxUtils.setResponseStatus(httpServerRequest, status);

            httpServerRequest.response().end();
        } else {
            // Build Vert.x Response
            JsonObject response = new JsonObject().putString("status", status.toString());

            // Send Response via Vert.x event bus
            vertxRequestMessage.reply(response);
        }
    }

    /**
     * Send a JsonArray Response.
     *
     * Usually used when performing a bulk query.
     *
     * @param status    (HTTP) Status Code
     *                  The HTTP Response Status Code is now the common status code for both transport protocols.
     * @param results   Optional Result
     * @param hasMore   More data records to come.
     */
    public void sendResponseChunk(HttpResponseStatus status, JsonArray results, boolean hasMore) {
        sendResponseChunk(status, results, 0, results == null? 0 : results.size(), hasMore);
    }

    /**
     * Send a JsonArray Response.
     *
     * Usually used when performing a bulk query.
     *
     * @param status    (HTTP) Status Code
     *                  The HTTP Response Status Code is now the common status code for both transport protocols.
     * @param results   Optional Result
     * @param skip      If non zero, Skip the first # of records in the results array
     * @param limit     If greater than zero, specify the max # of records needed from the results array
     * @param hasMore   More data records to come.
     */
    public void sendResponseChunk(
            HttpResponseStatus status,
            JsonArray results,
            int skip,
            int limit,
            boolean hasMore) {
        if (httpServerRequest != null) {
            StringBuilder chunk = new StringBuilder();

            // Build response chunk
            if (results != null) {
                int start = skip > 0 ? skip : 0;
                int end = limit > 0? (skip + limit) : results.size();
                for (int i = start; i < end; i ++) {
                    if (i >= results.size()) {
                        // Do not overflow the array
                        break;
                    }
                    JsonObject oneElement = results.getJsonObject(i);
                    if (i < (results.size() - 1)) {
                        chunk.append(oneElement.encodePrettily() + ",");
                    } else {
                        chunk.append(oneElement.encodePrettily());
                    }
                }
            }

            if (hasMore == false) {
                // Append the ']' to mark the end of the JSON Array
                chunk.append("]\n");
            }

            // Write the chunk
            httpServerRequest.response().write(chunk.toString());

            if (hasMore == false) {
                // End the response if no more chunks
                httpServerRequest.response().end();
            }
        } else {
            // Build Vert.x Response
            JsonObject response = new JsonObject().put(AcsConstants.FIELD_NAME_STATUS_CODE, status.toString());
            if (hasMore) {
                response.put(AcsConstants.FIELD_NAME_MORE_EXIST, true);
            }
            if (results != null) {
                response.put(AcsConstants.FIELD_NAME_RESULT, results);
            }

            // Send Response via Vert.x event bus
            vertxRequestMessage.reply(response);
        }
    }

    /**
     * Return the value of the given query parameter name.
     * @param paramName
     * @return
     */
    public Object getQueryParam(String paramName) {
        if (queryParameters == null) {
            return null;
        }

        return queryParameters.getField(paramName);
    }

    /**
     * Get the skip count.
     */
    public int getQuerySkipCount() {
        Integer intValue = (Integer)getQueryParam(AbstractAcNbiCrudService.QUERY_KEYWORD_SKIP);
        return intValue == null? 0 : intValue;
    }

    /**
     * Get the limit count.
     */
    public int getQueryLimitCount() {
        Integer intValue = (Integer)getQueryParam(AbstractAcNbiCrudService.QUERY_KEYWORD_LIMIT);
        return intValue == null? 0 : intValue;
    }

    /**
     * Get the query "brief" value.
     */
    public boolean getQueryBrief() {
        Boolean brief = (Boolean)getQueryParam(AbstractAcNbiCrudService.QUERY_KEYWORD_BRIEF);
        return brief == null? false : brief;
    }

    /**
     * Get Service Data.
     * @param <T>
     * @return
     */
    public <T> T getServiceData() {
        return (T)serviceData;
    }
}
