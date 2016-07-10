package vertx2.cpeserver.deviceop;

import com.calix.sxa.VertxJsonUtils;
import vertx2.cpeserver.session.CwmpRequest;
import vertx2.cpeserver.session.CwmpSession;
import vertx2.cwmp.CwmpException;
import vertx2.cwmp.CwmpMessage;
import vertx2.model.ConfigurationCategory;
import vertx2.model.CpeDeviceOp;
import dslforumOrgCwmp12.ParameterNames;
import dslforumOrgCwmp12.ParameterValueList;
import dslforumOrgCwmp12.ParameterValueStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.xmlsoap.schemas.soap.envelope.Body;

/**
 * Project:  SXA-CC (aka CCFG)
 *
 * Handle Dynamic Objects creation/update (required when applying some special profile like DNS-host-mapping).
 *
 * @author: ronyang
 */
public class DynamicObject extends SetParameterValuesNbi.CpeSetParameterValuesNbiResponseHandler {
    private static final Logger log = LoggerFactory.getLogger(AddObject.class.getName());

    /**
     * POJO Attributes
     */
    public CwmpSession session;
    public JsonObject keyParameter;
    public String keyParameterName;
    public String keyParameterValue;
    public String pathPrefix;
    public JsonObject paramValues;
    public DynamicObject nextDynamicObject;

    /**
     * Start the procedure that processes one or more dynamic objects.
     *
     * @param session
     * @param parentDeviceOp
     */
    public static void start(JsonObject parentDeviceOp, CwmpSession session) {
        /**
         * Create a new DynamicObject POJO for each item in the "dynamicObjects" array
         */
        JsonArray rawDynamicObjects= parentDeviceOp.getArray(CpeDeviceOp.FIELD_NAME_DYNAMIC_OBJECTS);
        DynamicObject[] dynamicObjects = new DynamicObject[rawDynamicObjects.size()];
        for (int i = 0; i < dynamicObjects.length; i++) {
            dynamicObjects[i] = new DynamicObject(
                    parentDeviceOp,
                    session,
                    (JsonObject) rawDynamicObjects.get(i));
        }

        /**
         * Chain them together
         */
        for (int i = 0; i < (dynamicObjects.length - 1); i++) {
            dynamicObjects[i].nextDynamicObject = dynamicObjects[i + 1];
        }

        /**
         * Kick off the first one
         */
        dynamicObjects[0].start();
    }

    /**
     * The deviceOps are passed in during the Construction.
     *
     * @param deviceOp
     * @param session
     * @param rawDynamicObject
     */
    public DynamicObject(
            JsonObject deviceOp,
            CwmpSession session,
            JsonObject rawDynamicObject) {
        super(deviceOp);

        this.session = session;

        keyParameter = rawDynamicObject.getObject(ConfigurationCategory.FIELD_NAME_KEY_PARAMETER);
        keyParameterName = keyParameter.getString("name");

        pathPrefix = rawDynamicObject
                .getString(ConfigurationCategory.FIELD_NAME_TR098_PATH_PREFIX)
                .replace("{i}.", "");

        paramValues = rawDynamicObject.copy();
        paramValues.removeField(ConfigurationCategory.FIELD_NAME_KEY_PARAMETER);
        paramValues.removeField(ConfigurationCategory.FIELD_NAME_TR098_PATH_PREFIX);
        keyParameterValue = VertxJsonUtils.deepGet(paramValues, pathPrefix + "{i}." + keyParameterName);
    }

    /**
     * Start processing this dynamic object by reading the entire table
     */
    public void start() {
        // Start reading
        ParameterNames parameterNames = ParameterNames.Factory.newInstance();
        parameterNames.addString(pathPrefix);
        GetParameterValues.start(
                session,
                parameterNames,
                this,
                CwmpRequest.CWMP_REQUESTER_ACS
        );
    }

    /**
     * Processing of this dynamic object is now completed.
     *
     * Move to the next dynamic object or service or finish the device op.
     */
    public void done() {
        // Done.
        log.info(session.cpeKey + ": Completed processing Dynamic Object " + pathPrefix);

        // Any more dynamic objects lined up?
        if (nextDynamicObject != null) {
            nextDynamicObject.start();
        } else if (deviceOp.containsField(CpeDeviceOp.FIELD_NAME_SERVICES)) {
            // The parent device op contain services
            /**
             * Process WAN Services
             */
            WanService.start(deviceOp, session);
        } else {
            // All done
            DeviceOpUtils.callback(
                    session,
                    deviceOp,
                    CpeDeviceOp.CPE_DEVICE_OP_STATE_SUCCEEDED,
                    null
            );
        }
    }

    /**
     * Abstract Response Handler Class that must be extended by actual requests
     *
     * @param responseMessage
     */
    @Override
    public void responseHandler(CwmpSession session, CwmpRequest request, CwmpMessage responseMessage)
            throws CwmpException {
        Body body = responseMessage.soapEnv.getBody();

        switch (request.methodName) {
            case "GetParameterValues":
                /**
                 * Process "GetParameterValuesResponse"
                 */
                ParameterValueList parameterValueList = body.getGetParameterValuesResponse().getParameterList();
                if (parameterValueList != null) {
                    ParameterValueStruct[] parameterValueStructs = parameterValueList.getParameterValueStructArray();
                    if (parameterValueStructs != null && parameterValueStructs.length > 0) {
                        /**
                         * Convert to JSON Object
                         */
                        JsonObject existingParamValues =
                                GetParameterValuesNbi.parameterValueStructsToJsonObject(parameterValueStructs);
                        JsonObject existingObjects = VertxJsonUtils.deepGet(existingParamValues, pathPrefix);

                        for (String existingObjIndex : existingObjects.getFieldNames()) {
                            JsonObject anExistingObject = existingObjects.getObject(existingObjIndex);
                            String existingKeyParamValue = anExistingObject.getString(keyParameterName);
                            if (existingKeyParamValue != null && existingKeyParamValue.equals(keyParameterValue)) {
                                // Found it
                                String path = pathPrefix + existingObjIndex + ".";
                                log.info(session.cpeKey + ": Found existing entry: " + path);

                                /**
                                 * Does the existing object already match the profile instance?
                                 */
                                JsonObject profileObject = VertxJsonUtils.deepGet(paramValues, pathPrefix + "{i}");
                                if (anExistingObject.equals(profileObject)) {
                                    done();
                                    return;
                                } else {
                                    log.debug("Existing object: " + anExistingObject);
                                    log.debug("Profile  object: " + profileObject);
                                }

                                if (keyParameter.getBoolean(
                                        ConfigurationCategory.FIELD_NAME_KEY_PARAMETER_ALLOW_OVERWRITE,
                                        true)) {
                                    /**
                                     * We can overwrite the existing entry
                                     */
                                    doSetParameterValues(session, path);
                                } else {
                                    /**
                                     * Cannot overwrite.
                                     *
                                     * Must delete first and re-create the object
                                     */
                                    DeleteObject.start(
                                            path,
                                            session,
                                            this
                                    );
                                }
                                return;
                            }
                        }
                    }
                }

                /**
                 * Did not find an existing entry with the same key parameter value.
                 *
                 * Create a new object
                 */
                AddObject.start(
                        pathPrefix,
                        session,
                        this
                );
                break;

            case "AddObject":
                /**
                 * Process "AddObjectResponse"
                 */
                long instanceNumber = body.getAddObjectResponse().getInstanceNumber();
                String objectCreated = pathPrefix + instanceNumber + ".";
                log.info("The Object " + objectCreated + " has been created.");
                doSetParameterValues(session, objectCreated);
                break;

            case "DeleteObject":
                /**
                 * Process "DeleteObjectResponse".
                 *
                 * Now it is safe to create a new object
                 */
                AddObject.start(
                        pathPrefix,
                        session,
                        this
                );
                break;

            case "SetParameterValues":
                /**
                 * Process "SetParameterValuesResponse".
                 */
                SetParameterValues.defaultHandler.responseHandler(session, request, responseMessage);

                /**
                 * Done
                 */
                done();
                break;
        }
    }

    /**
     * Convert all dynamic path with real path and the send the "SetParameterValues" message.
     *
     * @param session
     * @param path      The real actual path (must end with ".")
     */
    public void doSetParameterValues(CwmpSession session, String path) {
        /**
         * Convert dynamic path with real path
         */
        JsonObject childObject = VertxJsonUtils.deepGet(paramValues, pathPrefix + "{i}");
        VertxJsonUtils.deepRemove(paramValues, pathPrefix + "{i}");
        VertxJsonUtils.deepAdd(paramValues, path, childObject);

        SetParameterValuesNbi.start(session,
                SetParameterValuesNbi.jsonObjToParameterValuesList(session.cpe, paramValues, null, ""),
                this,
                CwmpRequest.CWMP_REQUESTER_ACS
        );
    }
}
