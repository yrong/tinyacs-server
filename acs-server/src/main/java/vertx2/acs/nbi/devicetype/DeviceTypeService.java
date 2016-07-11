package vertx2.acs.nbi.devicetype;

import vertx2.VertxException;
import vertx2.VertxMongoUtils;
import vertx2.acs.nbi.AbstractAcNbiCrudService;
import vertx2.acs.nbi.model.AcsNbiRequest;
import vertx2.model.AcsApiCrudTypeEnum;
import vertx2.model.Cpe;
import vertx2.model.CpeDeviceType;
import vertx2.util.AcsConstants;
import vertx2.util.AcsMiscUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.List;

/**
 * Project:  cwmp ACS API
 *
 * Device Type Web Service Implementation.
 *
 * @author: ronyang
 */
public class DeviceTypeService extends AbstractAcNbiCrudService {
    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_DEVICE_TYPE;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return CpeDeviceType.DB_COLLECTION_NAME;
    }

    /**
     * All Device Type Attributes are index fields.
     */
    @Override
    public String[] getIndexFieldName() {
        return CpeDeviceType.INDEX_FIELDS;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return CpeDeviceType.EDITABLE_FIELDS;
    }

    /**
     * Validate an NBI Request.
     *
     * Must be implemented by actual services.
     *
     * If validation is completed, returns true.
     * If validation is not completed (for example pending a further DB query callback), returns false.
     *
     * @param nbiRequest
     * @param crudType      Type of the CRUD operation.
     *
     * @return boolean
     * @throws VertxException
     */
    public boolean validate(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws VertxException {
        switch (crudType) {
            case Create:
            case Update:
            case Delete:
                throw new VertxException("Device Types are read only!");
        }

        return true;
    }

    /**
     * Override Default Post Retrieve Handler.
     *
     * @param nbiRequest
     * @param queryResults
     * @param moreExist         More Batch(es) Available.
     *
     * @return  The processed query results as a JSON Array, or null if there is more work to be done.
     */
    @Override
    public JsonArray postRetrieve(
            final AcsNbiRequest nbiRequest,
            final JsonArray queryResults,
            final boolean moreExist) {
        Integer totalCount = new Integer(queryResults.size());
        nbiRequest.serviceData = totalCount;

        if (queryResults.size() > 0) {
            /**
             * Create another JSON Array for the final results
             */
            final JsonArray results = new JsonArray();

            /**
             * Retrieve the CPE Counts for each device type(s)
             */
            for (int i = 0; i < queryResults.size(); i++) {
                final JsonObject aDeviceType = queryResults.get(i);

                /**
                 * Check for illegal device types
                 */
                String oui = aDeviceType.getString(CpeDeviceType.FIELD_NAME_OUI);
                if (oui != null && !AcsMiscUtils.isOuiString(oui)) {
                    log.error("Found an illegal device type " + aDeviceType);
                    totalCount --;

                    try {
                        VertxMongoUtils.delete(
                                vertx.eventBus(),
                                CpeDeviceType.DB_COLLECTION_NAME,
                                aDeviceType.getString(AcsConstants.FIELD_NAME_ID),
                                null
                        );
                    } catch (Exception ex) {

                    }
                } else {
                    // Build CPE Matcher by Device Type
                    JsonObject cpeMatcher = new JsonObject();
                    for (String fieldName : aDeviceType.getFieldNames()) {
                        if (fieldName.equals(AcsConstants.FIELD_NAME_ID)) {
                            continue;
                        }

                        String value = aDeviceType.getString(fieldName);
                        if (value != null) {
                            cpeMatcher.putString(fieldName, value);
                        }
                    }

                    //log.debug("Built CPE Matcher:\n" + cpeMatcher.encodePrettily());
                    try {
                        VertxMongoUtils.count(
                                vertx.eventBus(),
                                Cpe.CPE_COLLECTION_NAME,
                                cpeMatcher,
                                new Handler<Long>() {
                                    @Override
                                    public void handle(Long count) {
                                        if (count != null) {

                                            // Update Pending Count for this batch
                                            Integer pendingCount = (Integer)nbiRequest.serviceData;
                                            pendingCount--;
                                            nbiRequest.serviceData = pendingCount;

                                            if (count > 0) {
                                                aDeviceType.putNumber(CpeDeviceType.FIELD_NAME_CPE_COUNT, count);
                                                results.add(aDeviceType);
                                            } else {
                                                log.error("Found a device type with no matching device: " + aDeviceType);

                                                try {
                                                    VertxMongoUtils.delete(
                                                            vertx.eventBus(),
                                                            CpeDeviceType.DB_COLLECTION_NAME,
                                                            aDeviceType.getString(AcsConstants.FIELD_NAME_ID),
                                                            null
                                                    );
                                                } catch (Exception ex) {

                                                }
                                            }

                                            if (pendingCount == 0) {
                                                // Send Response Chunk if all queries are done (for this batch)
                                                nbiRequest.sendResponseChunk(
                                                        HttpResponseStatus.OK,
                                                        results,
                                                        moreExist
                                                );
                                            }
                                        }
                                    }
                                }
                        );
                    } catch (VertxException e) {
                        e.printStackTrace();
                    }
                }
            }

            // Return true to skip the default response handling
            return null;
        } else {
            // continue with the default response code.
            return AbstractAcNbiCrudService.EMPTY_JSON_ARRAY;
        }
    }
}
