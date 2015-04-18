package com.calix.sxa.cc.acs.nbi.event;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.cc.acs.nbi.AbstractAcNbiCrudService;
import com.calix.sxa.cc.acs.nbi.model.AcsNbiRequest;
import com.calix.sxa.cc.model.AcsApiCrudTypeEnum;
import com.calix.sxa.cc.model.Event;
import com.calix.sxa.cc.util.AcsConstants;
import org.vertx.java.core.json.JsonObject;

import java.util.List;

/**
 * Project:  SXA-CC ACS API
 *
 * Event Web Service Implementation.
 *
 * @author: jqin
 */
public class EventService extends AbstractAcNbiCrudService {
    /**
     * Static Exceptions
     */
    public static final SxaVertxException CANNOT_CREATE = new SxaVertxException("Cannot create new events!");
    public static final SxaVertxException CANNOT_UPDATE = new SxaVertxException("Cannot modify events!");

    /**
     * Get the name of the service which is to be used to build URL Path Prefix.
     *
     * For example service name "device-op" maps to URL path "/cc/device-op".
     */
    @Override
    public String getServiceName() {
        return AcsConstants.ACS_API_SERVICE_EVENT;
    }

    /**
     * Get the MongoDB Collection Name, for example "acs-event-subscriptions" or "acs-device-ops"
     *
     * @return
     */
    @Override
    public String getDbCollectionName() {
        return Event.DB_COLLECTION_NAME;
    }

    /**
     * Get the names of the fields that are used as the index fields that identifies each record uniquely.
     *
     * Return null to always allow creations.
     */
    @Override
    public String[] getIndexFieldName() {
        return null;
    }

    /**
     * Get the names of fields that are editable (i.e. subject to update).
     */
    @Override
    public List<String> getEditableFields() {
        return null;
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
     */
    public boolean validate(AcsNbiRequest nbiRequest, AcsApiCrudTypeEnum crudType) throws SxaVertxException {
        switch (crudType) {
            case Create:
                throw  CANNOT_CREATE;

            case Update:
                throw CANNOT_UPDATE;
        }

        return true;
    }

    /**
     * For bulk query, get the "sort" JSON Object on how to sort the results.
     *
     * @param nbiRequest
     * @return  Default to null (let MongoDB to sort it)
     */
    private static final JsonObject SORT_BY_TIME =
            // Descending Order for Timestamp, so newest first
            new JsonObject().putNumber(Event.FIELD_NAME_TIMESTAMP, -1);
    @Override
    public JsonObject getDefaultQuerySort(AcsNbiRequest nbiRequest) {
        return SORT_BY_TIME;
    }

    /**
     * Get the list of field names that contain MongoDB "$date" timestamp.
     *
     * @return  True if the actual service already took care of the response,
     *          or false to continue with the default response code.
     */
    private static final String[] TIMESTAMP_FIELDS = {
            Event.FIELD_NAME_TIMESTAMP
    };
    @Override
    public String[] getDateTimeFieldName() {
        return TIMESTAMP_FIELDS;
    }
}
