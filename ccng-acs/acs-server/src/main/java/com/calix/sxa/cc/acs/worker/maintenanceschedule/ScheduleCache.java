package com.calix.sxa.cc.acs.worker.maintenanceschedule;

import com.calix.sxa.cc.cache.AbstractLocalCache;
import com.calix.sxa.cc.model.MaintenanceSchedule;
import com.calix.sxa.cc.util.AcsConstants;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonObject;

/**
 * Project:  SXA-CC
 *
 * @author: jqin
 */
public class ScheduleCache extends AbstractLocalCache {
    /**
     * Constructor.
     *
     * @param vertx
     * @param crudEventAddress
     * @param dbCollectionName
     * @param cachedObjectType
     */
    public ScheduleCache(Vertx vertx, String crudEventAddress, String dbCollectionName, String cachedObjectType) {
        super(vertx, crudEventAddress, dbCollectionName, cachedObjectType);
    }

    /**
     * Build actual index String by the raw JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public String getIndexString(JsonObject jsonObject) {
        return jsonObject.getString(AcsConstants.FIELD_NAME_ID);
    }

    /**
     * Get a POJO by JSON Object.
     *
     * @param jsonObject
     */
    @Override
    public Object getPojoByJsonObject(JsonObject jsonObject) throws Exception{
        return new MaintenanceSchedule(jsonObject);
    }
}
