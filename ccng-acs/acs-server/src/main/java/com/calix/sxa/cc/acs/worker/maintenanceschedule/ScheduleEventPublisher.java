package com.calix.sxa.cc.acs.worker.maintenanceschedule;

import com.calix.sxa.cc.model.MaintenanceSchedule;
import com.calix.sxa.cc.util.AcsConstants;
import org.vertx.java.platform.Verticle;

/**
 * Project:  SXA-CC
 *
 * This class is responsible for publishing "maintenance window open"/"maintenance window close " events for each
 * existing maintenance schedules.
 *
 * @author: jqin
 */
public class ScheduleEventPublisher extends Verticle {
    /**
     * A local cache of all the Maintenance Schedules.
     */
    ScheduleCache scheduleCache = new ScheduleCache(
            vertx,
            AcsConstants.VERTX_ADDRESS_MAINTENANCE_SCHEDULE,
            MaintenanceSchedule.DB_COLLECTION_NAME,
            MaintenanceSchedule.class.getSimpleName()
    );

    @Override
    public void start() {
        /**
         * Initialize local cache of all schedules
         */
        scheduleCache = new ScheduleCache(
                vertx,
                AcsConstants.VERTX_ADDRESS_MAINTENANCE_SCHEDULE,
                MaintenanceSchedule.DB_COLLECTION_NAME,
                MaintenanceSchedule.class.getSimpleName()
        );
    }
}
