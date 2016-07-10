package vertx2.model;

import com.calix.sxa.SxaVertxException;
import com.calix.sxa.VertxJsonUtils;
import vertx2.CcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

import java.text.ParseException;
import java.util.Date;

/**
 * Project:  SXA-CC
 *
 * @author: ronyang
 */
public class MaintenanceWindow {
    private static final Logger log = LoggerFactory.getLogger(MaintenanceWindow.class.getName());

    /**
     * Field Names
     *
     * TODO: Add timezone related attributes.
     */
    public static final String FIELD_NAME_START_DATE_TIME = "startDateTime";
    public static final String FIELD_NAME_END_WINDOW_LENGTH = "windowLength";
    public static final String FIELD_NAME_RECURRING_INTERVAL = "recurringInterval";
    public static final String FIELD_NAME_MAX_RECURRENCE = "maxRecurrence";

    /**
     * Static Exceptions
     */
    public static final CcException INVALID_RECURRING_INTERVAL =
            new CcException("Invalid Recurring! (must be greater than window length)");
    public static final CcException INVALID_START_DATE_TIME =
            new CcException("Invalid Start Date/Time!");
    public static final CcException INVALID_WINDOW_LENGTH =
            new CcException("Invalid Window Length (must be greater than 0)!");

    /**
     * Variables
     */
    public Date startDateTime;
    public int windowLength;
    public int recurringInterval = 0;
    public int maxRecurrence = 0;
    public long startTimeInSeconds;
    public long endTimeInSeconds;

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator mandatoryFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_START_DATE_TIME, VertxJsonUtils.JsonFieldType.DateTime)
                    .append(FIELD_NAME_END_WINDOW_LENGTH, VertxJsonUtils.JsonFieldType.Integer);

    public static final VertxJsonUtils.JsonFieldValidator optionalFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(FIELD_NAME_RECURRING_INTERVAL, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_MAX_RECURRENCE, VertxJsonUtils.JsonFieldType.Integer);


    /**
     * Constructor by JSON Object
     * @param jsonObject
     */
    public MaintenanceWindow(JsonObject jsonObject) throws SxaVertxException {
        // Validate Field Types (all 5 fields are mandatory)
        VertxJsonUtils.validateFields(jsonObject, mandatoryFields, optionalFields);

        // Parse Start Date/Time
        try {
            startDateTime = VertxJsonUtils.iso8601StringToDate(jsonObject.getString(FIELD_NAME_START_DATE_TIME));
        } catch (ParseException e) {
            throw INVALID_START_DATE_TIME;
        }

        // Validate window length
        windowLength = jsonObject.getInteger(FIELD_NAME_END_WINDOW_LENGTH);
        if (windowLength <= 0) {
            throw INVALID_WINDOW_LENGTH;
        }

        recurringInterval = jsonObject.getInteger(FIELD_NAME_RECURRING_INTERVAL, 0);
        maxRecurrence = jsonObject.getInteger(FIELD_NAME_MAX_RECURRENCE, 0);

        // Validate recurringInterval
        if (recurringInterval > 0 && recurringInterval <= windowLength) {
            throw INVALID_RECURRING_INTERVAL;
        }

        // Initialize start/end times in # of seconds
        startTimeInSeconds = startDateTime.getTime() / 1000;
        endTimeInSeconds = startTimeInSeconds + windowLength + recurringInterval * maxRecurrence;

        if (timeTillNextOpen() < 0) {
            // window ends in the past
            throw INVALID_START_DATE_TIME;
        }
    }

    /**
     * Check if we are inside the window right now.
     */
    public boolean isOpen() {
        long currTimeInSeconds = System.currentTimeMillis() / 1000;
        if (currTimeInSeconds < startTimeInSeconds || currTimeInSeconds > endTimeInSeconds) {
            return false;
        }

        long offset = currTimeInSeconds - startTimeInSeconds;
        if (recurringInterval > 0) {
            offset = offset % recurringInterval;
        }
        if (offset > windowLength) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Time (# of seconds) till next open
     */
    public long timeTillNextOpen() {
        if (isOpen()) {
            // already open
            return 0;
        }

        long currTimeInSeconds = System.currentTimeMillis() / 1000;

        // Has the window ever opened for at least once?
        if (currTimeInSeconds < startTimeInSeconds) {
            // not yet
            return startTimeInSeconds - currTimeInSeconds;
        } else if (currTimeInSeconds < endTimeInSeconds) {
            // the window has opened before
            return windowLength - (currTimeInSeconds - startTimeInSeconds) % windowLength;
        } else {
            log.info("currTime: " + currTimeInSeconds
                    + ", window start/end: " + startTimeInSeconds + "/" + endTimeInSeconds);
            return -1;
        }
    }
}
