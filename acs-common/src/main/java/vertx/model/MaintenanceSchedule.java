package vertx.model;

import vertx.VertxException;
import vertx.VertxJsonUtils;
import vertx.util.AcsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Project:  cwmp
 *
 * @author: ronyang
 */
public class MaintenanceSchedule extends MaintenanceWindow{
    private static final Logger log = LoggerFactory.getLogger(MaintenanceSchedule.class.getName());

    /**
     * DB Collection Name
     */
    public static final String DB_COLLECTION_NAME = "CWMP-maintenance-schedules";

    /**
     * Index Field(s)
     */
    public static final String[] INDEX_FIELDS = {
            AcsConstants.FIELD_NAME_ORG_ID,
            AcsConstants.FIELD_NAME_NAME
    };

    /**
     * Editable Fields
     */
    public static final List<String> EDITABLE_FIELDS = new ArrayList<String>() {{
        add(AcsConstants.FIELD_NAME_NAME);
        add(AcsConstants.FIELD_NAME_DESCRIPTION);
        add(FIELD_NAME_START_DATE_TIME);
        add(FIELD_NAME_END_WINDOW_LENGTH);
        add(FIELD_NAME_RECURRING_INTERVAL);
        add(FIELD_NAME_MAX_RECURRENCE);
    }};

    /**
     * Define static JSON Field Validators
     */
    public static final VertxJsonUtils.JsonFieldValidator mandatoryFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ORG_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsConstants.FIELD_NAME_NAME, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_START_DATE_TIME, VertxJsonUtils.JsonFieldType.DateTime)
                    .append(FIELD_NAME_END_WINDOW_LENGTH, VertxJsonUtils.JsonFieldType.Integer);

    public static final VertxJsonUtils.JsonFieldValidator optionalFields =
            new VertxJsonUtils.JsonFieldValidator()
                    .append(AcsConstants.FIELD_NAME_ID, VertxJsonUtils.JsonFieldType.String)
                    .append(AcsConstants.FIELD_NAME_DESCRIPTION, VertxJsonUtils.JsonFieldType.String)
                    .append(FIELD_NAME_RECURRING_INTERVAL, VertxJsonUtils.JsonFieldType.Integer)
                    .append(FIELD_NAME_MAX_RECURRENCE, VertxJsonUtils.JsonFieldType.Integer);


    /**
     * Constructor by JSON Object
     * @param jsonObject
     */
    public MaintenanceSchedule(JsonObject jsonObject) throws VertxException {
        super(jsonObject);

        // Validate Field Types (all 5 fields are mandatory)
        VertxJsonUtils.validateFields(jsonObject, mandatoryFields, optionalFields);
    }
}
