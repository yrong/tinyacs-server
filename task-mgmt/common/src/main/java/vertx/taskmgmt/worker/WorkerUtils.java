package vertx.taskmgmt.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

/**
 * Project:  SXA Task Management
 *
 * @author: ronang
 */
public class WorkerUtils {
    private static Logger log = LoggerFactory.getLogger(WorkerUtils.class);

    public static final String FIELD_NAME_CLASS_NAMES = "classNames";
    public static final String FIELD_NAME_MAX_OUTSTANDING_TASKS = "maxOutstandingTasks";


    /**
     * Get a list of Task Types (AbstractSxaTaskImpl POJOs) from deployment configuration
     *
     * @return  The list, or null.
     */
    public static List<? extends AbstractSxaTaskImpl> getTaskTypes(JsonObject config) {
        JsonArray classNames = config.getJsonArray(FIELD_NAME_CLASS_NAMES);
        if (classNames == null || classNames.size() <= 0) {
            log.error(FIELD_NAME_CLASS_NAMES + " is missing!");
            return null;
        }
        AbstractSxaTaskImpl[] array = new AbstractSxaTaskImpl[classNames.size()];

        /**
         * Traverse the class name array and create (new) POJOs
         */
        String className = null;
        try {
            for (int i = 0; i < classNames.size(); i ++) {
                className = classNames.getString(i);
                Class<?> clazz = Class.forName(className);
                Constructor<?> constructor = clazz.getConstructor();
                array[i] = (AbstractSxaTaskImpl) constructor.newInstance();
            }
        } catch (Exception e) {
            log.error("\n\nInvalid AbstractSxaTaskImpl Class " + className +" Found!\n\n");
            log.error("Full array:\n" + classNames.encodePrettily());
            return null;
        }

        return Arrays.asList(array);
    }

    /**
     * Get the max # of outstanding tasks from deployment configuration.
     *
     * @param config
     * @return
     */
    public static int getMaxOutstandingTasks(JsonObject config) {
        return config.getInteger(FIELD_NAME_MAX_OUTSTANDING_TASKS, 1);
    }

    /**
     * Build Worker/Poller Vertice Config.
     *
     * @param classNames
     * @param maxOutstandingTasks
     * @return
     */
    public static JsonObject buildConfig(String[] classNames, int maxOutstandingTasks) {
        return new JsonObject().put(FIELD_NAME_MAX_OUTSTANDING_TASKS, maxOutstandingTasks)
                .put(FIELD_NAME_CLASS_NAMES, new JsonArray(Arrays.asList(classNames)));
    }

}
