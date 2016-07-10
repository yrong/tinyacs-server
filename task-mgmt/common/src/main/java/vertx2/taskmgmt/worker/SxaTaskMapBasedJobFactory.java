package vertx2.taskmgmt.worker;

import net.greghaines.jesque.Job;
import net.greghaines.jesque.worker.MapBasedJobFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.json.JsonObject;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Project:  sxa-cc-parent
 *
 * @author: jqin
 */
public class SxaTaskMapBasedJobFactory extends MapBasedJobFactory {
    private static final Logger log = LoggerFactory.getLogger(SxaTaskMapBasedJobFactory.class.getName());

    /**
     * Worker Vertice Instance
     */
    WorkerVertice workerVertice;

    /**
     * Constructor.
     *
     * @param jobTypes the map of job names and types to execute
     */
    public SxaTaskMapBasedJobFactory(Map<String, ? extends Class<?>> jobTypes, WorkerVertice workerVertice) {
        super(jobTypes);
        this.workerVertice = workerVertice;
    }

    /**
     * Determine if a job name and job type are valid.
     *
     * @param jobName
     *            the name of the job
     * @param jobType
     *            the class of the job
     * @throws IllegalArgumentException
     *             if the name or type are invalid
     */
    @Override
    protected void checkJobType(final String jobName, final Class<?> jobType) {
        if (jobName == null) {
            throw new IllegalArgumentException("jobName must not be null");
        }
        if (jobType == null) {
            throw new IllegalArgumentException("jobType must not be null");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object materializeJob(final Job job) throws Exception {
        //return JesqueUtils.materializeJob(job, this.getJobTypes());
        log.info("Job Class Name: " + job.getClassName() + ", Job's 1st arg: " + job.getArgs()[0].toString() +
                ", mapping to " + getJobTypes().get(job.getClassName()));

        /**
         * The first arg is an JSON Object, construct the CC Task with it.
         */
        Constructor<?> constructor =
                getJobTypes().get(job.getClassName())
                        .getConstructor(JsonObject.class, WorkerVertice.class);
        if (constructor != null) {
            if (job.getArgs()[0] instanceof String) {
                JsonObject jsonObject = new JsonObject((String)job.getArgs()[0]);
                return constructor.newInstance(jsonObject, workerVertice);
            } else {
                return constructor.newInstance(job.getArgs()[0], workerVertice);
            }
        } else {
            throw new Exception("Invalid class " + job.getClassName());
        }
    }
}
