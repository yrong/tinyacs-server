/*
 * Copyright 2012 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.calix.sxa.taskmgmt;

import com.calix.sxa.VertxUtils;

/**
 * Enums for Task related event types
 */
public class TaskConstants {
    // Address for new jobs polled from Jesque
    // When using this address, the sender must append the queue name at the end
    public static final String VERTX_ADDRESS_NEW_TASKS = VertxUtils.getHostnameAndPid() + ".new_tasks";

    // Address for job process results to back to Jesque poller
    public static final String VERTX_ADDRESS_TASK_RESULTS = VertxUtils.getHostnameAndPid() + ".worker_results";

    // Address for sub task updates
    public static final String VERTX_ADDRESS_SUB_TASK_UPDATES = "sub.task.updates";

    /**
     * Task States (as strings)
     */
    public final static String TASK_STATE_PENDING = "pending";
    public final static String TASK_STATE_CANCELLED = "cancelled";
    public final static String TASK_STATE_IN_PROGRESS = "in-progress";
    public static final String TASK_STATE_SUCCEEDED = "succeeded";
    public static final String TASK_STATE_FAILED = "failed";

    /**
     * Resque Namespaces (aka Redis keys)
     */
    public final static String RESQUE_NAME_SPACE_ROOT = "resque";
    public final static String RESQUE_NAME_SPACE_QUEUES = RESQUE_NAME_SPACE_ROOT + ":queues";
    public final static String RESQUE_NAME_SPACE_QUEUE = RESQUE_NAME_SPACE_ROOT + ":queue:";

    /**
     * Name of the MongoDB Collection that stores all the in-progress and completed tasks
     */
    public final static String MONGODB_TASK_COLLECTION = "tasks";

    /**
     * Following Constants are for Redis Key Space Name Prefixes
     */

    // Task Root
    public final static String TASK_ROOT = "cc_tasks";

    /**
     * Internal ACS Server API Task Roots
     */
    public final static String INTERNAL_ACS_API_TASK_NS_ROOT = TASK_ROOT + "_acs";

    /**
     * Internal ACS Server API Task Roots by Types
     */
    // Profile Management
    public final static String INTERNAL_ACS_API_TASK_NS_WORKFLOW = INTERNAL_ACS_API_TASK_NS_ROOT + "_workflow";

    /**
     * Internal ACS-CPE Server API Task Root
     */
    public final static String INTERNAL_ACS_CPE_API_TASK_NS_ROOT = TASK_ROOT + "_acs-cpe";

    /**
     * Internal ACS-CPE Server API Task Roots by Types
     */
    // Device Operations
    public final static String INTERNAL_ACS_CPE_API_TASK_NS_DO = INTERNAL_ACS_CPE_API_TASK_NS_ROOT + "_do";
    // Connection Requests
    public final static String INTERNAL_ACS_CPE_API_TASK_NS_CONN_REQ = INTERNAL_ACS_CPE_API_TASK_NS_ROOT + "_conn_req";

    /**
     * Second/Minute/Hour/Day in the number of milli seconds
     */
    public final static long ONE_SECOND_IN_MS = 1000;
    public final static long ONE_MINUTE_IN_MS = ONE_SECOND_IN_MS * 60;
    public final static long ONE_HOUR_IN_MS = ONE_MINUTE_IN_MS *60;
    public final static long ONE_DAY_IN_MS = ONE_HOUR_IN_MS * 24;
}
