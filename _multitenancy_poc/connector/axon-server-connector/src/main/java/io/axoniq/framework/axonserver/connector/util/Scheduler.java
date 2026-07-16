/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.axonserver.connector.util;

import java.util.concurrent.TimeUnit;

/**
 * Schedules tasks to be executed in future.
 *
 * @author Sara Pellegrini
 * @since 4.2.1
 */
public interface Scheduler {

    /**
     * Schedules a {@code command} to be executed periodically after {@code initialDelay}. Frequency of execution is
     * represented by {@code delay}. New task is scheduled after execution of current one.
     *
     * @param command      the task to be executed
     * @param initialDelay for how long the first execution is delayed
     * @param delay        frequency of task execution
     * @param timeUnit     the time unit
     * @return information about the schedule, and means to cancel it
     */
    ScheduledTask scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit timeUnit);

    /**
     * Cancels all scheduled tasks.
     */
    void shutdownNow();

    interface ScheduledTask {

        /**
         * Cancels the scheduled registration.
         *
         * @param mayInterruptIfRunning {@code true} if the thread executing this task should be interrupted; otherwise,
         *                              in-progress tasks are allowed to complete
         */
        void cancel(boolean mayInterruptIfRunning);
    }
}
