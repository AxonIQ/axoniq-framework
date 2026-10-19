/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;


import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Delivers workflow timeout and retry-backoff deadlines.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface WorkflowScheduler {

    /**
     * Schedules a deadline notification.
     *
     * @param deadline deadline according to the configured workflow clock
     * @return handle of the scheduled task
     */
    ScheduledTask schedule(Instant deadline);

    /**
     * Scheduled task handle.
     */
    interface ScheduledTask {

        /**
         * Future completed when the deadline is reached, or cancelled when the deadline is cancelled.
         *
         * @return completion future
         */
        CompletableFuture<Void> completion();

        /**
         * Cancel the task if it has not run yet.
         */
        void cancel();
    }
}
