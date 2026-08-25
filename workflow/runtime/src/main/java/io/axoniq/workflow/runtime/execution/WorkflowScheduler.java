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
package io.axoniq.workflow.runtime.execution;

import jakarta.annotation.Nonnull;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Delivers workflow timeout and retry-backoff deadlines.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface WorkflowScheduler {

    /**
     * Schedules a deadline notification.
     *
     * @param deadline deadline according to the configured workflow clock
     * @return handle of the scheduled task
     */
    @Nonnull
    ScheduledTask schedule(@Nonnull Instant deadline);

    /**
     * Scheduled task handle.
     */
    interface ScheduledTask {

        /**
         * Future completed when the deadline is reached, or cancelled when the deadline is cancelled.
         *
         * @return callback future
         */
        @Nonnull
        CompletableFuture<Void> completion();

        /**
         * Cancel the task if it has not run yet.
         */
        void cancel();
    }
}
