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
package io.axoniq.workflow.runtime.test.utils;

import io.axoniq.workflow.runtime.execution.WorkflowScheduler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Manual timeout scheduler driven by the fixture clock.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class ManualWorkflowScheduler implements WorkflowScheduler {

    private final ConcurrentSkipListMap<Instant, List<ManualScheduledTask>> tasks = new ConcurrentSkipListMap<>();

    /**
     * Schedules a deadline notification when fixture time reaches the given deadline.
     *
     * @param deadline deadline according to fixture-controlled time
     * @return scheduled task handle
     */
    @Override
    public ScheduledTask schedule(Instant deadline) {
        var scheduledTask = new ManualScheduledTask();
        tasks.compute(deadline, (ignored, existing) -> {
            var updated = existing == null ? new ArrayList<ManualScheduledTask>() : new ArrayList<>(existing);
            updated.add(scheduledTask);
            return updated;
        });
        return scheduledTask;
    }

    /**
     * Run all tasks due up to the given instant.
     *
     * @param now current fixture time
     */
    public void runDueTasks(Instant now) {
        while (!tasks.isEmpty() && !tasks.firstKey().isAfter(now)) {
            var due = tasks.pollFirstEntry();
            if (due != null) {
                due.getValue().forEach(ManualScheduledTask::run);
            }
        }
    }

    private static class ManualScheduledTask implements ScheduledTask {

        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private ManualScheduledTask() {
        }

        /**
         * Returns a future completed when the deadline is reached or is cancelled.
         *
         * @return completion future
         */
        @Override
        public CompletableFuture<Void> completion() {
            return completion;
        }

        /**
         * Cancels the scheduled task if it has not run yet.
         */
        @Override
        public void cancel() {
            completion.cancel(false);
        }

        private void run() {
            if (completion.isDone()) {
                return;
            }
            completion.complete(null);
        }
    }
}
