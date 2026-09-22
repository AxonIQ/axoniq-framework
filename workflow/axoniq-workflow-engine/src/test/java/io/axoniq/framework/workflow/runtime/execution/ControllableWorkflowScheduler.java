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
package io.axoniq.framework.workflow.runtime.execution;


import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Deterministic test scheduler for timeout and retry race tests.
 *
 * @author Simon Zambrovski
 */
final class ControllableWorkflowScheduler implements WorkflowScheduler {

    private final List<ControlledTask> tasks = new ArrayList<>();
    private boolean fireDuringSchedule;

    void fireDuringSchedule() {
        fireDuringSchedule = true;
    }

    void fireNext() {
        tasks.stream().filter(task -> !task.completion.isDone()).findFirst().orElseThrow().fire();
    }

    int pendingTaskCount() {
        return (int) tasks.stream().filter(task -> !task.completion.isDone()).count();
    }

    @Override
    public ScheduledTask schedule(Instant deadline) {
        var controlledTask = new ControlledTask(deadline);
        tasks.add(controlledTask);
        if (fireDuringSchedule) {
            controlledTask.fire();
        }
        return controlledTask;
    }

    private static final class ControlledTask implements ScheduledTask {

        private final Instant deadline;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private ControlledTask(Instant deadline) {
            this.deadline = deadline;
        }

        private void fire() {
            if (!completion.isDone()) {
                completion.complete(null);
            }
        }

        @Override
        public CompletableFuture<Void> completion() {
            return completion;
        }

        @Override
        public void cancel() {
            completion.cancel(false);
        }
    }
}
