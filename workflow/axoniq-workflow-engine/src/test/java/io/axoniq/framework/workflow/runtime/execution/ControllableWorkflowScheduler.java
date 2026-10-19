/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
