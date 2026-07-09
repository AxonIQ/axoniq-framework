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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

final class CheckpointSupport {

    interface Host {

        boolean isExecutable();

        boolean hasQueuedTasks();

        boolean isTaskActive();

        void appendTask(@Nonnull Consumer<WorkflowExecution> task);
    }

    private final Host host;
    private final Object checkpointIntentMonitor = new Object();
    private boolean checkpointIntentQueued;
    private final List<Runnable> checkpointIntentCallbacks = new ArrayList<>();

    CheckpointSupport(@Nonnull Host host) {
        this.host = Objects.requireNonNull(host, "Checkpoint support host must not be null");
    }

    void appendCheckpointIntent(@Nonnull Runnable onDrained) {
        var callback = Objects.requireNonNull(onDrained, "On drained callback must not be null");
        if (!host.isExecutable()) {
            callback.run();
            return;
        }

        var shouldAppendIntent = false;
        synchronized (checkpointIntentMonitor) {
            checkpointIntentCallbacks.add(callback);
            if (checkpointIntentQueued) {
                return;
            }
            checkpointIntentQueued = true;
            shouldAppendIntent = true;
        }

        if (shouldAppendIntent) {
            host.appendTask(new CheckpointIntent());
        }
    }

    boolean hasPendingCheckpointWork() {
        if (!host.isExecutable()) {
            return false;
        }
        synchronized (checkpointIntentMonitor) {
            return host.isTaskActive() || host.hasQueuedTasks() || checkpointIntentQueued;
        }
    }

    @Nullable
    Runnable afterTask(@Nonnull Consumer<WorkflowExecution> task) {
        return task instanceof CheckpointIntent checkpointIntent ? checkpointIntent.onDrained() : null;
    }

    private final class CheckpointIntent implements Consumer<WorkflowExecution> {

        private Runnable onDrained = () -> {
        };

        @Override
        public void accept(WorkflowExecution ignored) {
            var callbacks = new ArrayList<Runnable>();
            synchronized (checkpointIntentMonitor) {
                checkpointIntentQueued = false;
                callbacks.addAll(checkpointIntentCallbacks);
                checkpointIntentCallbacks.clear();
            }
            onDrained = () -> callbacks.forEach(Runnable::run);
        }

        private Runnable onDrained() {
            return onDrained;
        }
    }
}
