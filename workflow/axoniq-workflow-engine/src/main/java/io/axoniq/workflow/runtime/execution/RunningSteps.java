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

import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Tracks running {@link CompletableFuture}s for workflow steps, keyed by step name.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public class RunningSteps implements DescribableComponent {

    private final ConcurrentHashMap<String, CompletableFuture<?>> runningFutures = new ConcurrentHashMap<>();

    /**
     * Register a new running step.
     *
     * @param stepName step name.
     * @param future   future to register.
     */
    public void register(String stepName, CompletableFuture<?> future) {
        runningFutures.put(stepName, future);
    }

    /**
     * Remove a running step.
     *
     * @param stepName step name.
     */
    public void remove(String stepName) {
        runningFutures.remove(stepName);
    }

    /**
     * Returns a snapshot of the names of the currently-running steps.
     *
     * @return a snapshot set of the running step names.
     */
    public Set<String> stepNames() {
        return new HashSet<>(runningFutures.keySet());
    }

    /**
     * Cancel and remove a running step.
     *
     * @param stepName              step name.
     * @param mayInterruptIfRunning whether to interrupt the step if it is running.
     */
    public void cancelAndRemove(String stepName, boolean mayInterruptIfRunning) {
        var future = runningFutures.remove(stepName);
        if (future != null) {
            future.cancel(mayInterruptIfRunning);
        }
    }

    /**
     * Cancel a running step providing a cause to be used for the cancellation as a failed future cause.
     *
     * @param stepName step name.
     * @param cause    a cause for the cancellation.
     * @return true, if the future was cancelled successfully.
     */
    public boolean cancelWithCause(String stepName, @Nullable Throwable cause) {
        var future = runningFutures.remove(stepName);
        if (future == null) {
            return false;
        }
        var ex = cause != null ? cause : new StepCancellationException("Step cancelled");
        return future.completeExceptionally(ex);
    }

    /**
     * Cancel all running steps, waiting for them to complete and passing the cancelled step names to the given
     * consumer.
     *
     * @param cause            the cause of the cancellation.
     * @param awaitTermination consumer of cancelled step names.
     */
    public void cancelAll(@Nullable Throwable cause, Consumer<Set<String>> awaitTermination) {
        var stepNames = new HashSet<>(runningFutures.keySet());
        var ex = cause != null ? cause : new StepCancellationException("Workflow terminated");
        runningFutures.values().forEach(f -> f.completeExceptionally(ex));
        awaitTermination.accept(stepNames);
        runningFutures.clear();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("runningSteps", runningFutures.keySet().stream().toList());
    }
}
