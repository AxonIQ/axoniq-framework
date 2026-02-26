/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.impl;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;

import io.axoniq.workflow.runtime.api.StepCancellationException;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Tracks running {@link CompletableFuture}s for workflow steps, keyed by step name.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
public class RunningSteps implements DescribableComponent {

    private final ConcurrentHashMap<String, CompletableFuture<?>> runningFutures = new ConcurrentHashMap<>();

    public void register(@Nonnull String stepName, @Nonnull CompletableFuture<?> future) {
        runningFutures.put(stepName, future);
    }

    public void remove(@Nonnull String stepName) {
        runningFutures.remove(stepName);
    }

    public void cancelAndRemove(@Nonnull String stepName, boolean mayInterruptIfRunning) {
        var future = runningFutures.remove(stepName);
        if (future != null) {
            future.cancel(mayInterruptIfRunning);
        }
    }

    public boolean cancelWithCause(@Nonnull String stepName, @Nullable Throwable cause) {
        var future = runningFutures.remove(stepName);
        if (future == null) {
            return false;
        }
        var ex = cause != null ? cause : new StepCancellationException("Step cancelled");
        return future.completeExceptionally(ex);
    }

    public void cancelAll(@Nullable Throwable cause, @Nonnull Consumer<Set<String>> awaitTermination) {
        var stepNames = new HashSet<>(runningFutures.keySet());
        var ex = cause != null ? cause : new StepCancellationException("Workflow terminated");
        runningFutures.values().forEach(f -> f.completeExceptionally(ex));
        awaitTermination.accept(stepNames);
        runningFutures.clear();
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("runningSteps", runningFutures.keySet().stream().toList());
    }
}
