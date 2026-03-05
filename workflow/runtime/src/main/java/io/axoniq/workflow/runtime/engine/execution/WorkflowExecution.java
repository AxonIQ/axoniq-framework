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
package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Represents the part of the execution accessed by the Workflow Engine (internal).
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Allard Buijze
 * @since 1.0.0
 */
@Internal
public interface WorkflowExecution {

    /**
     * Execute workflow.
     *
     * @param <T> type of the workflow context.
     * @return workflow context.
     */
    @Nonnull
    <T extends WorkflowContext> T execute();

    /**
     * Returns workflow context of the current execution.
     *
     * @param <T> type of the context.
     * @return context.
     */
    <T extends WorkflowContext> T workflowContext();

    void awaitStateChange(@Nonnull Predicate<WorkflowExecution> condition) throws InterruptedException;

    void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext);

    void appendTask(@Nonnull Consumer<WorkflowExecution> task);

    @Nullable
    Consumer<WorkflowExecution> getNextTask();

    boolean isExecutable();

    boolean hasTasks();

    /**
     * Registers a new wait condition.
     *
     * @param stepName            waiting step name.
     * @param eventCondition      event condition.
     * @param eventNameCustomizer event name customizer.
     */
    void registerWaitCondition(@Nonnull String stepName,
                               @Nonnull EventCondition eventCondition,
                               @Nonnull EventNameCustomizer eventNameCustomizer);

    /**
     * Remove existing wait condition.
     *
     * @param stepName name of the waiting step.
     */
    void removeWaitCondition(@Nonnull String stepName);

    void registerRunningStep(@Nonnull String stepName, @Nonnull CompletableFuture<?> future);

    void removeRunningStep(@Nonnull String stepName);

    boolean cancelRunningStep(@Nonnull String stepName, @Nullable Throwable cause);

    void cancelAllRunningSteps(@Nullable Throwable cause);

    void cancelAndRemoveRunningStep(@Nonnull String stepName, boolean mayInterruptIfRunning);

    @Nonnull
    WorkflowState state();

    // FIXME check if we can replace this for the Context interface
    @Nonnull
    ProcessingContext processingContext();
}
