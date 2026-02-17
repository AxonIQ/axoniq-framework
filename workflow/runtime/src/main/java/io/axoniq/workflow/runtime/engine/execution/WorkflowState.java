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

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.step.WorkflowStep;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Internal looking API for the execution.
 */
public interface WorkflowState {

    /**
     * Checks if provided class is a subtype of {@link WorkflowState} and throws an exception if this checks fails.
     *
     * @param clazz class to check.
     * @return provided class.
     */
    @Nonnull
    static <C> Class<C> requireIsWorkflowState(@Nonnull Class<C> clazz) {
        if (!WorkflowState.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format(
                    "Provided type %s must be instance of WorkflowState, but it was not.",
                    clazz.getName()));
        }
        return clazz;
    }


    @Nonnull
    <T extends WorkflowContext> T execute(@Nonnull WorkflowConfiguration<T> workflowConfiguration,
                                          @Nonnull WorkflowContext workflowContext) throws ExecutionSuspended;

    void applyStateChange(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext);

    void awaitStateChange(@Nonnull Predicate<WorkflowState> condition) throws InterruptedException;

    @Nonnull
    WorkflowStep getStep(@Nonnull String stepName);

    void onEvent(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext);

    void addStep(@Nonnull WorkflowStep workflowStep);

    boolean containsStep(@Nonnull String stepName);

    void appendTask(@Nonnull Consumer<WorkflowState> task);

    @Nullable
    Consumer<WorkflowState> getNextTask();

    @Nonnull
    WorkflowStatus getStatus();

    boolean isExecutable();

    boolean hasTasks();

    void registerWaitCondition(@Nonnull String stepName, @Nonnull QualifiedName qualifiedName,
                               @Nonnull Predicate<EventMessage> predicate,
                               @Nonnull EventNameCustomizer eventNameCustomizer);

    void removeWaitCondition(@Nonnull String stepName);

    // FIXME check if we can replace this for the Context interface
    @Nonnull
    ProcessingContext processingContext();
}
