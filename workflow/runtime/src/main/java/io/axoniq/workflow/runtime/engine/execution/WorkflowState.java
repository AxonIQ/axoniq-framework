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
import io.axoniq.workflow.runtime.api.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.step.WorkflowStep;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
public interface WorkflowState {

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
    WorkflowStatus workflowStatus();

    /**
     * Returns the cause of workflow termination, if the workflow has been terminated via fail or cancel.
     *
     * @return the termination cause, or {@link Optional#empty()} if the workflow has not been terminated.
     */
    @Nonnull
    Optional<Throwable> getTerminationCause();

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
     * @param stepName name of waiting step.
     */
    void removeWaitCondition(@Nonnull String stepName);

    /**
     * Returns the step name that reached a terminal state first among the given candidates,
     * determined by event-sourced timestamps. This is a safeguard against a race condition
     * during event-sourcing replay: when multiple steps completed before cancellation took
     * effect, array iteration order would pick an arbitrary winner. The event store timestamps
     * are the source of truth for ordering and are stable across replays.
     *
     * @param stepNames the candidate step names to compare.
     * @return the step name with the earliest terminal-state timestamp, or empty if none found.
     */
    @Nonnull
    default Optional<String> firstCompletedAmong(@Nonnull Set<String> stepNames) {
        return stepNames.stream()
                .filter(name -> containsStep(name) && getStep(name).status().isTerminal())
                .min(Comparator.comparing(name -> getStep(name).timestamp()));
    }

    /**
     * Returns the step names that reached a terminal state among the given candidates,
     * sorted by event-sourced timestamps (earliest first). This is the plural counterpart
     * of {@link #firstCompletedAmong(Set)}.
     *
     * @param stepNames the candidate step names to compare.
     * @return step names with terminal states, sorted by earliest timestamp first.
     */
    @Nonnull
    default List<String> sortedCompletedAmong(@Nonnull Set<String> stepNames) {
        return stepNames.stream()
                .filter(name -> containsStep(name) && getStep(name).status().isTerminal())
                .sorted(Comparator.comparing(name -> getStep(name).timestamp()))
                .toList();
    }

    void registerRunningStep(@Nonnull String stepName, @Nonnull java.util.concurrent.CompletableFuture<?> future);

    void removeRunningStep(@Nonnull String stepName);

    boolean cancelRunningStep(@Nonnull String stepName, @Nullable Throwable cause);

    void cancelAllRunningSteps(@Nullable Throwable cause);

    void cancelAndRemoveRunningStep(@Nonnull String stepName, boolean mayInterruptIfRunning);

    // FIXME check if we can replace this for the Context interface
    @Nonnull
    ProcessingContext processingContext();
}
