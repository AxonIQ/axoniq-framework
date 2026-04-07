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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.api.execution.state;

import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Event sourced state of the workflow execution.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@Internal
public interface WorkflowState extends DescribableComponent {

    /**
     * Retrieves a list of step names in the workflow execution.
     *
     * @return list of step names.
     */
    List<String> workflowStepNames();

    /**
     * Retrieves a step by name.
     *
     * @param stepName name of the step.
     * @return workflow step.
     */
    @Nonnull
    WorkflowStep getStep(@Nonnull String stepName);

    /**
     * Checks if a step with the given name exists in the workflow execution.
     *
     * @param stepName name of the step.
     * @return true if the step exists, false otherwise.
     */
    boolean containsStep(@Nonnull String stepName);

    /**
     * Returns the status of the workflow execution.
     *
     * @return workflow status.
     */
    @Nonnull
    WorkflowStatus workflowStatus();

    /**
     * Retrieves the payload of the workflow execution.
     *
     * @return payload of the workflow execution.
     */
    @Nonnull
    Map<String, Object> payload();

    /**
     * Guards against invoking any primitive when the workflow has already reached a terminal state. Rethrows the
     * original termination cause wrapped in the appropriate exception type.
     */
    void throwTerminalCause();

    /**
     * Handles an event message received during workflow execution. This handle is responsible for the modification of
     * the state.
     *
     * @param eventMessage      the event message received.
     * @param processingContext the processing context for the event.
     * @return new evolved state.
     */
    WorkflowState evolve(@Nonnull EventMessage eventMessage, @Nonnull ProcessingContext processingContext);

    /**
     * Returns the step name that reached a terminal state first among the given candidates, determined by event-sourced
     * timestamps. This is a safeguard against a race condition during event-sourcing replay: when multiple steps
     * completed before cancellation took effect, array iteration order would pick an arbitrary winner. The event store
     * timestamps are the source of truth for ordering and are stable across replays.
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
     * Returns the step names that reached a terminal state among the given candidates, sorted by event-sourced
     * timestamps (earliest first). This is the plural counterpart of {@link #firstCompletedAmong(Set)}.
     *
     * @param stepNames the candidate step names to compare.
     * @return step names with terminal states, sorted by the earliest timestamp first.
     */
    @Nonnull
    default List<String> sortedCompletedAmong(@Nonnull Set<String> stepNames) {
        return stepNames.stream()
                        .filter(name -> containsStep(name) && getStep(name).status().isTerminal())
                        .sorted(Comparator.comparing(name -> getStep(name).timestamp()))
                        .toList();
    }
}
