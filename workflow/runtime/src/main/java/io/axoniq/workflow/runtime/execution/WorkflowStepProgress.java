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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import jakarta.annotation.Nonnull;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the workflow steps reached while a workflow body is invoked.
 * <p>
 * This tracker represents the invocation-local progress through the workflow definition. Before a workflow body is
 * invoked, its recorded step names are cleared. Each primitive records the step it reaches, allowing the engine to
 * compare the current definition's progress with the event-sourced {@link WorkflowState}. When terminal steps exist in
 * the state but were not reached by the current body invocation, the tracker detects replay drift before a new event is
 * emitted.
 * <p>
 * This is deliberately separate from {@link RunningSteps}. {@code RunningSteps} tracks active asynchronous step
 * executions and supports their lifecycle operations, whereas this class records only which workflow steps the body has
 * reached. A reached step can be terminal, pending, or no longer running; conversely, determining replay progress must
 * not depend on whether a step currently has an active execution.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 0.3.0
 */
final class WorkflowStepProgress implements DescribableComponent {

    private final Set<String> referencedStepNames = ConcurrentHashMap.newKeySet();

    /**
     * Records that the current workflow body invocation reached a step.
     *
     * @param stepName name of the reached workflow step
     */
    void record(@Nonnull String stepName) {
        referencedStepNames.add(stepName);
    }

    /**
     * Clears the progress collected for the preceding workflow body invocation.
     */
    void clear() {
        referencedStepNames.clear();
    }

    /**
     * Indicates whether the event-sourced state contains a terminal step not reached by the current invocation.
     *
     * @param state event-sourced state to compare with this invocation's progress
     * @return {@code true} when at least one terminal state step was not reached
     */
    boolean hasUnreferencedTerminalStep(@Nonnull WorkflowState state) {
        return !unreferencedTerminalSteps(state).isEmpty();
    }

    /**
     * Rejects event emission when the current invocation no longer reaches terminal steps in the event-sourced state.
     *
     * @param workflowId identifier of the workflow being invoked
     * @param state event-sourced state to compare with this invocation's progress
     * @param aboutToExecute description of the operation that would emit a new event
     * @throws WorkflowReplayDriftException when terminal state steps were not reached by the current invocation
     */
    void guardAgainstReplayDrift(@Nonnull String workflowId,
                                 @Nonnull WorkflowState state,
                                 @Nonnull String aboutToExecute) {
        List<String> unreferenced = unreferencedTerminalSteps(state);
        if (!unreferenced.isEmpty()) {
            throw new WorkflowReplayDriftException(workflowId, aboutToExecute, unreferenced);
        }
    }

    @Nonnull
    private List<String> unreferencedTerminalSteps(@Nonnull WorkflowState state) {
        return state.workflowStepNames().stream()
                    .filter(name -> !referencedStepNames.contains(name))
                    .filter(name -> {
                        var step = state.getStep(name);
                        return step != null && step.status().isTerminal();
                    })
                    .toList();
    }

    @Override
    public void describeTo(@Nonnull ComponentDescriptor descriptor) {
        descriptor.describeProperty("referencedSteps", referencedStepNames.stream().toList());
    }
}
