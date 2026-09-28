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

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import io.axoniq.framework.workflow.runtime.util.WorkflowStateUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records steps reached by one workflow body invocation for replay-drift detection.
 * <p>
 * Before a workflow body is invoked, the recorded step names are cleared. Each primitive records the step it reaches,
 * allowing the engine to compare the current invocation with the event-sourced {@link WorkflowState}. When a terminal
 * state step was not reached by the current invocation, this class detects replay drift before a new event is emitted.
 * <p>
 * This is separate from {@link RunningSteps}, which tracks active asynchronous executions. A reached step can be
 * terminal, pending, or no longer running, so replay-drift detection must not depend on active execution state.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public class ReachedSteps implements DescribableComponent {

    private final Set<String> referencedStepNames = ConcurrentHashMap.newKeySet();

    /**
     * Records that the current workflow body invocation reached a step.
     *
     * @param stepName name of the reached workflow step
     */
    void record(String stepName) {
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
    boolean hasUnreferencedTerminalStep(WorkflowState state) {
        return !unreferencedTerminalSteps(state).isEmpty();
    }

    /**
     * Asserts that the current invocation has reached every terminal step in the recorded state.
     *
     * @param workflowId     identifier of the workflow being invoked
     * @param state          recorded state to compare with this invocation's progress
     * @param aboutToExecute description of the operation being attempted
     * @throws WorkflowReplayDriftException when terminal state steps were not reached by the current invocation
     */
    void assertNoReplayDrift(String workflowId,
                             WorkflowState state,
                             String aboutToExecute) {
        List<String> unreferenced = unreferencedTerminalSteps(state);
        if (!unreferenced.isEmpty()) {
            throw new WorkflowReplayDriftException(workflowId, aboutToExecute, unreferenced);
        }
    }

    private List<String> unreferencedTerminalSteps(WorkflowState state) {
        return state.workflowStepNames().stream()
                    .filter(name -> !referencedStepNames.contains(name))
                    .filter(name -> WorkflowStateUtils.isStepTerminal(state, name))
                    .toList();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("referencedSteps", referencedStepNames.stream().toList());
    }
}
