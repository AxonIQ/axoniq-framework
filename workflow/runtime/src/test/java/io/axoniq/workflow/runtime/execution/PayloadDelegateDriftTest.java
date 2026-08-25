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

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.PayloadPrimitive;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadModification;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Tests for the replay-drift guard in {@link PayloadDelegate}.
 *
 * @author Stefan Dragisic
 */
class PayloadDelegateDriftTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private PayloadDelegate delegate;
    private final ReachedSteps reachedSteps = new ReachedSteps();

    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        state = mock(WorkflowState.class);
        EventSink eventSink = mock(EventSink.class);
        UnitOfWorkFactory unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        Executor executor = Runnable::run;
        EventNameCustomizer parent = DefaultEventNameCustomizer.Builder.defaults();

        when(workflowExecution.workflowId()).thenReturn("wf-1");
        when(workflowExecution.state()).thenReturn(state);

        delegate = new PayloadDelegate(
                workflowContext, workflowExecution, new RunningSteps(), reachedSteps, parent,
                Clock.systemUTC(), unitOfWorkFactory, eventSink, executor, new ControllableWorkflowScheduler()
        );
    }

    @Test
    void modifyPayload_throwsDriftException_whenUnreferencedTerminalStepsInState() {
        reachedSteps.record("A");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));
        when(state.containsStep("newPayloadMod")).thenReturn(false);

        PayloadModification mod = p -> p;
        assertThatThrownBy(() -> delegate.modifyPayload(
                PrimitiveCommands.modifyPayload("newPayloadMod", mod,
                                                DefaultEventNameCustomizer.Builder.defaults())))
                .isInstanceOf(WorkflowReplayDriftException.class)
                .satisfies(ex -> {
                    var drift = (WorkflowReplayDriftException) ex;
                    assertThat(drift.aboutToExecute()).isEqualTo("newPayloadMod");
                    assertThat(drift.orphans()).containsExactly("B");
                });
    }

    private WorkflowStep terminalStep(String name) {
        return new WorkflowStep(name, StepStatus.COMPLETED, null, null, Instant.now(), null);
    }
}
