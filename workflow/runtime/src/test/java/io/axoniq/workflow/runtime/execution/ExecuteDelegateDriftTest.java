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
import io.axoniq.workflow.runtime.api.execution.context.ExecutePrimitive;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowReplayDriftException;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the replay-drift guard in {@link ExecuteDelegate}: throws {@link WorkflowReplayDriftException} when state
 * contains terminal steps the current invocation has not referenced, but only when the primitive is about to publish a
 * STARTED event live (cached lookups are exempt).
 *
 * @author Stefan Dragisic
 */
class ExecuteDelegateDriftTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private ExecuteDelegate delegate;
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
        // No queued task is available in this fixture.
        when(workflowExecution.isRunning()).thenReturn(true);
        when(workflowExecution.hasTasks()).thenReturn(false);

        delegate = new ExecuteDelegate(
                workflowContext,
                workflowExecution,
                new RunningSteps(),
                reachedSteps,
                parent,
                Clock.systemUTC(),
                unitOfWorkFactory,
                executor,
                new ControllableWorkflowScheduler(),
                new DefaultExecuteStepActionResolver()
        );
    }

    @Test
    void executeThrowsDriftExceptionWhenUnreferencedTerminalStepsInState() {
        // History has A and B terminal; invocation has only referenced A so far; about to run new step "C".
        reachedSteps.record("A");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));
        when(state.containsStep("C")).thenReturn(false);

        assertThatThrownBy(() -> delegate.execute(executeCommand("C")))
                .isInstanceOf(WorkflowReplayDriftException.class)
                .satisfies(ex -> {
                    var drift = (WorkflowReplayDriftException) ex;
                    assertThat(drift.workflowId()).isEqualTo("wf-1");
                    assertThat(drift.aboutToExecute()).isEqualTo("C");
                    assertThat(drift.orphans()).containsExactly("B");
                });
    }

    @Test
    void executeGuardPassesWhenAllTerminalStepsReferenced() {
        // Body has referenced both terminal steps; about to run a new step legitimately appended.
        // The full delegate path needs more mocks to run; we only assert the drift guard does NOT
        // fire (any downstream NPE from incomplete mock setup is fine — it proves the guard let us
        // through).
        reachedSteps.record("A");
        reachedSteps.record("B");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));
        when(state.containsStep("C")).thenReturn(false);

        assertNoDriftThrown(() -> delegate.execute(executeCommand("C")));
    }

    @Test
    void executeGuardPassesForCachedStepLookup() {
        // State has a terminal step the invocation hasn't referenced yet, BUT execute is called on
        // a step already in state — that's a cached lookup, no live publish, no drift exception.
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(terminalStep("A"));
        when(state.getStep("B")).thenReturn(terminalStep("B"));
        when(state.containsStep("A")).thenReturn(true);

        assertNoDriftThrown(() -> delegate.execute(executeCommand("A")));
    }

    @Test
    void executeGuardPassesOnEmptyState() {
        // Fresh workflow — nothing in state yet — no drift possible.
        when(state.workflowStepNames()).thenReturn(List.of());
        when(state.containsStep("first")).thenReturn(false);

        assertNoDriftThrown(() -> delegate.execute(executeCommand("first")));
    }

    /**
     * Asserts the call does not throw {@link WorkflowReplayDriftException}. Any other exception (e.g. NPE from
     * incomplete mock setup after the guard) is fine — the guard let us through.
     */
    private void assertNoDriftThrown(Runnable r) {
        try {
            r.run();
        } catch (WorkflowReplayDriftException e) {
            throw new AssertionError("Unexpected drift exception: " + e.getMessage(), e);
        } catch (Throwable ignored) {
            // expected — full delegate flow is not mocked, but the drift guard passed
        }
    }

    private WorkflowStep terminalStep(String name) {
        return new WorkflowStep(name, StepStatus.COMPLETED, null, null, Instant.now(), null);
    }

    private ExecutePrimitive.ExecuteCommand executeCommand(String stepName) {
        PayloadProcessor action = (ctx, payload) -> Map.of();
        return new PrimitiveCommands.WorkflowStepResultExecuteCommand(
                stepName,
                Map.of(),
                action,
                LocalOnlyPayloadReducer.INSTANCE,
                GlobalOnlyPayloadReducer.INSTANCE,
                Duration.ofSeconds(5),
                DefaultEventNameCustomizer.Builder.defaults(),
                RetryPolicy.NONE
        );
    }
}
