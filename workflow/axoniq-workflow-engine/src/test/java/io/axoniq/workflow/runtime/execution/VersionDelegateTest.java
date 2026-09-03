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
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link VersionDelegate}, including the downstream-steps guard correctness invariant.
 *
 * @author Stefan Dragisic
 */
class VersionDelegateTest {

    private WorkflowContext workflowContext;
    private WorkflowExecution workflowExecution;
    private WorkflowState state;
    private EventSink eventSink;
    private ProcessingContext processingContext;
    private UnitOfWorkFactory unitOfWorkFactory;
    private Executor executor;
    private EventNameCustomizer parentCustomizer;
    private VersionDelegate delegate;

    private final ReachedSteps reachedSteps = new ReachedSteps();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        workflowContext = mock(WorkflowContext.class);
        workflowExecution = mock(WorkflowExecution.class);
        state = mock(WorkflowState.class);
        eventSink = mock(EventSink.class);
        processingContext = mock(ProcessingContext.class);
        unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        executor = Runnable::run;
        parentCustomizer = DefaultEventNameCustomizer.Builder.defaults();

        UnitOfWork unitOfWork = mock(UnitOfWork.class);
        when(unitOfWorkFactory.create(any(Function.class))).thenReturn(unitOfWork);
        when(unitOfWorkFactory.create(anyString(), any(Function.class))).thenReturn(unitOfWork);
        when(unitOfWork.executeWithResult(any(Function.class)))
                .thenAnswer(invocation -> {
                    Function<ProcessingContext, CompletableFuture<?>> action = invocation.getArgument(0);
                    return action.apply(processingContext);
                });

        when(workflowContext.workflowId()).thenReturn("wf-1");
        when(workflowContext.workflowPayload()).thenReturn(java.util.Map.of());
        when(workflowExecution.workflowId()).thenReturn("wf-1");
        when(workflowExecution.processingContext()).thenReturn(processingContext);
        when(workflowExecution.workflowContext()).thenReturn(workflowContext);
        when(workflowContext.processingContext()).thenReturn(processingContext);
        when(workflowContext.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(processingContext.component(EventConverter.class)).thenReturn(TestEventConverter.INSTANCE);
        when(workflowExecution.state()).thenReturn(state);
        when(workflowExecution.appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        delegate = new VersionDelegate(
                workflowContext,
                workflowExecution,
                reachedSteps,
                parentCustomizer,
                Clock.systemUTC()
        );
    }

    /**
     * Test helper: invoke the primitive and resolve the version it committed to (read from state after
     * the call completes, mirroring how the DSL layer surfaces the value).
     */
    private String invokeVersion(String stepName, String newVersion) {
        WorkflowStepResult r = delegate.version(
                PrimitiveCommands.version(stepName, newVersion, parentCustomizer));
        r.await();
        if (state.hasVersionMigrationStep(stepName)) {
            return state.versionFor(stepName);
        }
        return state.workflowDefinitionId().version();
    }

    private void givenWorkflowDefinitionVersion(String version) {
        when(state.workflowDefinitionId()).thenReturn(new MessageType("TestWorkflow", version));
    }

    @Test
    void requestSameAsCurrentEmitsNoEvent() {
        when(state.hasVersionMigrationStep("payment-redesign")).thenReturn(false);
        givenWorkflowDefinitionVersion("0.0.1");
        when(state.workflowStepNames()).thenReturn(List.of());

        String v = invokeVersion("payment-redesign", "0.0.1");

        assertThat(v).isEqualTo("0.0.1");
        verify(workflowExecution, never()).appendTask(any());
        verify(workflowExecution, never()).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    @Test
    void newWorkflowUpgradesToHigherVersionEmitsMarkerNamedAfterChangeId() throws InterruptedException {
        // State starts without the marker; once the task runs, the marker becomes present.
        when(state.hasVersionMigrationStep("payment-redesign")).thenReturn(false, true);
        when(state.versionFor("payment-redesign")).thenReturn("0.0.2");
        givenWorkflowDefinitionVersion("0.0.1");
        when(state.workflowStepNames()).thenReturn(List.of());
        when(workflowContext.workflowVersion()).thenReturn("0.0.1");
        runAppendedTaskImmediately();

        String v = invokeVersion("payment-redesign", "0.0.2");

        assertThat(v).isEqualTo("0.0.2");

        ArgumentCaptor<EventMessage> eventCaptor = ArgumentCaptor.forClass(EventMessage.class);
        verify(workflowExecution).appendWorkflowEvent(eventCaptor.capture(), eq(processingContext));
        EventMessage published = eventCaptor.getValue();
        // Wire-level event name carries the changeId — the "what changed" signal.
        // DefaultEventNameCustomizer capitalises the first letter of the anchor.
        assertThat(published.type().toString()).contains("Payment-redesign.Versioned");
        // MessageType.version() carries the new workflow version on the marker event itself.
        assertThat(published.type().version()).isEqualTo("0.0.2");
        assertThat(MetadataUtils.getVersionChangeId(published.metadata())).contains("payment-redesign");
        assertThat(MetadataUtils.getVersion(published.metadata())).contains("0.0.2");

        verify(workflowExecution).awaitStateChange(any());
    }

    @Test
    void downgradeRejected() {
        when(state.hasVersionMigrationStep("payment-redesign")).thenReturn(false);
        givenWorkflowDefinitionVersion("0.0.5");

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                delegate.version(PrimitiveCommands.version("payment-redesign", "0.0.2", parentCustomizer))
        ).isInstanceOf(IllegalArgumentException.class)
         .hasMessageContaining("not strictly greater");

        verify(workflowExecution, never()).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    @Test
    void replayedWorkflowWithMarkerReturnsRecordedValueDoesNotEmit() {
        when(state.hasVersionMigrationStep("payment-redesign")).thenReturn(true);
        when(state.versionFor("payment-redesign")).thenReturn("0.0.2");

        String v = invokeVersion("payment-redesign", "0.0.7");

        // Recorded value wins, even when the call site requests something different.
        assertThat(v).isEqualTo("0.0.2");
        verify(workflowExecution, never()).appendTask(any());
        verify(workflowExecution, never()).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    @Test
    void secondVersionCallForSameChangeIdReturnsRecordedValue() throws InterruptedException {
        // First call: no marker, emit; second call: marker present, no emit.
        when(state.hasVersionMigrationStep("payment-redesign")).thenReturn(false, true, true);
        when(state.versionFor("payment-redesign")).thenReturn("0.0.2");
        givenWorkflowDefinitionVersion("0.0.1");
        when(state.workflowStepNames()).thenReturn(List.of());
        when(workflowContext.workflowVersion()).thenReturn("0.0.1");
        runAppendedTaskImmediately();

        String first = invokeVersion("payment-redesign", "0.0.2");
        String second = invokeVersion("payment-redesign", "0.0.2");

        assertThat(first).isEqualTo("0.0.2");
        assertThat(second).isEqualTo("0.0.2");
        verify(workflowExecution, times(1)).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    /**
     * Worked example A from ADR 005: an old workflow with terminal steps {A, B, C}; new code inserts
     * `ctx.migrateVersion("x", "0.0.2")` between A and B. At the version call, the body has referenced only A
     * but state contains B and C terminal — the downstream-steps guard must block emission and return
     * the current version unchanged.
     */
    @Test
    void downstreamStepsGuardBlocksEmissionWhenLaterStepsArePresentInState() {
        reachedSteps.record("A");
        when(state.hasVersionMigrationStep("x")).thenReturn(false);
        givenWorkflowDefinitionVersion("0.0.1");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B", "C"));
        when(state.getStep("A")).thenReturn(stepInStatus(StepStatus.COMPLETED));
        when(state.getStep("B")).thenReturn(stepInStatus(StepStatus.COMPLETED));
        when(state.getStep("C")).thenReturn(stepInStatus(StepStatus.COMPLETED));

        String v = invokeVersion("x", "0.0.2");

        assertThat(v).isEqualTo("0.0.1");
        verify(workflowExecution, never()).appendTask(any());
        verify(workflowExecution, never()).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    /**
     * Worked example C from ADR 005: workflow stopped at A (state has {A} terminal only), new code
     * inserts `ctx.migrateVersion("x", "0.0.2")` between A and B. Body has referenced A; nothing is
     * downstream yet — guard allows emission, workflow adopts v0.0.2.
     */
    @Test
    void downstreamStepsGuardAllowsEmissionWhenAllStateStepsReferenced() throws InterruptedException {
        reachedSteps.record("A");
        when(state.hasVersionMigrationStep("x")).thenReturn(false, true);
        when(state.versionFor("x")).thenReturn("0.0.2");
        givenWorkflowDefinitionVersion("0.0.1");
        when(state.workflowStepNames()).thenReturn(List.of("A"));
        when(state.getStep("A")).thenReturn(stepInStatus(StepStatus.COMPLETED));
        when(workflowContext.workflowVersion()).thenReturn("0.0.1");
        runAppendedTaskImmediately();

        String v = invokeVersion("x", "0.0.2");

        assertThat(v).isEqualTo("0.0.2");
        verify(workflowExecution).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    @Test
    void downstreamStepsGuardEmptyStateAllowsEmission() throws InterruptedException {
        when(state.hasVersionMigrationStep("x")).thenReturn(false, true);
        when(state.versionFor("x")).thenReturn("0.0.3");
        givenWorkflowDefinitionVersion("0.0.1");
        when(state.workflowStepNames()).thenReturn(List.of());
        when(workflowContext.workflowVersion()).thenReturn("0.0.1");
        runAppendedTaskImmediately();

        String v = invokeVersion("x", "0.0.3");

        assertThat(v).isEqualTo("0.0.3");
        verify(workflowExecution).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    /**
     * A non-terminal step ahead of the cursor (e.g. a STARTED step the old code was still working on)
     * should NOT block emission — only terminal steps prove old-code execution committed past this point.
     */
    @Test
    void downstreamStepsGuardIgnoresNonTerminalStepsAhead() throws InterruptedException {
        reachedSteps.record("A");
        when(state.hasVersionMigrationStep("x")).thenReturn(false, true);
        when(state.versionFor("x")).thenReturn("0.0.2");
        givenWorkflowDefinitionVersion("0.0.1");
        when(state.workflowStepNames()).thenReturn(List.of("A", "B"));
        when(state.getStep("A")).thenReturn(stepInStatus(StepStatus.COMPLETED));
        when(state.getStep("B")).thenReturn(stepInStatus(StepStatus.STARTED));
        when(workflowContext.workflowVersion()).thenReturn("0.0.1");
        runAppendedTaskImmediately();

        String v = invokeVersion("x", "0.0.2");

        assertThat(v).isEqualTo("0.0.2");
        verify(workflowExecution).appendWorkflowEvent(any(EventMessage.class), any(ProcessingContext.class));
    }

    private WorkflowStep stepInStatus(StepStatus status) {
        // WorkflowStep is a record — instantiate it directly rather than mocking.
        return new WorkflowStep("step", status, null, null, Instant.now(), null);
    }

    @SuppressWarnings("unchecked")
    private void runAppendedTaskImmediately() {
        // Execute every task synchronously the moment it's appended, so the delegate's
        // awaitStateChange() sees the world after the publish step has happened.
        Mockito.doAnswer(invocation -> {
            Consumer<WorkflowExecution> task = invocation.getArgument(0);
            task.accept(workflowExecution);
            return null;
        }).when(workflowExecution).appendTask(any(Consumer.class));

        // awaitStateChange must not block — by the time we call it the predicate is already true.
        try {
            Mockito.doAnswer(invocation -> {
                Predicate<WorkflowState> predicate = invocation.getArgument(0);
                if (!predicate.test(state)) {
                    throw new AssertionError("awaitStateChange predicate not satisfied — test scaffolding bug");
                }
                return null;
            }).when(workflowExecution).awaitStateChange(any());
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
