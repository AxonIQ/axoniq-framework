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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.axoniq.workflow.configuration.WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.FOUR_SEGMENTS;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.anyIdOn;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.idOnAnotherSegmentThan;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Neither segment callback may hold a segment forever when the work it waits on never completes.
 * <p>
 * Both wait on work that can stall without failing: a claim on a remote workflow store loading durable state, a release
 * on interrupted bodies unwinding. Both report failure once the timeout elapses, so the coordinator can act on it
 * instead of the segment staying claimed on a node with no instance running and nothing in the log.
 */
class WorkflowEngineSegmentCallbackTest {

    private static final Segment SEGMENT = FOUR_SEGMENTS.getFirst();
    private static final String OWNED_ID = anyIdOn(SEGMENT);
    private static final String FOREIGN_ID = idOnAnotherSegmentThan(OWNED_ID);

    private static final Duration SHORT_TIMEOUT = Duration.ofMillis(200);

    @Test
    void aClaimWhoseStateLoadNeverCompletesFailsInsteadOfHoldingTheSegment() {
        var workflowStore = mock(WorkflowStore.class);
        // A load that never completes, which is what a hanging store looks like from here.
        when(workflowStore.loadRunningWorkflows(any())).thenReturn(new CompletableFuture<>());
        var engine = engine(new InMemoryWorkflowExecutionRepository(), workflowStore);

        var claim = engine.restoreWorkflowsFor(SEGMENT,
                                               null,
                                               mock(ProcessingContext.class),
                                               mock(ProcessingContext.class));

        assertThatThrownBy(claim::join)
                .as("""
                    The claim must complete exceptionally once the timeout elapses. Completing it normally reports a \
                    segment as claimed while none of its instances is running; never completing it holds the segment \
                    on this node with no diagnostics.""")
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void aReleaseWaitsForTheInterruptedBodiesOfItsOwnSegmentOnly() {
        var owned = executionNotUnwinding(OWNED_ID);
        var foreign = executionNotUnwinding(FOREIGN_ID);
        var repository = new InMemoryWorkflowExecutionRepository();
        repository.save(OWNED_ID, () -> owned);
        repository.save(FOREIGN_ID, () -> foreign);

        var release = engine(repository, mock(WorkflowStore.class)).releaseWorkflowsFor(SEGMENT);

        assertThat(release)
                .as("""
                    Segment %s owns '%s', whose body has not unwound yet, so the release must still be pending. \
                    Completing it here reports this node quiet on the segment while a body of it is still running, so \
                    the node claiming it next resumes the instance alongside the one still going here.""",
                    SEGMENT, OWNED_ID)
                .isNotDone();
        verify(foreign, never()).interrupt();
    }

    @Test
    void aReleaseOfASegmentWithoutInstancesCompletesRightAway() {
        var engine = engine(new InMemoryWorkflowExecutionRepository(), mock(WorkflowStore.class));

        assertThat(engine.releaseWorkflowsFor(SEGMENT))
                .as("nothing to drain, so nothing to wait for")
                .isCompleted();
    }

    @Test
    void interruptingAnInstanceWhoseBodyNeverStartedReportsItQuietRightAway() {
        // A restored instance waiting for its segment to catch up: materialized, but never executed.
        var configuration = mock(WorkflowConfiguration.class);
        when(configuration.workflowName()).thenReturn("RestoredWorkflow");
        when(configuration.workflowVersion()).thenReturn("1.0.0");
        when(configuration.workflowStatusChangeListeners()).thenReturn(Map.of());
        when(configuration.eventNameCustomizer()).thenReturn(defaults());
        var restored = new SimpleWorkflowExecution(OWNED_ID,
                                                  Map.of(),
                                                  bodyContext(),
                                                  configuration,
                                                  mock(WorkflowContext.class));

        assertThat(restored.interrupt())
                .as("""
                    Nothing is running for this instance, so there is nothing to unwind. Handing back a pending future \
                    here would stall the release of every segment holding a restored instance until the timeout.""")
                .isCompleted();
    }

    @Test
    void aReleaseHandsBackAFutureACallerMayBoundWithoutTouchingTheInstance() {
        var execution = executionNotUnwinding(OWNED_ID);
        var repository = new InMemoryWorkflowExecutionRepository();
        repository.save(OWNED_ID, () -> execution);
        var release = engine(repository, mock(WorkflowStore.class)).releaseWorkflowsFor(SEGMENT);

        release.orTimeout(1, TimeUnit.MILLISECONDS);

        assertThat(execution.interrupt())
                .as("""
                    The processor bounds the release with its own timeout, which must not complete the drain state of \
                    the instance itself: an instance whose body is still unwinding has to keep reporting that.""")
                .isNotDone();
    }

    @Test
    void theProductionTimeoutIsUsedWhenNoneIsSetForATest() {
        var engine = new WorkflowEngine(new SimpleWorkflowConfigurationRegistry(),
                                        new InMemoryWorkflowExecutionRepository(),
                                        mock(WorkflowStore.class),
                                        mock(UnitOfWorkFactory.class));

        assertThat(engine.restoreTimeout).isEqualTo(WorkflowEngine.DEFAULT_RESTORE_TIMEOUT);
    }

    private static WorkflowEngine engine(WorkflowExecutionRepository repository, WorkflowStore workflowStore) {
        var engine = new WorkflowEngine(new SimpleWorkflowConfigurationRegistry(),
                                        repository,
                                        workflowStore,
                                        mock(UnitOfWorkFactory.class));
        engine.setEngineSupportComponents(new WorkflowEngineReplaySupport(engine),
                                          new WorkflowEngineCheckpointingSupport(engine));
        engine.restoreTimeout = SHORT_TIMEOUT;
        return engine;
    }

    /** The context a restored body would run under; its executor never runs anything in this test. */
    private static ProcessingContext bodyContext() {
        var context = mock(ProcessingContext.class);
        when(context.resources()).thenReturn(Map.of());
        when(context.component(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));
        when(context.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(context.component(ExecutorService.class, WORKFLOW_ENGINE_EXECUTOR))
                .thenReturn(mock(ExecutorService.class));
        when(context.component(EventSink.class)).thenReturn(mock(EventSink.class));
        when(context.component(WorkflowScheduler.class)).thenReturn(mock(WorkflowScheduler.class));
        when(context.component(ExecuteStepActionResolver.class)).thenReturn(mock(ExecuteStepActionResolver.class));
        return context;
    }

    /** An instance whose interrupt never reports back, which is what a body that does not unwind looks like. */
    private static WorkflowExecution executionNotUnwinding(String workflowId) {
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn(workflowId);
        when(execution.interrupt()).thenReturn(new CompletableFuture<>());
        return execution;
    }
}
